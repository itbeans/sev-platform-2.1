/**
 * k6 OCPP WebSocket load test
 *
 * Simulates 1 000+ concurrent charging stations connecting via OCPP-J 1.6.
 * Each VU models one station: connect → BootNotification → Heartbeats →
 * Authorize → StartTransaction → MeterValues loop → StopTransaction → close.
 *
 * Run:
 *   k6 run --vus 200 --duration 5m ocpp-ws-load.js              # ramp to 200
 *   k6 run -e GATEWAY_URL=ws://localhost:8010 \
 *          -e TARGET_VUS=1000 ocpp-ws-load.js                   # full scale
 *
 * Pass/fail thresholds (printed at end):
 *   - 99th-pct OCPP response < 30 s  (protocol requirement)
 *   - p95 response < 2 s             (operational target)
 *   - error rate < 1 %
 */

import ws from "k6/ws";
import { check, sleep } from "k6";
import { Counter, Trend, Rate } from "k6/metrics";
import { randomString, randomIntBetween } from "https://jslib.k6.io/k6-utils/1.4.0/index.js";

// ── Custom metrics ────────────────────────────────────────────────────────────
const ocppResponseTime = new Trend("ocpp_response_ms", true);
const ocppErrors       = new Counter("ocpp_errors");
const ocppBootOk       = new Counter("ocpp_boot_accepted");
const ocppTxStarted    = new Counter("ocpp_tx_started");
const ocppTxStopped    = new Counter("ocpp_tx_stopped");
const wsConnectFail    = new Rate("ws_connect_fail");

// ── Config ────────────────────────────────────────────────────────────────────
const GATEWAY_URL  = __ENV.GATEWAY_URL  || "ws://localhost:8010";
const TARGET_VUS   = parseInt(__ENV.TARGET_VUS || "200", 10);
const TEST_TENANT  = __ENV.TEST_TENANT  || "test-tenant";

export const options = {
  scenarios: {
    stations: {
      executor: "ramping-vus",
      startVUs: 0,
      stages: [
        { duration: "2m",  target: Math.floor(TARGET_VUS * 0.25) }, // ramp
        { duration: "5m",  target: TARGET_VUS },                    // sustained
        { duration: "2m",  target: Math.floor(TARGET_VUS * 0.25) }, // drain
        { duration: "30s", target: 0 },
      ],
      gracefulRampDown: "30s",
    },
  },
  thresholds: {
    "ocpp_response_ms":            ["p(99)<30000", "p(95)<2000"],
    "ocpp_errors":                 ["count<10"],
    "ws_connect_fail":             ["rate<0.01"],
    "ws_connecting_duration":      ["p(95)<3000"],
  },
};

// ── OCPP-J helpers ────────────────────────────────────────────────────────────
let _msgSeq = 0;
function callMsg(action, payload) {
  const id = `${__VU}-${++_msgSeq}`;
  return { id, frame: JSON.stringify([2, id, action, payload]) };
}

function parseFrame(raw) {
  try { return JSON.parse(raw); } catch { return null; }
}

// ── VU entrypoint ─────────────────────────────────────────────────────────────
export default function () {
  const stationId  = `EV-LOAD-${TEST_TENANT}-${__VU}-${randomString(4).toUpperCase()}`;
  const url        = `${GATEWAY_URL}/ocpp/1.6/${stationId}`;

  // Pending call registry: msgId → {resolve, sentAt}
  const pending = {};

  const res = ws.connect(
    url,
    { headers: { "Sec-WebSocket-Protocol": "ocpp1.6" } },
    function (socket) {
      wsConnectFail.add(false);

      // ── Incoming frame router ──────────────────────────────────────────────
      socket.on("message", (raw) => {
        const msg = parseFrame(raw);
        if (!msg) { ocppErrors.add(1); return; }

        const [type, id] = msg;
        if (type === 3 && pending[id]) {
          // CallResult
          const elapsed = Date.now() - pending[id].sentAt;
          ocppResponseTime.add(elapsed);
          pending[id].resolve(msg[2]);
          delete pending[id];
        } else if (type === 4) {
          // CallError
          ocppErrors.add(1);
          if (pending[id]) delete pending[id];
        }
        // type===2 (Call from server — e.g. GetConfiguration) — ignore in load test
      });

      socket.on("error", () => { ocppErrors.add(1); });

      // ── Helper: send a Call and await its CallResult ───────────────────────
      function call(action, payload, timeoutMs = 10_000) {
        return new Promise((resolve, reject) => {
          const { id, frame } = callMsg(action, payload);
          pending[id] = { resolve, sentAt: Date.now() };
          socket.send(frame);
          // Timeout guard
          setTimeout(() => {
            if (pending[id]) {
              ocppErrors.add(1);
              delete pending[id];
              reject(new Error(`${action} timeout`));
            }
          }, timeoutMs);
        });
      }

      // ── Station session ────────────────────────────────────────────────────
      socket.setTimeout(async () => {
        try {
          // 1. BootNotification
          const bootResp = await call("BootNotification", {
            chargePointModel: "EV-LOAD-SIM",
            chargePointVendor: "itbeans-loadtest",
            firmwareVersion: "1.0.0",
          });
          check(bootResp, { "boot accepted": (r) => r.status === "Accepted" });
          if (bootResp.status === "Accepted") ocppBootOk.add(1);
          else { socket.close(); return; }

          // 2. StatusNotification — Available
          await call("StatusNotification", {
            connectorId: 1,
            errorCode: "NoError",
            status: "Available",
          });

          // 3. Heartbeat loop while idle (random 10-30 s)
          const idleSecs = randomIntBetween(10, 30);
          for (let i = 0; i < Math.floor(idleSecs / 5); i++) {
            await call("Heartbeat", {});
            sleep(5);
          }

          // 4. Authorize
          const idTag = `TAG-${randomString(6).toUpperCase()}`;
          const authResp = await call("Authorize", { idTag });
          check(authResp, { "authorize accepted": (r) => r.idTagInfo?.status === "Accepted" });

          // 5. StartTransaction
          const startResp = await call("StartTransaction", {
            connectorId: 1,
            idTag,
            meterStart: 0,
            timestamp: new Date().toISOString(),
          });
          check(startResp, { "tx started": (r) => !!r.transactionId });
          if (!startResp.transactionId) { socket.close(); return; }
          ocppTxStarted.add(1);
          const txId = startResp.transactionId;

          // 6. MeterValues every 15 s for 30-90 s
          const txDuration = randomIntBetween(30, 90);
          let meterKwh = 0;
          for (let i = 0; i < Math.floor(txDuration / 15); i++) {
            meterKwh += randomIntBetween(200, 800) / 100;
            await call("MeterValues", {
              connectorId: 1,
              transactionId: txId,
              meterValue: [{
                timestamp: new Date().toISOString(),
                sampledValue: [
                  { value: String((meterKwh * 1000).toFixed(0)), measurand: "Energy.Active.Import.Register", unit: "Wh" },
                  { value: String(randomIntBetween(0, 32)), measurand: "Current.Import", unit: "A" },
                ],
              }],
            });
            sleep(15);
          }

          // 7. StopTransaction
          const stopResp = await call("StopTransaction", {
            transactionId: txId,
            meterStop: Math.round(meterKwh * 1000),
            timestamp: new Date().toISOString(),
            reason: "Local",
          });
          check(stopResp, { "tx stopped": (r) => r !== undefined });
          ocppTxStopped.add(1);

        } catch (err) {
          ocppErrors.add(1);
        } finally {
          socket.close();
        }
      }, 0);
    }
  );

  if (res.status !== 101) {
    wsConnectFail.add(true);
    ocppErrors.add(1);
  }
}
