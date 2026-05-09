// Initialize MongoDB replica set (required for transactions and change streams)
rs.initiate({
  _id: "rs0",
  members: [{ _id: 0, host: "localhost:27017" }]
});
