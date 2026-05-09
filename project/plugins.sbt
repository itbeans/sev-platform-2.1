// Code formatting
addSbtPlugin("org.scalameta" % "sbt-scalafmt" % "2.5.2")

// Static analysis
addSbtPlugin("ch.epfl.scala" % "sbt-scalafix" % "0.13.0")

// Docker image generation
addSbtPlugin("com.github.sbt" % "sbt-native-packager" % "1.10.4")

// Hot-reload in development
addSbtPlugin("io.spray" % "sbt-revolver" % "0.10.0")

// gRPC / protobuf code generation
addSbtPlugin("com.thesamet" % "sbt-protoc" % "1.0.7")

// Dependency updates checker
addSbtPlugin("com.timushev.sbt" % "sbt-updates" % "0.6.4")

// Build info (inject version into compiled code)
addSbtPlugin("com.eed3si9n" % "sbt-buildinfo" % "0.12.0")

// Test coverage
addSbtPlugin("org.scoverage" % "sbt-scoverage" % "2.1.1")

libraryDependencies += "com.thesamet.scalapb" %% "compilerplugin" % "0.11.17"
