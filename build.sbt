ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "3.3.3"

lazy val root = (project in file("."))
  .settings(
    name := "pekko-connectors-accumulo",
    libraryDependencies := Seq(
      "org.apache.pekko" %% "pekko-stream" % "1.1.4",
      "org.apache.accumulo" % "accumulo-core" % "2.1.3" % Provided,
      "org.scalatest" %% "scalatest" % "3.2.19" % Test,
      "org.apache.pekko" %% "pekko-stream-testkit" % "1.1.4" % Test,
      "org.geomesa.testcontainers" % "testcontainers-accumulo" % "1.4.1" % Test,
      "org.slf4j" % "slf4j-simple" % "2.0.17"
    )
  )
