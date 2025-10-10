name := "pekko-essentials-typed"

version := "0.1"

scalaVersion := "3.3.3"

val pekkoVersion = "1.1.2"
val scalaTestVersion = "3.2.9"
val logbackVersion = "1.5.6"

libraryDependencies ++= Seq(
  "org.apache.pekko" %% "pekko-actor-typed" % pekkoVersion,
  "org.apache.pekko" %% "pekko-actor-testkit-typed" % pekkoVersion,
  "org.scalatest" %% "scalatest" % scalaTestVersion,
  "ch.qos.logback" % "logback-classic" % logbackVersion,
  "io.github.samueleresca" %% "pekko-quartz-scheduler" % "1.3.0-pekko-1.1.x"
)
