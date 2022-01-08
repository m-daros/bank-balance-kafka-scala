ThisBuild / version := "0.1.0-SNAPSHOT"
ThisBuild / scalaVersion := "2.13.7"

val akkaVersion = "2.6.18"
val kafkaVersion = "2.8.1"
val circeVersion = "0.14.1"

val scalaTest = "org.scalatest" %% "scalatest" % "3.2.9" % Test

lazy val root = ( project in file ( "." ) )
  .aggregate ( `bank-transactions-producer`, `bank-balance-processor`, `data-model` )
  .settings ( name := "bank-balance-kafka-scala" )

lazy val `bank-transactions-producer` = ( project in file ( "bank-transactions-producer" ) )
  .dependsOn ( `data-model` )
  .settings ( libraryDependencies ++=  Seq (
    "com.typesafe.akka" %% "akka-stream" % akkaVersion,
    "org.apache.kafka" % "kafka-clients" % kafkaVersion,
    "io.circe" %% "circe-core" % circeVersion,
    "io.circe" %% "circe-generic" % circeVersion,
    "io.circe" %% "circe-parser" % circeVersion,
    "org.slf4j" % "slf4j-api" % "1.7.32",
    "ch.qos.logback" % "logback-core" % "1.2.10",
    "ch.qos.logback" % "logback-classic" % "1.2.10",

    // Test dependencies
    scalaTest,
    "org.mockito" %% "mockito-scala" % "1.16.49" % Test
  ) )

lazy val `bank-balance-processor` = ( project in file ( "bank-balance-processor" ) )
  .dependsOn ( `data-model` )
  .settings ( libraryDependencies ++=  Seq (
    "org.apache.kafka" % "kafka-clients" % kafkaVersion,
    "org.apache.kafka" % "kafka-streams" % kafkaVersion,
    "org.apache.kafka" %% "kafka-streams-scala" % kafkaVersion,
    "io.circe" %% "circe-core" % circeVersion,
    "io.circe" %% "circe-generic" % circeVersion,
    "io.circe" %% "circe-parser" % circeVersion,
    "org.slf4j" % "slf4j-api" % "1.7.32",
    "ch.qos.logback" % "logback-core" % "1.2.10",
    "ch.qos.logback" % "logback-classic" % "1.2.10",

    // Test dependencies
    scalaTest,
    "org.mockito" %% "mockito-scala" % "1.16.49" % Test,
    "org.apache.kafka" % "kafka-streams-test-utils" % kafkaVersion % Test
  ) )

lazy val `data-model` = ( project in file ( "data-model" ) )
  .settings ( libraryDependencies ++=  Seq (
    "org.apache.kafka" % "kafka-clients" % kafkaVersion,
    "org.apache.kafka" % "kafka-streams" % kafkaVersion,

    scalaTest
  ) )