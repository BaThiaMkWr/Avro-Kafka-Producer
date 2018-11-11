name := "avro-kafka-producer"
version := "0.0"
scalaVersion := "2.11.8"

libraryDependencies ++= Seq(
  "org.apache.spark" % "spark-core_2.11" % "2.1.0",
  "org.apache.spark" % "spark-sql_2.11" % "2.1.0",
  "org.apache.avro" % "avro" % "1.8.1",
  "org.apache.kafka" % "kafka-clients" % "0.10.0.0",
)