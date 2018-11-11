# Avro-Kafka-Producer


How to run :
From the projet dir

export JARS=./lib/kafka-clients-0.10.0.0.jar,./lib/kafka_2.11-0.10.0.0.jar,./lib/spark-avro_2.11-3.2.0.jar

spark-submit --jars $JARS --class avro.kafka.Producer <path>/avro-kafka-producer_2.11-0.0.jar <avroFilesDir> <KafkaBroker> <KafkaTopic>
