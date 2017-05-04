# Demo how to use spark streaming + Kafka + Kudu
## Business Use Case

The streaming application reads real-time security market price from Kafka, de-duplication based on timestamp, then update portfolio position value in Kudu

## Environments

The Demo was executed in CDH 5.11 in AWS. The cluster was created by Cloudera Director
Kudu version is 1.3
Kafka version is 0.10
Spark version is 2.1

## How to run the streaming application

Spark submit command

`spark2-submit --class com.cloudera.ps.StreamingPOC --conf spark.streaming.kafka.maxRatePerPartition=100 streamingpoc-1.0-SNAPSHOT-jar-with-dependencies.jar`

