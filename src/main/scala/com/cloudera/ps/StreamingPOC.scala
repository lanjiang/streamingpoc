package com.cloudera.ps

/**
  * Created by ljiang on 12/13/16.
  */
import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.streaming.StreamingContext._
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kudu.spark.kudu.KuduContext
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.kudu.spark.kudu._


case class SimpleSecurities(
                        as_of_date                     : String,
                        security_code_primary          : String,
                        as_of_timestamp                : Long,
                        security_code_type_primary     : String,
                        mkt_price                      : Double
                           )
object StreamingPOC {

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("SparkStreamingPOC")

    val ssc = new StreamingContext(conf, Seconds(5))
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "ip-10-0-0-130.us-west-2.compute.internal:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "StreamingKuduPOC",
      "auto.offset.reset" -> "earliest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )
    val sparkSession = SparkSession.builder.getOrCreate()

    val topics = Array("securities")
    val stream = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams)
    )

    stream.map(record =>  record.value).
      map(_.split(",")).map(array => array.map(_.trim)).
      foreachRDD(rdd => updateKudu(rdd))


    ssc.start()
    ssc.awaitTermination()

  }

  def updateKudu(rdd: RDD[Array[String]]): Unit = {
      val sparkSession = SparkSession.builder.getOrCreate()
      import sparkSession.implicits._
      val securitiesDF = rdd.map(a => SimpleSecurities(a(0), a(1), a(2).toLong, a(3), a(4).toDouble)).toDF()


      val kuduMasters = "ip-10-0-0-29.us-west-2.compute.internal:7051"
      val positionRawTableName = "impala::default.positions_kudu_raw"
      val positionValueTableName = "impala::default.positions_kudu_value"
      val securitiesTableName = "impala::default.securities_kudu"

      //deduplicate based on timestamp for each security. Only keep the latest timestamp for each microbatch
      val securitiesLatestDF = securitiesDF.groupBy($"security_code_primary").agg(max($"as_of_timestamp").alias("as_of_timestamp"))
      val securitiesFinalDF = securitiesDF.join(securitiesLatestDF, Seq("security_code_primary", "as_of_timestamp"))
      securitiesFinalDF.cache()
      securitiesFinalDF.show()

      // Upsert securities_kudu table
      val kuduContext = new KuduContext(kuduMasters)
      kuduContext.upsertRows(securitiesFinalDF, securitiesTableName)

      // Calculate Positions Value and update them
      val kuduOptions: Map[String, String] = Map(
        "kudu.table"  -> positionRawTableName,
        "kudu.master" -> kuduMasters)

      val positionsDF = sparkSession.read.options(kuduOptions).kudu.
        select("as_of_date", "portfolio_code", "security_code_primary", "units")

      val securitiesPriceDF = securitiesFinalDF.
        select("as_of_date", "security_code_primary", "mkt_price")
      val positionsUpdateDF = positionsDF.join(securitiesPriceDF, Seq("security_code_primary", "as_of_date")).
        withColumn("position_value", $"mkt_price"* $"units").
        select("as_of_date", "portfolio_code", "security_code_primary", "position_value")

      kuduContext.upsertRows(positionsUpdateDF, positionValueTableName)

  }
}
