package com.cloudera.ps


import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.kudu.spark.kudu._

/**
  * Created by ljiang on 5/3/17.
  */
object KuduPOC {
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder.
      master("local")
      .appName("KuduPOC")
      .getOrCreate()
    val kuduMasters = "ip-10-0-0-29.us-west-2.compute.internal:7051"
    val kuduTableName = "impala::default.positions_kudu"
    val kuduOptions: Map[String, String] = Map(
      "kudu.table"  -> kuduTableName,
      "kudu.master" -> kuduMasters)
/*    val sc = new SparkContext()
    val sqlContext = new SQLContext(sc)
    sqlContext.read.options(kuduOptions).kudu.show*/
    sparkSession.read.options(kuduOptions).kudu.show

  }

}
