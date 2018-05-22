package com.liss.spark

import org.apache.spark.sql.SparkSession

object A8SparkTest {
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "D:\\hadoop-2.6.4")
    val spark = SparkSession.builder.getOrCreate
    spark.sql("select * from parquet.`/tmp/data/withdraw_record/*.parquet`").createOrReplaceTempView("withdraw_record")
    spark.sql("select * from withdraw_record").show()
    spark.stop()
    SparkSession.builder.config("hive.merge.mapfiles",true).enableHiveSupport().getOrCreate()
  }

}
