package com.liss.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

object SparkRdd extends App {
  System.setProperty("hadoop.home.dir", "D:\\winutils-master\\hadoop-2.6.4")
  val spark = SparkSession.builder().master("local[1]").getOrCreate()

  val data = Array(1, 2, 3, 4, 5)
  val distData:RDD[Int] = spark.sparkContext.parallelize(data,2)

  val lines:RDD[String] = spark.sparkContext.textFile("D:/GitWorkspace/spark_test/src/main/resources/WordCount.txt")
  val dataFrame: DataFrame = spark.read.json("D:/GitWorkspace/spark_test/src/main/resources/person.json")
  dataFrame.show()
}
