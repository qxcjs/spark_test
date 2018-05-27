package com.liss.spark

import org.apache.spark.sql.SparkSession

object ScalaWordCount {

  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "D:\\hadoop-2.6.4")
    val spark = SparkSession.builder
      .appName("JavaWordCount")
      .master("local[1]")
      .getOrCreate
    spark.sparkContext.setCheckpointDir("")
    spark.sparkContext.getCheckpointDir
    val dataframe = spark.read.parquet("C:\\Users\\Administrator\\Desktop\\activiation.parquet")
    dataframe.checkpoint()
    dataframe.printSchema()
    dataframe.storageLevel.useMemory
    dataframe.repartition(1)
    dataframe.coalesce(1)
    dataframe.unpersist()
    println(dataframe.rdd.toDebugString)
    val lines = spark.sparkContext.textFile("D:/GitWorkspace/spark_test/src/main/resources/WordCount.txt")
    val counts = lines
      .flatMap(_.split(" "))
      .map((_, 1))
      .reduceByKey(_ + _)
    counts.foreach(println(_))
    println(counts.toDebugString)
    spark.stop()
  }
}
