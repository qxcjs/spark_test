package com.liss.spark

import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel

object CacheAndCheckpoint extends App {
  System.setProperty("hadoop.home.dir", "D:\\hadoop-2.6.4")
  private val spark: SparkSession = SparkSession.builder().master("local[2]").getOrCreate()
  spark.sparkContext.setCheckpointDir("/tmp/checkpoint/")
  val rdd = spark.sparkContext.parallelize(1 to 10).map(x => (x % 3, 1)).reduceByKey(_ + _)

  rdd.cache()
  //   cache/persist
  println("cache/persist debug")
  val indCache = rdd.mapValues(_ > 4)
  indCache.persist(StorageLevel.DISK_ONLY)
  println(indCache.toDebugString)
  println(indCache.count)
  println(indCache.toDebugString)

  // checkpoint
  println("checkpoint debug")
  val indChk = rdd.mapValues(_ > 4)
  println(indChk.cache)
  println(indChk.checkpoint)
  println(indChk.toDebugString)
  println(indChk.count)
  println(indChk.toDebugString)
}
