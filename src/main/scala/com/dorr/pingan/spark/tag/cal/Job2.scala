package com.dorr.pingan.spark.tag.cal

import java.util

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.collection.mutable.ListBuffer
import scala.io.Source

object Job2 {
  def main(args: Array[String]): Unit = {
    var dimension = 100;
    var parallelism = 20;
    var path = "E:\\github\\My\\DataStructure\\src\\main\\resources\\puhui_num.csv"
    args.sliding(2, 2).toList.collect {
      case Array("--dimension", argDimension: String) => dimension = argDimension.toInt
      case Array("--parallelism", argParallelism: String) => parallelism = argParallelism.toInt
      case Array("--path", argPath: String) => path = argPath
    }
    val spark = SparkSession.builder().appName("tags cal")
//      .master("local[100]")
      .enableHiveSupport().getOrCreate()
    var dataBit = ListBuffer[UserTag]()
    for (line <- Source.fromFile(path, "utf-8").getLines()) {
      val split = line split ",\\{"
      val part1 = split(0).split(",")
      val part2 = split(1).substring(0, split(1).length - 1)
      dataBit += new UserTag(part1(0), part1(1).toDouble, 1.0).setTags(part2);
    }
    val dataBits = spark.sparkContext.broadcast(dataBit)
    val tags: RDD[Int] = spark.sparkContext.parallelize((1 to dimension), 5)
    tags.flatMap(num => {
      val list = ListBuffer[String]()
      for (i <- num to dimension) {
        for (j <- i to dimension) {
          list += s"${num}_${i}_${j}"
        }
      }
      list
    }
    ).repartition(parallelism)
      .foreachPartition(partition => {
//        val start = System.currentTimeMillis()

//        val end = System.currentTimeMillis()
        var key = ""
        var score = 0.0
//        println(s"读取数据花费了${end - start} ms")
        // 可以使用redis 做存储
        partition.foreach(
          zuhe => {
            val zuhe3 = zuhe.split("_")
            for (i4 <- zuhe3(2).toInt to dimension) {
              var zuhe4 = new util.BitSet()
              zuhe3.foreach(s => zuhe4.set(s.toInt))
              zuhe4.set(i4)
              var fx = 0.0
              var zx = 0.0
              val clone = zuhe4.clone().asInstanceOf[util.BitSet]
              for (data <- dataBits.value) {
                zuhe4 and data.tagsBit
                if (zuhe4.cardinality() == 4) {
                  fx += data.fx
                  zx += data.zx
                }
                zuhe4 = clone.clone().asInstanceOf[util.BitSet]
              }
              if (zx > 0 && fx / zx > score) {
                score = fx / zx
                key = zuhe + "_" + i4
                // 存储此数据进入redis
                println(s"${key}_${fx}_${zx}_${score}")
              }
            }
            // 拿到分区内最大
            //            println(s"${key}_${score}")
          }
        )
      })

    spark.stop()
  }
}




