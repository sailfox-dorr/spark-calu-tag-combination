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
    var path = "puhui_num.csv"
    args.sliding(2, 2).toList.collect {
      case Array("--dimension", argDimension: String) => dimension = argDimension.toInt
      case Array("--parallelism", argParallelism: String) => parallelism = argParallelism.toInt
      case Array("--path", argPath: String) => path = argPath
    }
    val spark = SparkSession.builder().appName("tags cal")
      .master("local[10]")
      .enableHiveSupport().getOrCreate()
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
        val start = System.currentTimeMillis()
        var key = ""
        var score = 0.0
        val dataBit = ListBuffer[UserTag]()
        for (line <- scala.io.Source.fromInputStream(getClass.getResourceAsStream("/" + path)).getLines()) {
          val split = line split ",\\["
          val part1 = split(0).split(",")
          val part2 = split(1).substring(0, split(1).length - 1)
          dataBit += new UserTag(part1(0), part1(1).toDouble, 1.0).setTags(part2);
        }
        val end = System.currentTimeMillis()
        println(s"读取数据花费了${end - start} ms")
        // 可以使用redis 做存储
        partition.foreach(
          zuhe => {
            val zuhe3 = zuhe.split("_").map(_.toInt)
            var zuhe3bits = new util.BitSet()
            zuhe3.foreach(zuhe3bits.set)
            val zuheBase = zuhe3bits.clone().asInstanceOf[util.BitSet]
            val dataBitFilter = ListBuffer[UserTag]()
            for (userTag <- dataBit) {
              zuhe3bits and userTag.tagsBit
              if (zuhe3bits.cardinality() == 3) {
                dataBitFilter += userTag
              }
              zuhe3bits = zuheBase.clone().asInstanceOf[util.BitSet]
            }
            for (i4 <- zuhe3(2) to dimension) {
              var zuhe4 = zuheBase.clone().asInstanceOf[util.BitSet]
              zuhe4.set(i4)
              var fx = 0.0
              var zx = 0.0
              val clone = zuhe4.clone().asInstanceOf[util.BitSet]
              for (data <- dataBitFilter) {
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
              }
            }
            // 拿到分区内最大
            println(s"三级组合内最大: ${key}_${score}")
          }
        )
        println(s"分区内最大: ${key}_${score}")
      })

    spark.stop()
  }
}




