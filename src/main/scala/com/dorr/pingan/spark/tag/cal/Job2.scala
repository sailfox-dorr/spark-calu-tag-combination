package com.dorr.pingan.spark.tag.cal

import java.util
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}

import scala.collection.mutable.ListBuffer

object Job2 {
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "dorr")
    var dimension = 100;
    var parallelism = 20;
    var path = "puhui_num.csv"
    var partition = "202111"
    args.sliding(2, 2).toList.collect {
      case Array("--dimension", argDimension: String) => dimension = argDimension.toInt
      case Array("--parallelism", argParallelism: String) => parallelism = argParallelism.toInt
      case Array("--path", argPath: String) => path = argPath
      case Array("--path", argPartition: String) => partition = argPartition
    }
    val spark = SparkSession.builder().appName("tags cal")
      //      .master("local[10]")
      //      .config("hive.metastore.uris","thrift://localhost:9083")
      //      .config("spark.sql.warehouse.dir", "hdfs://localhost:8020/user/hive/warehouse/default/")
      .enableHiveSupport().getOrCreate()
    val tags: RDD[Int] = spark.sparkContext.parallelize((1 to dimension), 5)
    val tagv3 = tags.flatMap(num => {
      val list = ListBuffer[String]()
      for (i <- num to dimension) {
        for (j <- i to dimension) {
          list += s"${num}_${i}_${j}"
        }
      }
      list
    }
    ).repartition(parallelism)

    val rows: RDD[Row] = tagv3.mapPartitions(partition => {
      val start = System.currentTimeMillis()
      var max_key = ""
      var max_score = 0.0
      val dataBit = ListBuffer[UserTag]()
      val result = ListBuffer[Row]()
      for (line <- scala.io.Source.fromInputStream(getClass.getResourceAsStream("/" + path)).getLines()) {
        val split = line split ",\\["
        val part1 = split(0).split(",")
        val part2 = split(1).substring(0, split(1).length - 1)
        dataBit += new UserTag(part1(0), part1(1).toDouble, 1.0).setTags(part2);
      }
      val end = System.currentTimeMillis()
      partition.foreach(
        zuhe => {
          var fx = 0.0
          var zx = 0.0
          val zuhe3 = zuhe.split("_").map(_.toInt)
          var zuhe3bits = new util.BitSet()
          zuhe3.foreach(zuhe3bits.set)
          val zuheBase = zuhe3bits.clone().asInstanceOf[util.BitSet]
          val dataBitFilter = ListBuffer[UserTag]()
          for (userTag <- dataBit) {

            zuhe3bits and userTag.tagsBit
            if (zuhe3bits.cardinality() == 3) {
              dataBitFilter += userTag
              fx += userTag.fx
              zx += userTag.zx
            }

            zuhe3bits = zuheBase.clone().asInstanceOf[util.BitSet]

          }
          if (zx > 0.0) {
            result += Row(zuhe, fx, zx)
            fx = 0.0
            zx = 0.0
          }
          for (i4 <- zuhe3(2) to dimension) {
            var zuhe4 = zuheBase.clone().asInstanceOf[util.BitSet]
            zuhe4.set(i4)
            zuhe4.set(800)
            val clone = zuhe4.clone().asInstanceOf[util.BitSet]
            for (data <- dataBitFilter) {
              zuhe4 and data.tagsBit
              if (zuhe4.cardinality() == 4) {
                fx += data.fx
                zx += data.zx
              }
              zuhe4 = clone.clone().asInstanceOf[util.BitSet]
            }
            if (zx > 0.0) {
              result += Row(s"${zuhe}_${i4}", fx, zx)
            }
            if (zx > 0 && fx / zx > max_score) {
              max_score = fx / zx
              max_key = zuhe + "_" + i4
            }
            fx = 0.0
            zx = 0.0
          }
        }
      )
      result.iterator
    })
    val schema = StructType(Array(
      StructField("zuhe", StringType, nullable = false),
      StructField("sumfx", DoubleType, nullable = false),
      StructField("sumzx", DoubleType, nullable = false)))
    val df = spark.createDataFrame(rows, schema)

    df.createOrReplaceTempView("user_zuhe")
    spark.sql(s"insert overwrite table user_tags partition(dt=${partition})  select * from user_zuhe")
    //    df.write.mode(SaveMode.Overwrite).saveAsTable("user_tags")

    //    df.write.csv("/Users/dorr/Documents/workspace/2022/code/bigdata/source/self/spark-calu-tag-combination/src/main/resource/result.csv")
    //    tagv3
    //      .foreachPartition(partition => {
    //        val start = System.currentTimeMillis()
    //        var key = ""
    //        var score = 0.0
    //        val dataBit = ListBuffer[UserTag]()
    //        for (line <- scala.io.Source.fromInputStream(getClass.getResourceAsStream("/" + path)).getLines()) {
    //          val split = line split ",\\["
    //          val part1 = split(0).split(",")
    //          val part2 = split(1).substring(0, split(1).length - 1)
    //          dataBit += new UserTag(part1(0), part1(1).toDouble, 1.0).setTags(part2);
    //        }
    //        val end = System.currentTimeMillis()
    //        println(s"读取数据花费了${end - start} ms")
    //        // 可以使用redis 做存储
    //        partition.foreach(
    //          zuhe => {
    //            val zuhe3 = zuhe.split("_").map(_.toInt)
    //            var zuhe3bits = new util.BitSet()
    //            zuhe3.foreach(zuhe3bits.set)
    //            val zuheBase = zuhe3bits.clone().asInstanceOf[util.BitSet]
    //            val dataBitFilter = ListBuffer[UserTag]()
    //            for (userTag <- dataBit) {
    //              zuhe3bits and userTag.tagsBit
    //              if (zuhe3bits.cardinality() == 3) {
    //                dataBitFilter += userTag
    //              }
    //              zuhe3bits = zuheBase.clone().asInstanceOf[util.BitSet]
    //            }
    //            for (i4 <- zuhe3(2) to dimension) {
    //              var zuhe4 = zuheBase.clone().asInstanceOf[util.BitSet]
    //              zuhe4.set(i4)
    //              var fx = 0.0
    //              var zx = 0.0
    //              val clone = zuhe4.clone().asInstanceOf[util.BitSet]
    //              for (data <- dataBitFilter) {
    //                zuhe4 and data.tagsBit
    //                if (zuhe4.cardinality() == 4) {
    //                  fx += data.fx
    //                  zx += data.zx
    //                }
    //                zuhe4 = clone.clone().asInstanceOf[util.BitSet]
    //              }
    //              if (zx > 0 && fx / zx > score) {
    //                score = fx / zx
    //                key = zuhe + "_" + i4
    //              }
    //            }
    //            // 拿到分区内最大
    //            println(s"三级组合内最大: ${key}_${score}")
    //          }
    //        )
    //        println(s"分区内最大: ${key}_${score}")
    //      })
    spark.stop()
  }
}




