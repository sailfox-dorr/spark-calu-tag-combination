package com.dorr.pingan.spark.tags;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class SparkSQLDemo {

    public static void main(String[] args) {

        String warehouseLocation = "hdfs://localhost:8020/user/hive/warehouses/";

        SparkSession spark = SparkSession.builder().appName
                        ("Java Spark Hive Example")
                .master("local[4]")
                .config("spark.sql.warehouse.dir", warehouseLocation)
                .enableHiveSupport()
                .getOrCreate();

        Dataset<Row> df = spark.sql("show tables");
        df.show();
//        Dataset<Row> df2 = spark.sql("SELECT * FROM test2");
//        df2.show();
//        Dataset<Row> df3 = spark.sql("SELECT id,name FROM test1 where name = 'lucy'");
//        System.out.println("#############name = lucy  size:"+ df3.count());
        spark.stop();


    }}
