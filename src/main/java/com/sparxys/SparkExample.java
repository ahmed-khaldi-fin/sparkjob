package com.sparxys;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.current_date;

public class SparkExample {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("spark-example").setMaster("local").set("spark.cores.max", "10");
        JavaSparkContext sc = new JavaSparkContext(conf);

        SparkSession spark = SparkSession.builder().getOrCreate();
        spark.sql("set spark.sql.orc.impl=native");

        Dataset<Row> df = spark.read().option("header", "false").csv("/home/finaxys/Projects/sparkJobs/input.csv");
        Dataset<Row> df1 = df.withColumn("date", current_date());
        Dataset<Row> df2 = df1.withColumnRenamed("_c0", "index")
                              .withColumnRenamed("_c1", "column1")
                              .withColumnRenamed("_c2", "column2")
                              .withColumnRenamed("_c3", "column3")
                              .withColumnRenamed("_c4", "column4")
                              .withColumnRenamed("_c5", "column5")
                              .withColumnRenamed("_c6", "column6")
                              .withColumnRenamed("_c7", "column7");
        df2.write().mode(SaveMode.Append)
           .format("orc").partitionBy("date").save("spark-job");
        df2.show();
        sc.close();
    }

}
