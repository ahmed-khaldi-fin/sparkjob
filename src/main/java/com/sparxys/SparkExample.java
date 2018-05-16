package com.sparxys;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class SparkExample {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("spark-example").setMaster("local").set("spark.cores.max", "10");
        JavaSparkContext sc = new JavaSparkContext(conf);

        DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd");
        Date date = new Date();
        String today = dateFormat.format(date);

        //JavaRDD<String> data = sc.textFile("/home/finaxys/Projects/sparkJobs/input.csv");
        //data.map(row -> row+=","+today).take(8).forEach(System.out::println);
        
        SparkSession spark = SparkSession.builder().getOrCreate();
        spark.sql("set spark.sql.orc.impl=native");
        
        Dataset<Row> df = spark.read().csv("/home/finaxys/Projects/sparkJobs/input.csv");

        df.map((MapFunction<Row, String>) row -> row +","+today, Encoders.STRING()).write().format("orc").save("spark-job");

        sc.close();
    }

}
