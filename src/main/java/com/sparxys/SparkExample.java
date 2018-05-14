package com.sparxys;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

public class SparkExample {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("spark-example").setMaster("local").set("spark.cores.max", "10");
        JavaSparkContext sc = new JavaSparkContext(conf);

        DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd");
        Date date = new Date();
        String today = dateFormat.format(date);

        JavaRDD<String> data = sc.textFile("/home/finaxys/Projects/sparkJobs/input.csv");
        data.map(row -> row+=","+today).collect().forEach(System.out::println);

        sc.close();
    }

}
