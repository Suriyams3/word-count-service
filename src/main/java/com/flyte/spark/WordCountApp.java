package com.flyte.spark;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Encoders;
import static org.apache.spark.sql.functions.*;

import java.util.Arrays;
import java.util.List;

public class WordCountApp {
    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                .appName("JavaSparkWordCount")
                .getOrCreate();

        List<String> data = Arrays.asList(
                "Flyte handles workflows",
                "Spark handles data",
                "Java handles the logic"
        );

        Dataset<String> df = spark.createDataset(data, Encoders.STRING());

        FlatMapFunction<String, String> splitWords = s ->
                Arrays.asList(s.toLowerCase().split(" ")).iterator();

        Dataset<Row> counts = df
                .flatMap(splitWords, Encoders.STRING())
                .filter((String s) -> !s.isEmpty()) // Explicit type fixes ambiguity
                .groupBy("value")
                .count()
                .orderBy(desc("count"));

        counts.show();
        spark.stop();
    }
}