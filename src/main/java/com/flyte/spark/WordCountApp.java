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
        // Initialize Spark Session
        SparkSession spark = SparkSession.builder()
                .appName("JavaSparkWordCount")
                .getOrCreate();

        // 1. Create sample data
        List<String> data = Arrays.asList(
                "Flyte handles workflows",
                "Spark handles data",
                "Java handles the logic"
        );

        Dataset<String> df = spark.createDataset(data, Encoders.STRING());

        // 2. Define the FlatMapFunction explicitly to avoid ambiguity errors
        FlatMapFunction<String, String> splitWords = s ->
                Arrays.asList(s.toLowerCase().split(" ")).iterator();

        // 3. Transformation Logic
        Dataset<Row> counts = df
                .flatMap(splitWords, Encoders.STRING())
                .filter(s -> !s.isEmpty())
                .groupBy("value")
                .count()
                .orderBy(desc("count"));

        // 4. Output result to console
        counts.show();

        spark.stop();
    }
}