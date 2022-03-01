package com.jivega.etlbank.driver;
import com.jivega.etlbank.model.Balance;
import com.jivega.etlbank.pipeline.AnalyticsMoreBalance;
import org.apache.spark.sql.*;

public class Ingestion {
    public static String csvpath = "input/bank.csv";
    public static String parquetpath = "output/";
    public static void main(String[] args) {
        SparkSession spark = SparkSession
                .builder()
                .appName("etlbank-Ingestion")
                .master("local[4]")
                .getOrCreate();
        Dataset<Row> dsbank = spark.read().format("csv").option("header","true").load(csvpath);
        Dataset<Balance> dsBalance = dsbank.withColumnRenamed("default","defaultB").as(Encoders.bean(Balance.class));
        int numPartitions = getNumPartitions(dsbank);
        System.out.println("Schema dsBalance");
        dsBalance.printSchema();
        dsBalance.repartition(numPartitions).write().format("parquet").mode(SaveMode.Overwrite).save(parquetpath);


    }

    public static int getNumPartitions(Dataset ds) {
        //TODO Check the size of the Dataset and calculate the number of partitions accordingly
        return 1;
    }

}
