package com.jivega.etlbank.driver;

import com.jivega.etlbank.model.Balance;
import com.jivega.etlbank.pipeline.*;
import org.apache.spark.sql.*;

public class Analytics {
    public static String parquetpath = "output/";
    public static int delta = 5;
    public static void main(String[] args) {
        SparkSession spark = SparkSession
                .builder()
                .appName("etlbank-AnalyticsMoreloansAge")
                .master("local[4]")
                .getOrCreate();
        Dataset<Balance> dsbank = spark.read().format("parquet").load(parquetpath).as(Encoders.bean(Balance.class));


        AnalyticsMoreLoans.run(spark,dsbank);
        AnalyticsMoreBalance.run(spark, dsbank);
        AnalyticsContact.run(spark, dsbank);
        AnalyticsCampaign.run(spark, dsbank);
        AnalyticsJob.run(spark, dsbank);
        spark.stop();
    }

}
