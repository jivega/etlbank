package com.jivega.etlbank.pipeline;

import com.jivega.etlbank.model.Balance;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;

public class AnalyticsMoreBalance extends AnalyticsAbstract{

    public static void run(SparkSession spark, Dataset<Balance> dsbank) {
        Dataset<Row> dsbalance =  dsbank
                .withColumn("rangeAge", functions.concat(
                        dsbank.col("age" ).minus(dsbank.col("age" ).mod(delta)) ,
                        functions.lit(" - "),
                        dsbank.col("age" ).minus(dsbank.col("age" ).mod(delta)).plus(delta)))
                .groupBy("rangeAge","marital").agg(
                        functions.avg(dsbank.col("balance")).as("avgBalance")
                        );
        Row firstRow = dsbalance.orderBy(functions.col("avgBalance").desc()).select(dsbalance.col("rangeAge"),dsbalance.col("avgBalance")).first();
        System.out.println("El rango de edad con mas balance es: " + firstRow.getString(0));
    }
}
