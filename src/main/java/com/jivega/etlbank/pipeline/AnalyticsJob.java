package com.jivega.etlbank.pipeline;

import com.jivega.etlbank.model.Balance;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;

public class AnalyticsJob extends AnalyticsAbstract{
    public static void run(SparkSession spark, Dataset<Balance> dsbank) {
        Dataset<Row> dsagg =  dsbank
                .filter(
                        dsbank.col("marital").equalTo("married")
                                .and(dsbank.col("housing").equalTo("yes"))
                                .and(dsbank.col("balance").gt(1200))
                                .and(dsbank.col("campaign").equalTo(3))
                )
                .groupBy("job").agg(
                        functions.sum(functions.lit(1)).as("numJobs")
                );
        Row firstRow = dsagg.orderBy(functions.col("numJobs").desc()).select(dsagg.col("job")).first();
        System.out.println("El trabajo mas comun entre los casados, con casa propia y con mas de 1.200 euros y de la campanya 3 es:" + firstRow.getString(0));
    }
}
