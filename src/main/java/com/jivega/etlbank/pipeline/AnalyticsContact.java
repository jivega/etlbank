package com.jivega.etlbank.pipeline;

import com.jivega.etlbank.model.Balance;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;

public class AnalyticsContact extends AnalyticsAbstract{
    public static void run(SparkSession spark, Dataset<Balance> dsbank) {
        Dataset<Row> dsagg =  dsbank
                .withColumn("rangeAge", functions.concat(
                        dsbank.col("age" ).minus(dsbank.col("age" ).mod(delta)) ,
                        functions.lit(" - "),
                        dsbank.col("age" ).minus(dsbank.col("age" ).mod(delta)).plus(delta)))
                .groupBy("rangeAge","contact").agg(
                        functions.sum(functions.lit(1)).as("numContacts")
                );
        Row firstRow = dsagg
                .filter(dsagg.col("rangeAge").equalTo("25.0 - 30.0"))
                .orderBy(functions.col("numContacts").desc())
                .select(dsagg.col("contact"))
                .first();
        System.out.println("La forma mas comun de contactar entre 25 y 30 anyos es: " + firstRow.getString(0));

    }
}
