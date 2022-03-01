package com.jivega.etlbank.pipeline;

import com.jivega.etlbank.model.Balance;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;

public class AnalyticsMoreLoans extends AnalyticsAbstract {

    public static void run(SparkSession spark, Dataset<Balance> dsbank) {
        System.out.println("moreLoans");
        Dataset<Row> dsagecont =  dsbank
                .withColumn("rangeAge", functions.concat(
                        dsbank.col("age" ).minus(dsbank.col("age" ).mod(delta)) ,
                        functions.lit(" - "),
                        dsbank.col("age" ).minus(dsbank.col("age" ).mod(delta)).plus(delta)))
                .groupBy("rangeAge").agg(
                        functions.sum(functions.when(dsbank.col("loan").equalTo("yes"),1).otherwise(0)).as("withloan"),
                        functions.sum(functions.lit(1)).as("total"))
                .withColumn("percent",functions.col("withloan").multiply(functions.lit(100)).divide(functions.col("total")));

        Row firstRow = dsagecont.orderBy(functions.col("percent").desc()).select(dsagecont.col("rangeAge"),dsagecont.col("percent")).first();
        System.out.println("El rango de edad con mas probabilidad de tener prestamos es: " + firstRow.getString(0));

    }
}
