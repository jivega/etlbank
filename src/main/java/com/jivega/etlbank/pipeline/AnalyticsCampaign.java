package com.jivega.etlbank.pipeline;

import com.jivega.etlbank.model.Balance;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;

public class AnalyticsCampaign extends AnalyticsAbstract{
    public static void run(SparkSession spark, Dataset<Balance> dsbank) {
        Dataset<Row> dsagg =  dsbank
                .groupBy("campaign","marital","job")
                .agg(
                        functions.avg(dsbank.col("balance")).as("avgBalance"),
                        functions.max(dsbank.col("balance")).as("maxBalance"),
                        functions.min(dsbank.col("balance")).as("minBalance")
                )
                .select("campaign","marital","job","avgBalance","maxBalance","minBalance")
                .orderBy("campaign","marital","job");
        System.out.println("Los balances medios, maximos y minimos por campanya marital y trabajo son:  "  );
        dsagg.show(100);
    }
}
