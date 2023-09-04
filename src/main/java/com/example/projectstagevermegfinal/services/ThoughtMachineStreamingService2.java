package com.example.projectstagevermegfinal.services;


import com.example.projectstagevermegfinal.SchemaForeachWriter;
import com.example.projectstagevermegfinal.config.DatabaseConnection;
import com.example.projectstagevermegfinal.config.KafkaConsumer;
import com.example.projectstagevermegfinal.data.SchemaToMaintain;
import io.vavr.Tuple2;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.*;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.streaming.Trigger;
import org.apache.spark.sql.types.StructType;

import java.util.List;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

import static org.apache.spark.sql.functions.*;
import static org.apache.spark.sql.types.DataTypes.StringType;

@Slf4j
public class ThoughtMachineStreamingService2 {

    private final SparkSession spark;
    private final KafkaConsumer kafkaConsumer;
    private final DatabaseConnection databaseConnection;


    public ThoughtMachineStreamingService2(SparkSession sparkSession, KafkaConsumer kafkaConsumer, DatabaseConnection databaseConnection) {
        this.spark = sparkSession;
        this.kafkaConsumer = kafkaConsumer;
        this.databaseConnection = databaseConnection;
    }


    public void runStreamingQuery(final SchemaToMaintain schemaToMaintain) {
        try {

            Dataset<Row> mainStream = kafkaConsumer.consumeStream(spark);

            SchemaForeachWriter schemaForeachWriter = new SchemaForeachWriter(
                    databaseConnection,
                    schemaToMaintain
            );

            List<StreamingQuery> queries = prepareStreamingQueries(
                    prepareDatasets(mainStream,
                            schemaToMaintain.getTopicSchemaStreamDefinitions())
                    , schemaForeachWriter,
                    schemaToMaintain);

            waitQueriesTermination(queries);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void waitQueriesTermination(final List<StreamingQuery> queries) {
        Executors.newSingleThreadExecutor().submit(() -> queries.forEach(query -> {
            try {
                query.awaitTermination();
            } catch (StreamingQueryException e) {
                log.error(e.getMessage());
            }
        }));
    }

    private List<StreamingQuery> prepareStreamingQueries(
            final List<Dataset<Row>> datasets,
            final SchemaForeachWriter schemaForeachWriter,
            SchemaToMaintain schemaToMaintain) {

        return datasets.stream()
                .map(p -> prepareStreamingQuery(p, schemaForeachWriter,schemaToMaintain))
                .collect(Collectors.toList());
    }

    private List<Dataset<Row>> prepareDatasets(
            final Dataset<Row> mainStream,
            final List<Tuple2<StructType, Column[]>> topicSchemaStreamDefinitions) {
        return topicSchemaStreamDefinitions.stream()
                .map(topicSchemaStreamDefinition ->
                        readDatasetByTopic(mainStream, topicSchemaStreamDefinition)
                )
                .collect(Collectors.toList());
    }

    private StreamingQuery prepareStreamingQuery(
            final Dataset<Row> dataset,
            final SchemaForeachWriter schemaForeachWriter,
            SchemaToMaintain schemaToMaintain) {


        return dataset
                .writeStream()
                .foreachBatch((batchDF, batchId) -> {
                    if (!batchDF.isEmpty()) {
                        Dataset<Row> aggregatedDataset = null;
                        String topicName = batchDF.select("topic").first().getString(0);

                        if (topicName.equals(schemaToMaintain.getInsertKafkaStream().getTopicName())) {

                            aggregatedDataset=schemaToMaintain.applyAggregationTopicCreate(batchDF);

                        }else  {

                            aggregatedDataset=schemaToMaintain.applyAggregationTopicUpdate(batchDF);
                        }

                        schemaForeachWriter.open(batchId, System.currentTimeMillis());

                        assert aggregatedDataset != null;
                        for (Row row : aggregatedDataset.collectAsList()) {

                                schemaForeachWriter.process(row);
                            }
                            // customForeachWriter.close(null);


                    }

                })
                .outputMode("update")
                .trigger(Trigger.ProcessingTime("10 seconds"))
                .start();

    }

    private static Dataset<Row> Aggregation(Dataset<Row> batchDF) {
        if (!batchDF.isEmpty()) {

            String firstRow = batchDF.select("topic").first().getString(0);

            System.out.println(firstRow);
            switch (firstRow) {
                case "updateaccount3":
                    return batchDF.groupBy(col("id"))
                            .agg(
                                    last("status").as("status"),
                                    sum("interest_application_day").cast(StringType).as("interest_application_day")
                            ).drop("topic");
                // Or any other action you want to perform with the aggregated data
                case "createaccount3":
                    return batchDF.drop("topic");

                case "createcustomer3":
                    return batchDF.drop("topic");

                default:
                    return batchDF.groupBy("id")
                            .agg(
                                    last("first_name").as("first_name"),
                                    last("last_name").as("last_name"),
                                    last("gender").as("gender"),
                                    last("nationality").as("nationality"),
                                    last("email_address").as("email_address"),
                                    last("topic").as("topic")
                            ).drop("topic");
            }
        }
        return batchDF;
    }



        private Dataset<Row> readDatasetByTopic(
            final Dataset<Row> mainStream,
            final Tuple2<StructType, Column[]> updateCustomerStreamDef) {
        return mainStream.select(from_json(col("value"), updateCustomerStreamDef._1()).as("data"),
                        col("topic").as("topic"))
                .select(updateCustomerStreamDef._2())
                .na()
                .drop();
    }


}
