
package com.example.projectstagevermegfinal.services;


import com.example.projectstagevermegfinal.SchemaForeachWriter;
import com.example.projectstagevermegfinal.config.DatabaseConnection;
import com.example.projectstagevermegfinal.config.KafkaConsumer;
import com.example.projectstagevermegfinal.data.SchemaToMaintain;
import io.vavr.Tuple2;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.streaming.Trigger;
import org.apache.spark.sql.types.StructType;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.from_json;

/**
 * ThoughtMachineStreamingService is responsible for processing streaming data from Kafka,
 * preparing and executing streaming queries based on the provided schema.
 */
@Slf4j
public class ThoughtMachineStreamingService {

    private final SparkSession spark;
    private final KafkaConsumer kafkaConsumer;
    private final DatabaseConnection databaseConnection;

    /**
     * Constructor for initializing the ThoughtMachineStreamingService.
     *
     * @param sparkSession        The Spark session.
     * @param kafkaConsumer       The Kafka consumer.
     * @param databaseConnection  The database connection.
     */
    public ThoughtMachineStreamingService(SparkSession sparkSession, KafkaConsumer kafkaConsumer, DatabaseConnection databaseConnection) {
        this.spark = sparkSession;
        this.kafkaConsumer = kafkaConsumer;
        this.databaseConnection = databaseConnection;
    }

    /**
     * Run the streaming queries for the provided schema.
     *
     * @param schemaToMaintain The schema configuration to maintain.
     */
    public void runStreamingQuery(final SchemaToMaintain schemaToMaintain) {
        try {

            Dataset<Row> mainStream = kafkaConsumer.consumeStream(spark);

            SchemaForeachWriter schemaForeachWriter = new SchemaForeachWriter(
                    databaseConnection,
                    schemaToMaintain
            );

            waitQueriesTermination(
                    prepareStreamingQueries(
                            prepareDatasets(mainStream, schemaToMaintain.getTopicSchemaStreamDefinitions()),
                            schemaForeachWriter,
                            schemaToMaintain)
            );
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * Wait for streaming queries to terminate.
     *
     * @param queries The list of streaming queries to wait for.
     */
    private void waitQueriesTermination(final List<StreamingQuery> queries) {
        Executors.newSingleThreadExecutor().submit(() -> queries.forEach(query -> {
            try {
                query.awaitTermination();
            } catch (StreamingQueryException e) {
                log.error(e.getMessage());
            }
        }));
    }

    /**
     * Prepare and return a list of streaming queries.
     *
     * @param datasets            The list of datasets to process.
     * @param schemaForeachWriter The custom ForeachWriter implementation.
     * @param schemaToMaintain    The schema configuration to maintain.
     * @return List of prepared streaming queries.
     */
    private List<StreamingQuery> prepareStreamingQueries(
            final List<Dataset<Row>> datasets,
            final SchemaForeachWriter schemaForeachWriter,
            SchemaToMaintain schemaToMaintain) {

        return datasets.stream()
                .map(p -> prepareStreamingQuery(p, schemaForeachWriter,schemaToMaintain))
                .collect(Collectors.toList());
    }

    /**
     * Prepare datasets based on the main stream and topic schema definitions.
     *
     * @param mainStream               The main input stream.
     * @param topicSchemaStreamDefinitions    List of topic schema definitions.
     * @return List of prepared datasets.
     */
    private List<Dataset<Row>> prepareDatasets(
            final Dataset<Row> mainStream,
            final List<Tuple2<StructType, Column[]>> topicSchemaStreamDefinitions) {
        return topicSchemaStreamDefinitions.stream()
                .map(topicSchemaStreamDefinition ->
                        readDatasetByTopic(mainStream, topicSchemaStreamDefinition)
                )
                .collect(Collectors.toList());
    }

    /**
     * Prepare a streaming query based on the dataset, custom ForeachWriter, and schema.
     *
     * @param dataset             The dataset to process.
     * @param schemaForeachWriter The custom ForeachWriter implementation.
     * @param schemaToMaintain    The schema configuration to maintain.
     * @return The prepared streaming query.
     */
    private StreamingQuery prepareStreamingQuery(
            final Dataset<Row> dataset,
            final SchemaForeachWriter schemaForeachWriter,
            SchemaToMaintain schemaToMaintain) {

        List<String> columnNamesList = Arrays.asList(dataset.schema().fieldNames());
        Dataset<Row> aggregatedDataset = null;

        if ((columnNamesList.size() - 1) == schemaToMaintain.getInsertColumns().size()) {
            aggregatedDataset = schemaToMaintain.applyAggregationSchemaCreate(dataset);

        } else if ((columnNamesList.size() - 1) == schemaToMaintain.getUpdateColumns().size()) {
            aggregatedDataset = schemaToMaintain.applyAggregationSchemaUpdate(dataset);
        }
        assert aggregatedDataset != null;
        return aggregatedDataset
                .drop("topic")
                .writeStream()
                .foreach(schemaForeachWriter)
                .outputMode("update")
                .trigger(Trigger.ProcessingTime("10 seconds"))
                .start();
    }

    /**
     * Read a dataset by applying transformations based on the topic schema definition.
     *
     * @param mainStream           The main input stream.
     * @param updateCustomerStreamDef The topic schema definition.
     * @return The prepared dataset.
     */
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

