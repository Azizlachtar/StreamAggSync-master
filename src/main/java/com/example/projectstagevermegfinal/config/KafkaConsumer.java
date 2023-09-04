package com.example.projectstagevermegfinal.config;

import com.example.projectstagevermegfinal.utils.Constants;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.Arrays;
import java.util.List;

/**
 * Configuration class for consuming data from Kafka topics.
 */
public class KafkaConsumer {
    private static final List<String> topic = Constants.ALL_TOPICS;
    private static final String kafkaBootstrapServers =Constants.KAFKA_BOOTSTRAP_SERVER;


    /**
     * Consumes data from Kafka topics and returns a Dataset of rows.
     *
     * @param spark The SparkSession to use for streaming.
     * @return A Dataset of rows containing consumed data.
     */
    public Dataset<Row>  consumeStream(SparkSession spark){

        String kafkaTopicString = String.join(",", topic);
        return  spark
                .readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", kafkaBootstrapServers)
                .option("subscribe", kafkaTopicString)
                .option("startingOffsets", "earliest") //possible values : earliest, latest
                .load()
                .selectExpr("CAST(value AS STRING)","topic");

    }


}
