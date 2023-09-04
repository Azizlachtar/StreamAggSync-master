package com.example.projectstagevermegfinal.config;

import com.example.projectstagevermegfinal.utils.Constants;
import org.apache.spark.sql.SparkSession;
/**
 * Configuration class for setting up the Spark session used for streaming.
 */
public class SparkStreamingSessionFactory {
    private static final String appName= Constants.APP_NAME;
    private static final String checkpointLocation=Constants.CHECKPOINT_LOCATION;
    private static final String shufflePartitions="1";
    /**
     * Creates and configures a SparkSession instance for streaming.
     *
     * @return A configured SparkSession instance.
     */
    public SparkSession getSparkSession() {
        return SparkSession.builder()
                .appName(appName)
                .master("local[*]")
                .config("spark.sql.streaming.checkpointLocation", checkpointLocation)
                .config("spark.sql.shuffle.partitions", shufflePartitions)
                .getOrCreate();
    }
}
