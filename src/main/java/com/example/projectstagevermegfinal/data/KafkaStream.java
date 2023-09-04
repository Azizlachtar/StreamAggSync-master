package com.example.projectstagevermegfinal.data;

import lombok.Builder;
import lombok.Data;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.types.StructType;

/**
 * Represents a Kafka stream configuration for a specific topic.
 */
@Builder
@Data
public class KafkaStream {
    private final String topicName;
    private final Column[] columns;
    private final StructType structType;

}
