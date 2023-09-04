package com.example.projectstagevermegfinal.aggregationTopic;
import lombok.Builder;
import lombok.NoArgsConstructor;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import static org.apache.spark.sql.functions.last;

/**
 * This class defines aggregation logic for the 'Customer' topic for both creation and update operations.
 */
@Builder
@NoArgsConstructor
public class CustomerTopicAggregation implements AggregationLogicTopic{

    /**
     * Applies aggregation logic for creating a 'Customer' dataset.
     *
     * @param dataset The input dataset to apply aggregation logic on.
     * @return A new dataset with applied aggregation logic for creation.
     */
    @Override
    public  Dataset<Row>  applyAggregationCreate(Dataset<Row> dataset) {
        // Define your aggregation logic for Customer data here
        return dataset
                .drop("topic");
    }

    /**
     * Applies aggregation logic for updating a 'Customer' dataset.
     *
     * @param dataset The input dataset to apply aggregation logic on.
     * @return A new dataset with applied aggregation logic for update.
     */
    @Override
    public  Dataset<Row>  applyAggregationUpdate(Dataset<Row> dataset) {
        return dataset
                .groupBy("id")
                .agg(
                        last("first_name").as("first_name"),
                        last("last_name").as("last_name"),
                        last("gender").as("gender"),
                        last("nationality").as("nationality"),
                        last("email_address").as("email_address"),
                        last("topic").as("topic")
                )
                .drop("topic");
    }

}
