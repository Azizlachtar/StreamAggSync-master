package com.example.projectstagevermegfinal.aggregationTopic;

import lombok.Builder;
import lombok.NoArgsConstructor;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import static org.apache.spark.sql.functions.*;
import static org.apache.spark.sql.types.DataTypes.StringType;

/**
 * This class defines aggregation logic for the 'Account' topic for both creation and update operations.
 */
@Builder
@NoArgsConstructor
public class AccountTopicAggregation implements AggregationLogicTopic {

    /**
     * Applies aggregation logic for creating an 'Account' dataset.
     *
     * @param dataset The input dataset to apply aggregation logic on.
     * @return A new dataset with applied aggregation logic for creation.
     */
    @Override
    public Dataset<Row> applyAggregationCreate(Dataset<Row> dataset) {
        // Define your aggregation logic for Customer data here
        return dataset
                .drop("topic");
    }

    /**
     * Applies aggregation logic for updating an 'Account' dataset.
     *
     * @param dataset The input dataset to apply aggregation logic on.
     * @return A new dataset with applied aggregation logic for update.
     */
    @Override
    public Dataset<Row> applyAggregationUpdate(Dataset<Row> dataset) {
        // Define your aggregation logic for Customer data here
        return  dataset.groupBy(col("id"))
                .agg(
                        last("status").as("status"),
                        sum("interest_application_day").cast(StringType).as("interest_application_day")
                )
                .drop("topic");
    }
}
