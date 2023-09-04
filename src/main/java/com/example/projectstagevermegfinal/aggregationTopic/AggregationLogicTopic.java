package com.example.projectstagevermegfinal.aggregationTopic;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

/**
 * This interface defines the aggregation logic for topics.
 * Implementing classes must provide methods to apply aggregation logic for creation and update operations.
 */
public interface AggregationLogicTopic {
    /**
     * Applies aggregation logic for creating a dataset.
     *
     * @param dataset The input dataset to apply aggregation logic on.
     * @return A new dataset with applied aggregation logic for creation.
     */
    Dataset<Row> applyAggregationCreate(Dataset<Row> dataset);

    /**
     * Applies aggregation logic for updating a dataset.
     *
     * @param dataset The input dataset to apply aggregation logic on.
     * @return A new dataset with applied aggregation logic for update.
     */
    Dataset<Row> applyAggregationUpdate(Dataset<Row> dataset);
}
