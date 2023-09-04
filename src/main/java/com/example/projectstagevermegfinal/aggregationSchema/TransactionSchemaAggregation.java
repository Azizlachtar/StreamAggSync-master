package com.example.projectstagevermegfinal.aggregationSchema;

import com.example.projectstagevermegfinal.config.DatabaseConnection;
import com.example.projectstagevermegfinal.config.SparkStreamingSessionFactory;
import lombok.Builder;
import lombok.NoArgsConstructor;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * This class defines aggregation logic for the 'Account' schema for both creation and update operations.
 */
@Builder
@NoArgsConstructor
public class TransactionSchemaAggregation implements AggregationLogicSchema {

    /**
     * Applies aggregation logic for creating an 'Account' dataset.
     *
     * @param dataset The input dataset to apply aggregation logic on.
     * @return A new dataset with applied aggregation logic for creation.
     */
    @Override
    public Dataset<Row> applyAggregationCreate(Dataset<Row> dataset) {
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
        return dataset
                .drop("topic");
    }



}
