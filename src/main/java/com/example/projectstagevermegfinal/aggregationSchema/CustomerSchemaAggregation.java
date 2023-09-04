package com.example.projectstagevermegfinal.aggregationSchema;

import lombok.Builder;
import lombok.NoArgsConstructor;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import static org.apache.spark.sql.functions.last;

@Builder
@NoArgsConstructor
public class CustomerSchemaAggregation implements AggregationLogicSchema {
    @Override
    public Dataset<Row> applyAggregationCreate(Dataset<Row> dataset) {
        return dataset
                .drop("topic");
    }

    @Override
    public Dataset<Row> applyAggregationUpdate(Dataset<Row> dataset) {
        return dataset
                .groupBy("id")
                .agg(
                        last("first_name").as("first_name"),
                        last("last_name").as("last_name"),
                        last("gender").as("gender"),
                        last("nationality").as("nationality"),
                        last("email_address").as("email_address")
                )
                .drop("topic");
    }

}
