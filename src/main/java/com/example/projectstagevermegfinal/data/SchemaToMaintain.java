package com.example.projectstagevermegfinal.data;



import com.example.projectstagevermegfinal.aggregationSchema.*;
import com.example.projectstagevermegfinal.aggregationTopic.*;
import com.example.projectstagevermegfinal.data.definition.*;
import io.vavr.Tuple2;
import lombok.Builder;
import lombok.Data;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Represents a schema definition and related components for maintaining data.
 */
@Builder
@Data
public class SchemaToMaintain {
    private final String tableName;

    private final String rowKeyColumn;
    private final List<String> insertColumns;
    private final List<String> updateColumns;

    private final KafkaStream insertKafkaStream;
    private final KafkaStream updateKafkaStream;

    private final AggregationLogicSchema aggregationLogicSchema;

    private final AggregationLogicTopic aggregationLogicTopic;



    /**
     * Factory method for creating a customer schema definition.
     *
     * @return A SchemaToMaintain instance for customer data.
     */
    public static SchemaToMaintain Customer() {
        return SchemaToMaintain.builder()
                .tableName(CustomerDef.TABLE_NAME)
                .rowKeyColumn(CustomerDef.ROW_KEY)
                .insertColumns(CustomerDef.INSERT_COLUMN_NAMES)
                .updateColumns(CustomerDef.UPDATE_COLUMN_NAMES)
                .insertKafkaStream(KafkaStream.builder()
                        .topicName(CustomerDef.NEW_TOPIC_NAME)
                        .columns(CustomerDef.NEW_SELECT_COLUMNS)
                        .structType(CustomerDef.NEW_SCHEMA)
                        .build())
                .updateKafkaStream(KafkaStream.builder()
                        .topicName(CustomerDef.UPDATE_TOPIC_NAME)
                        .columns(CustomerDef.UPDATE_SELECT_COLUMNS)
                        .structType(CustomerDef.UPDATED_SCHEMA)
                        .build())
                .aggregationLogicSchema(CustomerSchemaAggregation.builder()
                        .build())
                .aggregationLogicTopic(CustomerTopicAggregation.builder()
                        .build())
                .build();
    }

    /**
     * Factory method for creating an account schema definition.
     *
     * @return A SchemaToMaintain instance for account data.
     */

    public static SchemaToMaintain Account() {
        return SchemaToMaintain.builder()
                .tableName(AccountDef.TABLE_NAME)
                .rowKeyColumn(AccountDef.ROW_KEY)
                .insertColumns(AccountDef.INSERT_COLUMN_NAMES)
                .updateColumns(AccountDef.UPDATE_COLUMN_NAMES)
                .insertKafkaStream(KafkaStream.builder()
                        .topicName(AccountDef.NEW_TOPIC_NAME)
                        .columns(AccountDef.NEW_SELECT_COLUMNS)
                        .structType(AccountDef.NEW_SCHEMA)
                        .build())
                .updateKafkaStream(KafkaStream.builder()
                        .topicName(AccountDef.UPDATE_TOPIC_NAME)
                        .columns(AccountDef.UPDATE_SELECT_COLUMNS)
                        .structType(AccountDef.UPDATED_SCHEMA)
                        .build())
                .aggregationLogicSchema(AccountSchemaAggregation.builder()
                        .build())
                .aggregationLogicTopic(AccountTopicAggregation.builder()
                        .build())
                .build();
    }

    /**
     * Factory method for creating a balance schema definition.
     *
     * @return A SchemaToMaintain instance for balance data.
     */

    public static SchemaToMaintain Balance() {
        return SchemaToMaintain.builder()
                .tableName(BalanceDef.TABLE_NAME)
                .rowKeyColumn(BalanceDef.ROW_KEY)
                .insertColumns(BalanceDef.INSERT_COLUMN_NAMES)
                .updateColumns(BalanceDef.INSERT_COLUMN_NAMES)
                .insertKafkaStream(KafkaStream.builder()
                        .topicName(BalanceDef.NEW_TOPIC_NAME)
                        .columns(BalanceDef.NEW_SELECT_COLUMNS)
                        .structType(BalanceDef.NEW_SCHEMA)
                        .build())
                .aggregationLogicSchema(BalanceSchemaAggregation.builder()
                        .build())
                .aggregationLogicTopic(BalanceTopicAggregation.builder()
                        .build())
                .build();
    }

    /**
     * Factory method for creating a transaction schema definition.
     *
     * @return A SchemaToMaintain instance for transaction data.
     */

    public static SchemaToMaintain Transaction() {
        return SchemaToMaintain.builder()
                .tableName(TransactionDef.TABLE_NAME)
                .rowKeyColumn(TransactionDef.ROW_KEY)
                .insertColumns(TransactionDef.INSERT_COLUMN_NAMES)
                .updateColumns(TransactionDef.INSERT_COLUMN_NAMES)
                .insertKafkaStream(KafkaStream.builder()
                        .topicName(TransactionDef.NEW_TOPIC_NAME)
                        .columns(TransactionDef.NEW_SELECT_COLUMNS)
                        .structType(TransactionDef.NEW_SCHEMA)
                        .build())
                .aggregationLogicSchema(TransactionSchemaAggregation.builder()
                        .build())
                .aggregationLogicTopic(TransactionTopicAggregation.builder()
                        .build())
                .build();
    }

    /**
     * Factory method for creating a PostingInstruction schema definition.
     *
     * @return A SchemaToMaintain instance for PostingInstruction data.
     */

   /* public static SchemaToMaintain PostingInstruction(final String table) {
        return SchemaToMaintain.builder()
                .tableName(table)
                .rowKeyColumn(PostingInstructionDef.ROW_KEY)
                .insertColumns(PostingInstructionDef.INSERT_COLUMN_NAMES)
                .updateColumns(PostingInstructionDef.UPDATE_COLUMN_NAMES)
                .insertKafkaStream(KafkaStream.builder()
                        .topicName(PostingInstructionDef.NEW_TOPIC_NAME)
                        .columns(PostingInstructionDef.NEW_SELECT_COLUMNS)
                        .structType(PostingInstructionDef.NEW_SCHEMA)
                        .build())
                .updateKafkaStream(KafkaStream.builder()
                        .topicName(PostingInstructionDef.UPDATE_TOPIC_NAME)
                        .columns(PostingInstructionDef.UPDATE_SELECT_COLUMNS)
                        .structType(PostingInstructionDef.UPDATED_SCHEMA)
                        .build())
                .aggregation(new PostingInstructionAggregation())
                .build();
    }*/

    /**
     * Returns a comma-separated string of topic names.
     *
     * @return Comma-separated topic names.
     */
    public String getTopics() {
        return String.format("%s,%s",
                insertKafkaStream.getTopicName(),
                updateKafkaStream.getTopicName());
    }

    /**
     * Returns a list of schema and column definitions for topic streams.
     *
     * @return List of Tuple2 objects containing StructType and Column[].
     */
    public List<Tuple2<StructType, Column[]>> getTopicSchemaStreamDefinitions() {
        List<Tuple2<StructType, Column[]>> topicSchemaStreamDefinitions = new ArrayList<>();

        // Always include the insertKafkaStream definition
        topicSchemaStreamDefinitions.add(
                new Tuple2<>(this.getInsertKafkaStream().getStructType(), this.getInsertKafkaStream().getColumns())
        );

        // Only include the updateKafkaStream definition if it's available
        if (this.getUpdateKafkaStream() != null) {
            topicSchemaStreamDefinitions.add(
                    new Tuple2<>(this.getUpdateKafkaStream().getStructType(), this.getUpdateKafkaStream().getColumns())
            );
        }

        return topicSchemaStreamDefinitions;
    } 


    /**
     * Applies aggregation logic for creating new data based on schema.
     *
     * @param dataset Input dataset.
     * @return Dataset with applied aggregation logic.
     */
    public Dataset<Row> applyAggregationSchemaCreate(Dataset<Row> dataset) {
        return aggregationLogicSchema.applyAggregationCreate(dataset);
    }

    /**
     * Applies aggregation logic for updating existing data based on schema.
     *
     * @param dataset Input dataset.
     * @return Dataset with applied aggregation logic.
     */
    public Dataset<Row> applyAggregationSchemaUpdate(Dataset<Row> dataset ) {
        return aggregationLogicSchema.applyAggregationUpdate(dataset);
    }

    /**
     * Applies aggregation logic for creating new data based on Topic.
     *
     * @param dataset Input dataset.
     * @return Dataset with applied aggregation logic.
     */
    public Dataset<Row> applyAggregationTopicCreate(Dataset<Row> dataset) {
        return aggregationLogicTopic.applyAggregationCreate(dataset);
    }

    /**
     * Applies aggregation logic for updating existing data based on Topic.
     *
     * @param dataset Input dataset.
     * @return Dataset with applied aggregation logic.
     */
    public Dataset<Row> applyAggregationTopicUpdate(Dataset<Row> dataset) {
        return aggregationLogicTopic.applyAggregationUpdate(dataset);
    }




}
