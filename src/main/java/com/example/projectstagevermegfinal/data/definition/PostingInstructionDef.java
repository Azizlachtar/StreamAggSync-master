package com.example.projectstagevermegfinal.data.definition;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.Arrays;
import java.util.List;

import static com.example.projectstagevermegfinal.utils.Constants.PostingInstructionBatch_TOPIC_NAME;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.explode;

public class PostingInstructionDef {
    public static final String NEW_TOPIC_NAME = PostingInstructionBatch_TOPIC_NAME;
    public static final String UPDATE_TOPIC_NAME = NEW_TOPIC_NAME;

    public static final StructType COMMITTED_POSTINGS_STRUCT = DataTypes.createStructType(
            new StructField[]{
                    DataTypes.createStructField("credit", DataTypes.StringType, true),
                    DataTypes.createStructField("amount", DataTypes.StringType, true),
                    DataTypes.createStructField("denomination", DataTypes.StringType, true),
                    DataTypes.createStructField("account_id", DataTypes.StringType, true),
                    DataTypes.createStructField("account_address", DataTypes.StringType, true),
                    DataTypes.createStructField("asset", DataTypes.StringType, true),
                    DataTypes.createStructField("phase", DataTypes.StringType, true)
            });

    public static final StructType POSTING_INSTRUCTIONS_STRUCT = DataTypes.createStructType(
            new StructField[]{
                    DataTypes.createStructField("id", DataTypes.StringType, true),
                    DataTypes.createStructField("client_transaction_id", DataTypes.StringType, true),
                    DataTypes.createStructField("committed_postings", DataTypes.createArrayType(COMMITTED_POSTINGS_STRUCT), true)
            });

    public static final StructType NEW_SCHEMA = DataTypes.createStructType(new StructField[]{
            DataTypes.createStructField("posting_instruction_batch", DataTypes.createStructType(new StructField[]{
                    DataTypes.createStructField("posting_instructions", DataTypes.createArrayType(POSTING_INSTRUCTIONS_STRUCT), true),
            }), true)
    });



    public static final StructType UPDATED_SCHEMA = NEW_SCHEMA;

    public static final Column[] NEW_SELECT_COLUMNS = {
            col("data.posting_instruction_batch.posting_instructions.id").as("id"),
            col("data.posting_instruction_batch.posting_instructions.client_transaction_id").as("client_transaction_id"),
            explode( col("data.posting_instruction_batch.posting_instructions.committed_postings")).as("committed_postings"),
            col("committed_postings.credit").alias("credit"),
            col("committed_postings.amount").alias("amount"),
            col("committed_postings.denomination").alias("denomination"),
            col("committed_postings.account_id").alias("account_id"),
            col("committed_postings.account_address").alias("account_address"),
            col("committed_postings.asset").alias("asset"),
            col("committed_postings.phase").alias("phase"),
            col("topic")
    };

    public static final Column[] UPDATE_SELECT_COLUMNS = NEW_SELECT_COLUMNS;

    public static List<String> INSERT_COLUMN_NAMES = Arrays.asList("id", "client_transaction_id", "credit","amount","denomination","account_id","account_address","asset", "phase");
    public static List<String> UPDATE_COLUMN_NAMES = INSERT_COLUMN_NAMES;
    public static String ROW_KEY = "id";
}
