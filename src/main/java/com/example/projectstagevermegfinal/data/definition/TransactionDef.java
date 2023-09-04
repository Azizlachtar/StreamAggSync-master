package com.example.projectstagevermegfinal.data.definition;

import com.example.projectstagevermegfinal.utils.Constants;
import lombok.Data;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.Arrays;
import java.util.List;

import static org.apache.spark.sql.functions.col;

/**
 * Data definition class for defining the schema and configuration related to balances.
 */
@Data
public class TransactionDef {

    // Kafka topic names
    public static final String NEW_TOPIC_NAME = Constants.TRANSACTION_TOPIC_NAME;



    // Database table name
    public static final String TABLE_NAME=Constants.TABLE_NAME_TRANSACTION;

    // balance schema definition
    public static final StructType NEW_SCHEMA = DataTypes.createStructType(new StructField[] {
            DataTypes.createStructField("transaction", DataTypes.createStructType(new StructField[] {
                    DataTypes.createStructField("id", DataTypes.StringType, false),
                    DataTypes.createStructField("balance_id", DataTypes.StringType, false),
                    DataTypes.createStructField("amount", DataTypes.StringType, false),
                    DataTypes.createStructField("toAccount", DataTypes.StringType, false),
                    DataTypes.createStructField("fromAccount", DataTypes.StringType, false),
                    DataTypes.createStructField("timestamp", DataTypes.StringType, false),
                    DataTypes.createStructField("status", DataTypes.StringType, false)
            }), false)
    });


    // Columns to select for new balances
    public static final Column[] NEW_SELECT_COLUMNS = {
            col("data.transaction.id"),
            col("data.transaction.balance_id"),
            col("data.transaction.amount"),
            col("data.transaction.toAccount"),
            col("data.transaction.fromAccount"),
            col("data.transaction.timestamp"),
            col("data.transaction.status"),
            col("topic")
    };


    // List of column names for inserts and updates
    public static List<String> INSERT_COLUMN_NAMES = Arrays.asList("id", "balance_id","amount","toAccount","fromAccount","timestamp","status");

    // Row key for the balance
    public static String ROW_KEY = "id";

}
