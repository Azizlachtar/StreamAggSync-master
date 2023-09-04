package com.example.projectstagevermegfinal.data.definition;

import com.example.projectstagevermegfinal.utils.Constants;
import lombok.Data;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.apache.spark.sql.functions.col;

/**
 * Data definition class for defining the schema and configuration related to balances.
 */
@Data
public class BalanceDef {

    // Kafka topic names
    public static final String NEW_TOPIC_NAME = Constants.BALANCE_TOPIC_NAME;



    // Database table name
    public static final String TABLE_NAME=Constants.TABLE_NAME_BALANCE;

    // balance schema definition
    public static final StructType NEW_SCHEMA = DataTypes.createStructType(new StructField[] {
            DataTypes.createStructField("balance", DataTypes.createStructType(new StructField[] {
                    DataTypes.createStructField("id", DataTypes.StringType, false),
                    DataTypes.createStructField("account_id", DataTypes.StringType, false),
                    DataTypes.createStructField("asset", DataTypes.StringType, false),
                    DataTypes.createStructField("amount", DataTypes.StringType, false),
                    DataTypes.createStructField("denomination", DataTypes.StringType, false)
            }), false)
    });


    // Columns to select for new balances
    public static final Column[] NEW_SELECT_COLUMNS = {
            col("data.balance.id"),
            col("data.balance.account_id"),
            col("data.balance.asset"),
            col("data.balance.amount"),
            col("data.balance.denomination"),
            col("topic")
    };


    // List of column names for inserts and updates
    public static List<String> INSERT_COLUMN_NAMES = Arrays.asList("id", "account_id", "asset","amount","denomination");

    // Row key for the balance
    public static String ROW_KEY = "id";

}
