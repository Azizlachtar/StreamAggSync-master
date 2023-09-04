package com.example.projectstagevermegfinal.data.definition;

import com.example.projectstagevermegfinal.utils.Constants;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.Arrays;
import java.util.List;

import static org.apache.spark.sql.functions.col;

/**
 * Definition class for customer data schema and related constants.
 */
public class CustomerDef {

    // Kafka topic names
    public static final String NEW_TOPIC_NAME = Constants.NEW_CUSTOMER_TOPIC_NAME;
    public static final String UPDATE_TOPIC_NAME = Constants.UPDATE_CUSTOMER_TOPIC_NAME;

    // Database table name
    public static final String TABLE_NAME=Constants.TABLE_NAME_CUSTOMER;


    // New customer schema definition
    public static final StructType NEW_SCHEMA = DataTypes.createStructType(new StructField[]{
            DataTypes.createStructField("customer", DataTypes.createStructType(new StructField[]{
                    DataTypes.createStructField("id", DataTypes.StringType, true),
                    DataTypes.createStructField("customer_details", DataTypes.createStructType(
                                    new StructField[]{
                                            DataTypes.createStructField("first_name", DataTypes.StringType, true),
                                            DataTypes.createStructField("last_name", DataTypes.StringType, true),
                                            DataTypes.createStructField("gender", DataTypes.StringType, true),
                                            DataTypes.createStructField("nationality", DataTypes.StringType, true),
                                            DataTypes.createStructField("email_address", DataTypes.StringType, true)
                                    }),
                            true)
            }), true)
    });

    // Updated customer schema definition
    public static final StructType UPDATED_SCHEMA = DataTypes.createStructType(new StructField[]{
            DataTypes.createStructField("id", DataTypes.StringType, true),
            DataTypes.createStructField("customer_details", DataTypes.createStructType(new StructField[]{
                    DataTypes.createStructField("first_name", DataTypes.StringType, true),
                    DataTypes.createStructField("last_name", DataTypes.StringType, true),
                    DataTypes.createStructField("gender", DataTypes.StringType, true),
                    DataTypes.createStructField("nationality", DataTypes.StringType, true),
                    DataTypes.createStructField("email_address", DataTypes.StringType, true),
            }), true)
    });

    // Columns to select for new customers
    public static final Column[] NEW_SELECT_COLUMNS = {
            col("data.customer.id"),
            col("data.customer.customer_details.first_name"),
            col("data.customer.customer_details.last_name"),
            col("data.customer.customer_details.gender"),
            col("data.customer.customer_details.nationality"),
            col("data.customer.customer_details.email_address"),
            col("topic")
    };

    // Columns to select for customer updates
    public static final Column[] UPDATE_SELECT_COLUMNS = {
            col("data.id"),
            col("data.customer_details.first_name"),
            col("data.customer_details.last_name"),
            col("data.customer_details.gender"),
            col("data.customer_details.nationality"),
            col("data.customer_details.email_address"),
            col("topic")
    };



    // List of column names for inserts and updates
    public static List<String> INSERT_COLUMN_NAMES = Arrays.asList("id", "first_name", "last_name", "gender", "nationality", "email_address");
    public static List<String> UPDATE_COLUMN_NAMES = Arrays.asList("id", "first_name", "last_name", "gender", "nationality", "email_address");

    // Row key for the customer
    public static String ROW_KEY = "id";

}
