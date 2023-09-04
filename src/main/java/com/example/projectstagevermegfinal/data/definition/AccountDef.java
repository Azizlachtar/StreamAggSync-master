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
 * Data definition class for defining the schema and configuration related to accounts.
 */
@Data
public class AccountDef {

    // Kafka topic names
    public static final String NEW_TOPIC_NAME = Constants.NEW_ACCOUNT_TOPIC_NAME;
    public static final String UPDATE_TOPIC_NAME = Constants.UPDATE_ACCOUNT_TOPIC_NAME;

    // Database table name
    public static final String TABLE_NAME=Constants.TABLE_NAME_ACCOUNT;

    // New account schema definition
    public static final StructType NEW_SCHEMA = DataTypes.createStructType(new StructField[]{
            DataTypes.createStructField("account_created", DataTypes.createStructType(new StructField[]{
                    DataTypes.createStructField("account", DataTypes.createStructType(
                            new StructField[]{
                                    DataTypes.createStructField("id", DataTypes.StringType, true),
                                    DataTypes.createStructField("name", DataTypes.StringType, true),
                                    DataTypes.createStructField("permitted_denominations", DataTypes.StringType, true),
                                    DataTypes.createStructField("status", DataTypes.StringType, true),
                                    DataTypes.createStructField("stakeholder_ids", DataTypes.StringType, true),
                                    DataTypes.createStructField("instance_param_vals", DataTypes.createStructType(new StructField[]{
                                            DataTypes.createStructField("arranged_overdraft_limit", DataTypes.StringType, true),
                                            DataTypes.createStructField("interest_application_day", DataTypes.StringType, true)
                                    }), true)
                            }), true)
            }), true)
    });

    // Updated account schema definition
    public static final StructType UPDATED_SCHEMA = DataTypes.createStructType(new StructField[]{
            DataTypes.createStructField("account_update_updated", DataTypes.createStructType(new StructField[]{
                    DataTypes.createStructField("account_update", DataTypes.createStructType(
                            new StructField[]{
                                    DataTypes.createStructField("account_id", DataTypes.StringType, true),
                                    DataTypes.createStructField("status", DataTypes.StringType, true),
                                    DataTypes.createStructField("instance_param_vals_update", DataTypes.createStructType(new StructField[]{
                                            DataTypes.createStructField("instance_param_vals", DataTypes.createStructType(new StructField[]{
                                                    DataTypes.createStructField("interest_application_day", DataTypes.StringType, true)
                                            }), true)
                                    }), true),
                            }), true)
            }), true)
    });

    // Columns to select for new accounts
    public static final Column[] NEW_SELECT_COLUMNS = {
            col("data.account_created.account.id"),
            col("data.account_created.account.name"),
            col("data.account_created.account.permitted_denominations"),
            col("data.account_created.account.status"),
            col("data.account_created.account.stakeholder_ids"),
            col("data.account_created.account.instance_param_vals.arranged_overdraft_limit"),
            col("data.account_created.account.instance_param_vals.interest_application_day"),
            col("topic")
    };

    // Columns to select for account updates
    public static final Column[] UPDATE_SELECT_COLUMNS = {
            col("data.account_update_updated.account_update.account_id").as("id"),
            col("data.account_update_updated.account_update.status"),
            col("data.account_update_updated.account_update.instance_param_vals_update.instance_param_vals.interest_application_day"),
            col("topic")
    };


    // List of column names for inserts and updates
    public static List<String> INSERT_COLUMN_NAMES = Arrays.asList("id", "name", "permitted_denominations","status","stakeholder_ids","arranged_overdraft_limit","interest_application_day");
    public static List<String> UPDATE_COLUMN_NAMES = Arrays.asList("id", "status", "interest_application_day");

    // Row key for the account
    public static String ROW_KEY = "id";

}
