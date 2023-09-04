package com.example.projectstagevermegfinal.utils;

import java.util.Arrays;
import java.util.List;

/**
 * This class contains constant values used throughout the application.
 */
public final class Constants {

    // Kafka topic names for customer and account events
    public static final String NEW_CUSTOMER_TOPIC_NAME = "createcustomer3";
    public static final String UPDATE_CUSTOMER_TOPIC_NAME = "updatecustomer3";
    public static final String NEW_ACCOUNT_TOPIC_NAME = "createaccount3";
    public static final String UPDATE_ACCOUNT_TOPIC_NAME = "updateaccount3";
    public static final String BALANCE_TOPIC_NAME = "createbalance3";
    public static final String TRANSACTION_TOPIC_NAME = "createtransaction3";
    public static final String PostingInstructionBatch_TOPIC_NAME="PostingInstructionBatch3";


    // Table names in the database
    public static final String TABLE_NAME_CUSTOMER="TM_CUSTOMERS3";
    public static final String TABLE_NAME_ACCOUNT="ACCOUNT3";
    public static final String TABLE_NAME_BALANCE="BALANCE";
    public static final String TABLE_NAME_TRANSACTION="TRANSACTIONS";


    //Database config
    public static final String DATABASE_URL="jdbc:postgresql://localhost:5432/vermeg";
    public static final String DATABASE_USERNAME="postgres";
    public static final String DATABASE_PASSWORD="aziz2000";


    // Kafka broker configuration
    public static final String KAFKA_BOOTSTRAP_SERVER="192.168.1.2:9092";

    // Checkpoint location for Spark streaming
    public static final String CHECKPOINT_LOCATION="D:/temp/checkpoint20";

    // Application name for Spark streaming
    public static final String APP_NAME="ThoughtMachineStreaming";

    // List of all Kafka topics
    public static final List<String> ALL_TOPICS = Arrays.asList(
            NEW_CUSTOMER_TOPIC_NAME,
            UPDATE_CUSTOMER_TOPIC_NAME,
            NEW_ACCOUNT_TOPIC_NAME,
            UPDATE_ACCOUNT_TOPIC_NAME,
            BALANCE_TOPIC_NAME,
            TRANSACTION_TOPIC_NAME,
            PostingInstructionBatch_TOPIC_NAME
    );
}
