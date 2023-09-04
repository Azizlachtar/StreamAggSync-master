package com.example.projectstagevermegfinal.aggregationTopic;

import com.example.projectstagevermegfinal.config.DatabaseConnection;
import lombok.Builder;
import lombok.NoArgsConstructor;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;

import static com.example.projectstagevermegfinal.utils.Constants.*;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.types.DataTypes.StringType;

/**
 * This class defines aggregation logic for the 'Account' topic for both creation and update operations.
 */
@Builder
public class TransactionTopicAggregation implements AggregationLogicTopic {


    /**
     * Applies aggregation logic for creating an 'Account' dataset.
     *
     * @param dataset The input dataset to apply aggregation logic on.
     * @return A new dataset with applied aggregation logic for creation.
     */
    @Override
    public Dataset<Row> applyAggregationCreate(Dataset<Row> dataset) {
        // Define your aggregation logic for Customer data here
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
        // Define your aggregation logic for Customer data here
        return dataset
                .drop("topic");
    }

    public Dataset<Row> readStreamFromTable() {

        SparkSession sparkSession= SparkSession.builder()
                .appName("ReadStream")
                .master("local[*]")
                .config("spark.sql.streaming.checkpointLocation", "D:/temp/checkpoint21")
                .getOrCreate();

        DatabaseConnection databaseConnection =new DatabaseConnection();


        Dataset<Row> transactions = sparkSession.read()
                .jdbc(databaseConnection.getUrl(), "transactions", databaseConnection.getProperties());
        Dataset<Row> balance = sparkSession.read()
                .jdbc(databaseConnection.getUrl(), "balance", databaseConnection.getProperties());

        // Calculate total transaction amount for 'fromaccount'
        Dataset<Row> totalTransactionAmountFromAccount = transactions
                .groupBy("fromaccount")
                .agg(functions.sum("amount").alias("amount"));

        // Calculate total transaction amount for 'toaccount'
        Dataset<Row> totalTransactionAmountToAccount = transactions
                .groupBy("toaccount")
                .agg(functions.sum("amount").alias("amount"));

        // Join balance with total transaction amount for 'fromaccount'
        Dataset<Row> joinedDataFromAccount = balance
                .join(totalTransactionAmountFromAccount, balance.col("account_id").equalTo(totalTransactionAmountFromAccount.col("fromaccount")), "inner")
                .withColumn("new_amount", balance.col("amount").minus(totalTransactionAmountFromAccount.col("amount")))
                .select(balance.col("id"), col("account_id").as("from_account_id"), col("asset").as("asset2"), col("new_amount").as("amount"), col("denomination").as("denomination2"))
                .cache(); // Cache the 'joinedDataFromAccount' DataFrame since it's reused


// Join balance with total transaction amount for 'toaccount'
        Dataset<Row> joinedDataToAccount = balance
                .join(totalTransactionAmountToAccount, balance.col("account_id").equalTo(totalTransactionAmountToAccount.col("toaccount")), "inner")
                .join(joinedDataFromAccount, balance.col("account_id").equalTo(joinedDataFromAccount.col("from_account_id")), "inner")
                .withColumn("new_amount", joinedDataFromAccount.col("amount").plus(totalTransactionAmountToAccount.col("amount")))
                .select(balance.col("id"), col("account_id"), col("asset"), col("new_amount").as("amount").cast(StringType), col("denomination"));

        joinedDataFromAccount.unpersist();

        return joinedDataToAccount;
    }


}
