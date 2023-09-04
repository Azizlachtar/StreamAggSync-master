package com.example.projectstagevermegfinal.ASupprimer;

import com.example.projectstagevermegfinal.config.DatabaseConnection;
import com.example.projectstagevermegfinal.config.KafkaConsumer;
import com.example.projectstagevermegfinal.config.SparkStreamingSessionFactory;
import com.example.projectstagevermegfinal.data.SchemaToMaintain;
import com.example.projectstagevermegfinal.services.ThoughtMachineStreamingService2;
import io.vavr.Tuple2;
import org.apache.spark.sql.*;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.streaming.Trigger;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import javax.annotation.PostConstruct;
import javax.xml.crypto.Data;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.example.projectstagevermegfinal.utils.Constants.*;
import static org.apache.spark.sql.functions.*;
import static org.apache.spark.sql.types.DataTypes.StringType;

@Configuration
public class AppConfiguration2 {


    @Bean
    public SparkStreamingSessionFactory sparkConfigBean() {
        return new SparkStreamingSessionFactory();
    }

    @Bean
    public DatabaseConnection databaseConfigBean() {
        return new DatabaseConnection();
    }

    @Bean
    public KafkaConsumer KafkaConfigBean() {
        return new KafkaConsumer();
    }
    @Bean
    public ThoughtMachineStreamingService2 streaming2() {

        SchemaToMaintain customerSchema = SchemaToMaintain.Customer();
        SchemaToMaintain accountSchema = SchemaToMaintain.Account();
        SchemaToMaintain balanceSchema = SchemaToMaintain.Balance();
        SchemaToMaintain transactionSchema = SchemaToMaintain.Transaction();
        ThoughtMachineStreamingService2 consumer = new ThoughtMachineStreamingService2(sparkConfigBean().getSparkSession(), KafkaConfigBean(), databaseConfigBean());
        consumer.runStreamingQuery(customerSchema);
        consumer.runStreamingQuery(accountSchema);
        consumer.runStreamingQuery(balanceSchema);
        consumer.runStreamingQuery(transactionSchema);

        return consumer;
    }



    public void streaming() throws StreamingQueryException {

        // readDatasetByTopic

        Dataset<Row> mainStream = KafkaConfigBean().consumeStream(sparkConfigBean().getSparkSession());

        SchemaToMaintain customerSchema = SchemaToMaintain.Customer();
        SchemaToMaintain accountSchema = SchemaToMaintain.Account();

        List<Tuple2<StructType, Column[]>> topicSchemaStreamDefinitions1 = Arrays.asList(
                new Tuple2<>(customerSchema.getInsertKafkaStream().getStructType(), customerSchema.getInsertKafkaStream().getColumns()),
                new Tuple2<>(customerSchema.getUpdateKafkaStream().getStructType(), customerSchema.getUpdateKafkaStream().getColumns())
        );

        List<Tuple2<StructType, Column[]>> topicSchemaStreamDefinitions2 = Arrays.asList(
                new Tuple2<>(accountSchema.getInsertKafkaStream().getStructType(), accountSchema.getInsertKafkaStream().getColumns()),
                new Tuple2<>(accountSchema.getUpdateKafkaStream().getStructType(), accountSchema.getUpdateKafkaStream().getColumns())
        );

        Tuple2<StructType, Column[]> firstTuple = topicSchemaStreamDefinitions2.get(1);


        Dataset<Row> aa = mainStream.select(
                        from_json(col("value"), firstTuple._1()).as("data"),
                        col("topic").as("topic") // Replace with actual topic column name
                )
                .select(firstTuple._2())
                .na()
                .drop();

        aa
                .writeStream()
                .foreachBatch((batchDF, batchId) -> {
                    String firstRow = batchDF.select("topic").first().getString(0);

                    System.out.println(firstRow);

                    if (firstRow.equals("updateaccount3")) {

                        Dataset<Row> aggregatedDF = batchDF.groupBy(col("id"))
                                .agg(
                                        last("status").as("status"),
                                        sum("interest_application_day").cast(StringType).as("interest_application_day")
                                );

                        aggregatedDF.show(); // Or any other action you want to perform with the aggregated data
                    } else {
                        batchDF.show();
                    }
                })
                .outputMode("update")
                .trigger(Trigger.ProcessingTime("4 seconds"))
                .start();

        StructType schema = DataTypes.createStructType(new StructField[]{
                DataTypes.createStructField("topic", DataTypes.StringType, false)
        });
        Row row = RowFactory.create("createaccount3");
        Dataset<Row> dataset = sparkConfigBean().getSparkSession().createDataFrame(Collections.singletonList(row), schema);

        // mainStream.writeStream().format("console").start();

        Dataset<Row> dd = aa
                .select("topic")
                .distinct();


        /////////////////////////////////////////////////

    }

    private Dataset<Row> readDatasetByTopic(
            final Dataset<Row> mainStream,
            final Tuple2<StructType, Column[]> updateCustomerStreamDef) {
        return mainStream.select(from_json(col("value"), updateCustomerStreamDef._1()).as("data"), col("topic"))
                .select(updateCustomerStreamDef._2())
                .na()
                .drop();
    }


    private Dataset<Row> createCustomerAggregation(Dataset<Row> input) {
        // Apply your "create customer" aggregation logic
        return input.groupBy("id")
                .agg(
                        last("first_name").as("first_name"),
                        last("last_name").as("last_name"),
                        last("gender").as("gender"),
                        last("nationality").as("nationality"),
                        last("email_address").as("email_address")
                );
    }

    private Dataset<Row> updateCustomerAggregation(Dataset<Row> input) {
        // Apply your "update customer" aggregation logic
        return input.groupBy("id")
                .agg(
                        last("first_name").as("first_name"),
                        last("last_name").as("last_name"),
                        last("gender").as("gender"),
                        last("nationality").as("nationality"),
                        last("email_address").as("email_address")
                );


    }

    private Dataset<Row> createAccountAggregation(Dataset<Row> input) {
        // Apply your "update customer" aggregation logic
        return input;


    }

    private Dataset<Row> updateAccountAggregation(Dataset<Row> input) {
        // Apply your "update customer" aggregation logic
        return input.groupBy("id")
                .agg(
                        last("status").as("status"),
                        sum("interest_application_day").cast(StringType).as("interest_application_day")
                );


    }
}

