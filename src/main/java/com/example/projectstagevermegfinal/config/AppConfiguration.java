/*


package com.example.projectstagevermegfinal.config;


import com.example.projectstagevermegfinal.services.ThoughtMachineStreamingService;
import com.example.projectstagevermegfinal.data.SchemaToMaintain;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;


*/
/**
 * Configuration class responsible for defining and configuring beans for the application.
 *//*


@Configuration
public class AppConfiguration {


*/
/**
     * Creates and configures the SparkStreamingSessionFactory bean.
     *
     * @return An instance of SparkStreamingSessionFactory.
     *//*



    @Bean
    public SparkStreamingSessionFactory sparkConfigBean(){
        return new SparkStreamingSessionFactory();
    }

*/
/**
     * Creates and configures the DatabaseConnection bean.
     *
     * @return An instance of DatabaseConnection.
     *//*



    @Bean
    public DatabaseConnection databaseConfigBean(){
        return new DatabaseConnection();
    }


*/
/**
     * Creates and configures the KafkaConsumer bean.
     *
     * @return An instance of KafkaConsumer.
     *//*



    @Bean
    public KafkaConsumer KafkaConfigBean(){
        return new KafkaConsumer();
    }


*/
/**
     * Creates and configures the ThoughtMachineStreamingService bean, which sets up and runs streaming queries
     * using Spark and Kafka.
     *
     * @return An instance of ThoughtMachineStreamingService.
     *//*


    @Bean
    public ThoughtMachineStreamingService streaming() {

        SchemaToMaintain customerSchema = SchemaToMaintain.Customer();
        SchemaToMaintain accountSchema = SchemaToMaintain.Account();
        SchemaToMaintain balanceSchema = SchemaToMaintain.Balance();
        SchemaToMaintain transactionSchema = SchemaToMaintain.Transaction();
        ThoughtMachineStreamingService consumer = new ThoughtMachineStreamingService(sparkConfigBean().getSparkSession(),KafkaConfigBean(),databaseConfigBean());
        consumer.runStreamingQuery(accountSchema);
        consumer.runStreamingQuery(customerSchema);
        consumer.runStreamingQuery(balanceSchema);
        consumer.runStreamingQuery(transactionSchema);


        return consumer;
    }
}


*/
