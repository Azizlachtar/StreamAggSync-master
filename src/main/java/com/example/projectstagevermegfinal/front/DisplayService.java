package com.example.projectstagevermegfinal.front;

import com.example.projectstagevermegfinal.config.DatabaseConnection;
import com.example.projectstagevermegfinal.config.SparkStreamingSessionFactory;
import com.example.projectstagevermegfinal.front.DTO.AccountDTO;
import com.example.projectstagevermegfinal.front.DTO.CustomerDTO;
import com.example.projectstagevermegfinal.utils.Constants;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.springframework.stereotype.Service;

import java.lang.reflect.Field;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.spark.sql.functions.col;


/**
 * Service class for retrieving customer and account data.
 */
@Service
public class DisplayService {
    private final SparkStreamingSessionFactory sparkSession;
    private final DatabaseConnection databaseConfig;


    public DisplayService(SparkStreamingSessionFactory sparkSession, DatabaseConnection databaseConfig) {
        this.sparkSession = sparkSession;
        this.databaseConfig = databaseConfig;
    }

    /**
     * Retrieves a list of customer data transfer objects.
     *
     * @return A list of CustomerDTO objects representing customer data.
     */
    public List<CustomerDTO> getCustomers() {
        Dataset<Row> customersStream = readStreamFromTable(Constants.TABLE_NAME_CUSTOMER);

        return customersStream.collectAsList().stream()
                .map(row -> convertRowToDTO(row, CustomerDTO.class))
                .collect(Collectors.toList());
    }

    /**
     * Retrieves a list of account data transfer objects for a given customer ID.
     *
     * @param customerId The ID of the customer.
     * @return A list of AccountDTO objects representing account data for the customer.
     */
    public List<AccountDTO> getCustomerAccounts(String customerId) {
        Dataset<Row> customerAccountsStream = readStreamFromTable(Constants.TABLE_NAME_ACCOUNT);

        return customerAccountsStream.filter(col("stakeholder_ids").contains(customerId))
                .collectAsList().stream()
                .map(row -> convertRowToDTO(row, AccountDTO.class))
                .collect(Collectors.toList());
    }
    /**
     * Converts a Spark Row to a DTO object using reflection.
     *
     * @param row      The Spark Row containing data.
     * @param dtoClass The DTO class to convert to.
     * @param <T>      The type of DTO.
     * @return The populated DTO object.
     */

    private <T> T convertRowToDTO(Row row, Class<T> dtoClass) {
        T dto = null;
        try {
            dto = dtoClass.getDeclaredConstructor().newInstance();

            StructType schema = row.schema();
            for (StructField field : schema.fields()) {
                String fieldName = field.name();
                Object fieldValue = row.getAs(fieldName);

                Field dtoField = dtoClass.getDeclaredField(fieldName);
                dtoField.setAccessible(true);
                dtoField.set(dto, fieldValue);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return dto;
    }

   /* private CustomerDTO convertRowToCustomerDTO(Row row) {
        CustomerDTO customerDTO = new CustomerDTO();
        customerDTO.setId(row.getAs("id"));
        customerDTO.setFirstName(row.getAs("first_name"));
        customerDTO.setLastName(row.getAs("last_name"));
        return customerDTO;
    }

    private AccountDTO convertRowToAccountDTO(Row row) {
        AccountDTO accountDTO = new AccountDTO();
        accountDTO.setId(row.getAs("id"));
        accountDTO.setName(row.getAs("name"));
        accountDTO.setPermittedDenominations(row.getAs("permitted_denominations"));
        accountDTO.setStatus(row.getAs("status"));
        accountDTO.setStakeholderIds(row.getAs("stakeholder_ids"));
        accountDTO.setArrangedOverdraftLimit(row.getAs("arranged_overdraft_limit"));
        accountDTO.setInterestApplicationDay(row.getAs("interest_application_day"));
        return accountDTO;
    }*/

    /**
     * Retrieves the name of a customer based on the customer ID.
     *
     * @param customerId The ID of the customer.
     * @return The name of the customer.
     */
    public String getCustomerName(String customerId) {
        Dataset<Row> customersStream = readStreamFromTable(Constants.TABLE_NAME_CUSTOMER);

        Row customerRow = customersStream.filter(customersStream.col("id").equalTo(customerId)).first();
        if (customerRow != null) {
            return customerRow.getAs("first_name") + " " + customerRow.getAs("last_name");
        } else {
            return "Unknown Customer";
        }
    }

    /**
     * Reads data from a database table using Spark.
     *
     * @param table The name of the table.
     * @return The Spark Dataset containing the table data.
     */
    private Dataset<Row> readStreamFromTable(String table) {
        return sparkSession.getSparkSession().read()
                .jdbc(
                        databaseConfig.getUrl(),
                        table,
                        databaseConfig.getProperties()
                );
    }
}
