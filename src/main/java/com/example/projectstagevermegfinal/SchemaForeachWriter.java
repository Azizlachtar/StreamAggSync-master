package com.example.projectstagevermegfinal;


import com.example.projectstagevermegfinal.config.DatabaseConnection;
import com.example.projectstagevermegfinal.data.SchemaToMaintain;
import com.example.projectstagevermegfinal.querry.*;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.ForeachWriter;
import org.apache.spark.sql.Row;

import java.sql.*;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * CustomForeachWriter is a class that implements the Spark ForeachWriter interface
 * for writing data to a database based on the provided schema.
 */
@Slf4j
public class SchemaForeachWriter extends ForeachWriter<Row> {


    private final DatabaseConnection databaseConnection;
    private final String table;
    private final List<String> insertColumns;
    private final List<String> updateColumns;
    private final String rowKeyColumn;
    private Connection connection;
    private PreparedStatement upsertStatement;
    private PreparedStatement upsertIfExistStatement;
    private PreparedStatement insertStatement;
    private PreparedStatement updateStatement;

    /**
     * Constructor for initializing the SchemaForeachWriter.
     *
     * @param databaseConnection The database connection.
     * @param schema             The schema configuration to maintain.
     */
    public SchemaForeachWriter(
            final DatabaseConnection databaseConnection,
            final SchemaToMaintain schema) {
        this.databaseConnection=databaseConnection;
        this.table = schema.getTableName();
        this.insertColumns = schema.getInsertColumns();
        this.updateColumns = schema.getUpdateColumns();
        this.rowKeyColumn = schema.getRowKeyColumn();
    }

    /**
     * Open method to initialize database connections and statements.
     *
     * @param l1   The partition ID.
     * @param l       The epoch ID.
     * @return True if the open operation succeeds, false otherwise.
     */
    @Override
    public boolean open(long l, long l1) {
        try {
            connection = DriverManager.getConnection(databaseConnection.getUrl(), databaseConnection.getProperties());
            connection.setAutoCommit(false);

            upsertStatement = connection.prepareStatement(toUpsertQuery(updateColumns));
            upsertIfExistStatement = connection.prepareStatement(toUpsertQuery(insertColumns));
            insertStatement = connection.prepareStatement(toInsertQuery());
            updateStatement = connection.prepareStatement(toUpdateQuery());

            return true;
        } catch (SQLException e) {
            log.error("Error opening the database connection: {}", e.getMessage());
            return false;
        }
    }

    /**
     * Process method to handle each incoming row of data.
     *
     * @param row The row of data to process.
     */
    @Override
    public void process(Row row) {
        try {
            PreparedStatement preparedStatement = prepareStatement(row);
            log.info(getSqlFromPreparedStatement(preparedStatement));

            preparedStatement.executeUpdate();
            connection.commit();
        } catch (SQLException e) {
            log.error("Error processing the row: {}", e.getMessage());
            rollbackProcess();
        }
    }


    /**
     * Close method to release resources.
     *
     * @param throwable The throwable if an exception occurred during processing, null otherwise.
     */
    @Override
    public void close(Throwable throwable) {
        try {
            if (upsertStatement != null) {
                upsertStatement.close();
            }

            if (insertStatement != null) {
                insertStatement.close();
            }

            if (updateStatement != null) {
                updateStatement.close();
            }

            if (upsertIfExistStatement != null) {
                upsertIfExistStatement.close();
            }

            if (connection != null) {
                connection.close();
            }
        } catch (SQLException e) {
            log.error("Error closing resources: {}", e.getMessage());
        }
    }

    /**
     * Rollback the database transaction in case of an error.
     */
    private void rollbackProcess() {
        try {
            connection.rollback();
        } catch (SQLException ex) {
            log.error("Error rolling back the transaction: {}", ex.getMessage());
        }
    }


    /**
     * Prepare the SQL statement based on the schema and row data.
     *
     * @param row The row of data to prepare the statement for.
     * @return The prepared SQL statement.
     * @throws SQLException If an SQL exception occurs.
     */
    private PreparedStatement prepareStatement(Row row) throws SQLException {


        if (insertColumns.size() == updateColumns.size()) {
            System.out.println("/////////////////////////////////////////////");
            System.out.println("upsertStatement");
            int length = row.schema().fields().length;
            System.out.println(length);
            System.out.println(insertColumns.size());
            System.out.println("/////////////////////////////////////////////");
            return toUpsertPreparedStatement(row, upsertStatement);
        }

        if (insertColumns.size() == row.schema().fields().length) {

            if (!isExist(row.getAs(rowKeyColumn))) {
                System.out.println("/////////////////////////////////////////////");
                System.out.println("upsertIfExistStatement");
                int length2 = row.schema().fields().length;
                System.out.println(length2);
                System.out.println(insertColumns.size());
                System.out.println("/////////////////////////////////////////////");
                return toUpsertPreparedStatement(row, insertStatement);
                //return toUpsertPreparedStatement(row, upsertIfExistStatement);
            }
            System.out.println("/////////////////////////////////////////////");
            System.out.println("insertStatement");
            int length3 = row.schema().fields().length;
            System.out.println(length3);
            System.out.println(insertColumns.size());
            System.out.println("/////////////////////////////////////////////");
            //return toUpsertPreparedStatement(row, insertStatement);
        }
        System.out.println("/////////////////////////////////////////////");
        System.out.println("updateStatement");
        int length3 = row.schema().fields().length;
        System.out.println(length3);
        System.out.println(insertColumns.size());
        System.out.println("/////////////////////////////////////////////");
        return toUpdatePreparedStatement(row);
    }


    /**
     * Check if a row with a given key already exists in the database.
     *
     * @param rowKeyValue The value of the row key.
     * @return True if the row exists, false otherwise.
     */
    private boolean isExist(final String rowKeyValue) {
        try {
            PreparedStatement queryStatement = connection.prepareStatement(
                    //String.format("SELECT COUNT(*) FROM %s WHERE %s=?", table, rowKeyColumn)
                    toCountQuery()
            );
            queryStatement.setString(1, rowKeyValue);
            ResultSet resultSet = queryStatement.executeQuery();

            if (resultSet.next()) {
                int count = resultSet.getInt(1);
                resultSet.close();
                queryStatement.close();

                return count > 0;
            }
        } catch (SQLException e) {
            log.error("Error checking if row exists: {}", e.getMessage());
        }

        return false;
    }

    /**
     * Generate an upsert SQL query based on the columns to be excluded.
     *
     * @param excludeColumns The list of columns to exclude.
     * @return The generated upsert SQL query.
     */
    private String toUpsertQuery(final List<String> excludeColumns) {

        StatementQuery statementQuery = new UpsertStatementQuery(table, insertColumns, excludeColumns, rowKeyColumn);
        return statementQuery.prepareQuery();
    }


    /**
     * Generate an insert SQL query.
     *
     * @return The generated insert SQL query.
     */
    private String toInsertQuery() {


        StatementQuery statementQuery = new InsertStatementQuery(table, insertColumns, rowKeyColumn);
        return statementQuery.prepareQuery();
    }

    /**
     * Generate an update SQL query.
     *
     * @return The generated update SQL query.
     */
    private String toUpdateQuery() {

        StatementQuery statementQuery = new UpdateStatementQuery(table, updateColumns, rowKeyColumn);
        return statementQuery.prepareQuery();
    }

    /**
     * Generate a count SQL query.
     *
     * @return The generated count SQL query.
     */
    private String toCountQuery() {
        StatementQuery statementQuery = new CountStatementQuery(table, rowKeyColumn);
        return statementQuery.prepareQuery();
    }

    /**
     * Prepare an upsert SQL statement based on the schema and row data.
     *
     * @param row                The row of data.
     * @param preparedStatement The prepared statement to prepare.
     * @return The prepared upsert SQL statement.
     */
    private PreparedStatement toUpsertPreparedStatement(Row row, PreparedStatement preparedStatement) {
        AtomicInteger index = new AtomicInteger(1);
        insertColumns.forEach(column -> {
            try {
                preparedStatement.setString(index.getAndIncrement(), row.getAs(column));
            } catch (SQLException e) {
                log.error("Error preparing upsert statement: {}", e.getMessage());
            }
        });

        return preparedStatement;
    }

    /**
     * Prepare an update SQL statement based on the schema and row data.
     *
     * @param row The row of data.
     * @return The prepared update SQL statement.
     * @throws SQLException If an SQL exception occurs.
     */
    private PreparedStatement toUpdatePreparedStatement(Row row) throws SQLException {
        AtomicInteger index = new AtomicInteger(1);
        updateColumns.forEach(column -> {
            if (!column.equals(rowKeyColumn)) {
                try {
                    updateStatement.setString(index.getAndIncrement(), row.getAs(column));
                } catch (SQLException e) {
                    log.error("Error preparing upsert statement: {}", e.getMessage());
                }
            }
        });
        updateStatement.setString(index.getAndIncrement(), row.getAs(rowKeyColumn));

        return updateStatement;
    }

    /**
     * Extract the SQL statement from a prepared statement.
     *
     * @param preparedStatement The prepared statement.
     * @return The extracted SQL statement.
     */
    private String getSqlFromPreparedStatement(PreparedStatement preparedStatement) {
        String originalSql = preparedStatement.toString();
        return originalSql.substring(originalSql.indexOf(": ") + 2);
    }
}

