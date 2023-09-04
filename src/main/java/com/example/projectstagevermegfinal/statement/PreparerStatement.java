package com.example.projectstagevermegfinal.statement;

import com.example.projectstagevermegfinal.querry.StatementQuery;
import lombok.AllArgsConstructor;
import org.apache.spark.sql.Row;

import java.sql.PreparedStatement;
import java.sql.SQLException;

@AllArgsConstructor
public abstract class PreparerStatement {
    private final PreparedStatement statement;

    abstract PreparedStatement prepare(Row row) throws SQLException;

    abstract StatementQuery getStatementQuery();

    PreparedStatement getStatement() {
        return statement;
    }
}
