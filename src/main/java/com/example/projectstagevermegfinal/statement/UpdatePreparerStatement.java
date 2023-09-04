package com.example.projectstagevermegfinal.statement;

import com.example.projectstagevermegfinal.querry.UpdateStatementQuery;
import org.apache.spark.sql.Row;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.concurrent.atomic.AtomicInteger;

public class UpdatePreparerStatement extends PreparerStatement {
    private final UpdateStatementQuery statementQuery;

    public UpdatePreparerStatement(
            final PreparedStatement statement,
            final UpdateStatementQuery statementQuery) {

        super(statement);

        this.statementQuery = statementQuery;
    }

    @Override
    PreparedStatement prepare(Row row) throws SQLException {
        AtomicInteger index = new AtomicInteger(1);
        statementQuery.getUpdateColumns().forEach(column -> {
            if (!column.equals(statementQuery.getRowKeyColumn())) {
                try {
                    getStatement().setString(index.getAndIncrement(), row.getAs(column));
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        });
        getStatement().setString(index.getAndIncrement(), row.getAs(statementQuery.getRowKeyColumn()));

        return getStatement();
    }

    @Override
    UpdateStatementQuery getStatementQuery() {
        return statementQuery;
    }
}
