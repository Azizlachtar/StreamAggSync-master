package com.example.projectstagevermegfinal.statement;



import com.example.projectstagevermegfinal.querry.InsertStatementQuery;
import com.example.projectstagevermegfinal.querry.StatementQuery;
import com.example.projectstagevermegfinal.querry.UpsertStatementQuery;
import org.apache.spark.sql.Row;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.concurrent.atomic.AtomicInteger;

public class UpsertPreparerStatement extends PreparerStatement {
    private final StatementQuery statementQuery;

    public UpsertPreparerStatement(
            final PreparedStatement statement,
            final InsertStatementQuery statementQuery) {

        super(statement);

        this.statementQuery = statementQuery;
    }

    public UpsertPreparerStatement(
            final PreparedStatement statement,
            final UpsertStatementQuery statementQuery) {

        super(statement);

        this.statementQuery = statementQuery;
    }

    @Override
    PreparedStatement prepare(Row row) throws SQLException {
        AtomicInteger index = new AtomicInteger(1);
        statementQuery.getInsertColumns().forEach(column -> {
            try {
                getStatement().setString(index.getAndIncrement(), row.getAs(column));
            } catch (SQLException e) {
                e.printStackTrace();
            }
        });

        return getStatement();
    }

    @Override
    StatementQuery getStatementQuery() {
        return statementQuery;
    }
}
