package com.example.projectstagevermegfinal.querry;

import java.util.List;

/**
 * Represents an insert statement query.
 */
public class InsertStatementQuery extends StatementQuery {
    private final List<String> insertColumns;

    public InsertStatementQuery(
            final String table,
            final List<String> insertColumns,
            final String rowKeyColumn) {
        super(table, rowKeyColumn);

        this.insertColumns = insertColumns;
    }

    @Override
    public String prepareQuery() {
        return String.format("INSERT INTO %s (%s) VALUES (%s)",
                getTable(),
                joinedColumnsClause(insertColumns),
                joinedValuesClause(insertColumns)
        );
    }

    public List<String> getInsertColumns() {
        return insertColumns;
    }
}
