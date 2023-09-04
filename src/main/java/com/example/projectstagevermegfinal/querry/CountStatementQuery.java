package com.example.projectstagevermegfinal.querry;

import java.util.List;

/**
 * Represents a count statement query.
 */
public class CountStatementQuery extends StatementQuery {
    public CountStatementQuery(
            final String table,
            final String rowKeyColumn) {
        super(table, rowKeyColumn);
    }

    @Override
    public String prepareQuery() {
        return String.format("SELECT COUNT(*) FROM %s WHERE %s=?", getTable(), getRowKeyColumn());
    }

    @Override
    public List<String> getInsertColumns() {
        return null;
    }
}
