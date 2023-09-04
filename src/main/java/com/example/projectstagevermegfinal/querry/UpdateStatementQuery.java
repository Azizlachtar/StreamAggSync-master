package com.example.projectstagevermegfinal.querry;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Represents an update statement query.
 */
public class UpdateStatementQuery extends StatementQuery {
    private final List<String> updateColumns;

    public UpdateStatementQuery(
            final String table,
            final List<String> updateColumns,
            final String rowKeyColumn) {
        super(table, rowKeyColumn);

        this.updateColumns = updateColumns;
    }

    @Override
    public String prepareQuery() {
        final String updateColumnsClause =    updateColumns.stream()
                .filter(p -> !p.equals( getRowKeyColumn()))
                .map(p -> String.format("%s=?", p))
                .collect(Collectors.joining(", "));

        return String.format("UPDATE %s SET %s WHERE %s=?", getTable(), updateColumnsClause, getRowKeyColumn());
    }

    @Override
    public List<String> getInsertColumns() {
        return null;
    }

    public List<String> getUpdateColumns() {
        return updateColumns;
    }
}
