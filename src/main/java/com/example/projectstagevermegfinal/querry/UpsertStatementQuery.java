package com.example.projectstagevermegfinal.querry;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Represents an upsert statement query.
 */
public class UpsertStatementQuery extends StatementQuery {
    private final List<String> insertColumns;
    private final List<String> excludeColumns;

    public UpsertStatementQuery(
            final String table,
            final List<String> insertColumns,
            final List<String> excludeColumns,
            final String rowKeyColumn) {
        super(table, rowKeyColumn);

        this.insertColumns = insertColumns;
        this.excludeColumns = excludeColumns;
    }


    @Override
    public String prepareQuery() {
        return String.format("INSERT INTO %s (%s) VALUES (%s) ON CONFLICT (%s) DO UPDATE SET %s",
                getTable(),
                joinedColumnsClause(insertColumns),
                joinedValuesClause(insertColumns),
                getRowKeyColumn(),
                excludedColumnsClause(excludeColumns)
        );
    }

    @Override
    public List<String> getInsertColumns() {
        return insertColumns;
    }

    private String excludedColumnsClause(final List<String> columns) {
        return columns.stream()
                .map(p -> String.format("%s=EXCLUDED.%s", p, p))
                .collect(Collectors.joining(", "));
    }
}
