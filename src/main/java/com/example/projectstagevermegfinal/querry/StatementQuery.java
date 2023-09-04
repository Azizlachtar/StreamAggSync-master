package com.example.projectstagevermegfinal.querry;

import lombok.AllArgsConstructor;

import java.util.List;
import java.util.stream.Collectors;


/**
 * Abstract class representing a database statement query.
 */
@AllArgsConstructor
public abstract class  StatementQuery {
    private final String table;

    private final String rowKeyColumn;

    /**
     * Returns the prepared SQL query.
     *
     * @return Prepared SQL query
     */
    public abstract String prepareQuery();

    /**
     * Returns the list of insert columns.
     *
     * @return List of insert columns
     */
    public abstract List<String> getInsertColumns();

    /**
     * Returns the table name.
     *
     * @return Table name
     */
    String getTable() {
        return table;
    }


    /**
     * Returns the row key column name.
     *
     * @return Row key column name
     */
    public String getRowKeyColumn() {
        return rowKeyColumn;
    }

    /**
     * Returns a comma-separated string of joined values clauses.
     *
     * @param columns List of columns
     * @return Comma-separated string of joined values clauses
     */
    String joinedValuesClause(final List<String> columns) {
        return columns.stream()
                .map(p -> "?")
                .collect(Collectors.joining(", "));
    }

    /**
     * Returns a comma-separated string of joined columns.
     *
     * @param columns List of columns
     * @return Comma-separated string of joined columns
     */
    String joinedColumnsClause(final List<String> columns) {
        return columns.stream()
                .collect(Collectors.joining(", "));
    }
}

