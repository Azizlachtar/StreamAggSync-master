package com.example.projectstagevermegfinal.data;

        import lombok.Builder;
        import lombok.Data;


/**
 * Represents a data sink configuration for streaming data.
 */
@Data
@Builder
public class StreamingDataSink {
    private final String url;
    private final String userName;
    private final String password;
    private final String tableName;
}
