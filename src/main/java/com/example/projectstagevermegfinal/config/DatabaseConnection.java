    package com.example.projectstagevermegfinal.config;

    import lombok.Data;
    import org.springframework.beans.factory.annotation.Value;

    import java.io.Serializable;
    import java.util.Properties;

    import static com.example.projectstagevermegfinal.utils.Constants.*;

    /**
     * Configuration class that holds database connection properties.
     */
    @Data
    public class DatabaseConnection implements Serializable {
        private final String url= DATABASE_URL ;
        private final String username=DATABASE_USERNAME ;
        private  final String password =DATABASE_PASSWORD;

        /**
         * Creates and returns properties for database connection.
         *
         * @return Properties containing database connection settings.
         */
        public Properties getProperties() {
            Properties properties = new Properties();
            properties.setProperty("user", username);
            properties.setProperty("password", password);
            properties.setProperty("useSSL", "false");
            return properties;
        }
    }
