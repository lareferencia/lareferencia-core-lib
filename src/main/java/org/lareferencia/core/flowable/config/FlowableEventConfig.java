package org.lareferencia.core.flowable.config;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.flowable.spring.SpringProcessEngineConfiguration;
import org.flowable.spring.boot.EngineConfigurationConfigurer;
import org.lareferencia.core.flowable.listener.ProcessCompletionListener;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;

import jakarta.annotation.PreDestroy;
import java.util.Collections;

/**
 * Configures Flowable process engine with custom event listeners and separate
 * datasource.
 * 
 * @author LA Referencia Team
 */
@Configuration
public class FlowableEventConfig implements EngineConfigurationConfigurer<SpringProcessEngineConfiguration> {

    @Autowired
    private ProcessCompletionListener processCompletionListener;

    @Autowired
    private Environment environment;

    /** Keep reference to close on shutdown */
    private HikariDataSource flowableDataSource;

    @Override
    public void configure(SpringProcessEngineConfiguration config) {
        // Register event listener for process completion
        config.setEventListeners(Collections.singletonList(processCompletionListener));

        // Configure separate datasource for Flowable
        flowableDataSource = createFlowableDataSource();
        if (flowableDataSource != null) {
            config.setDataSource(flowableDataSource);

            // IMPORTANT: Create a dedicated transaction manager for this datasource
            // Otherwise Flowable might use the primary (main DB) transaction manager
            System.out.println(
                    "DEBUG: [FlowableEventConfig] Creating dedicated DataSourceTransactionManager for Flowable");
            org.springframework.jdbc.datasource.DataSourceTransactionManager transactionManager = new org.springframework.jdbc.datasource.DataSourceTransactionManager(
                    flowableDataSource);
            config.setTransactionManager(transactionManager);
        }
        // Configure graceful shutdown for async executor (fixes exit hanging)
        config.setAsyncExecutorActivate(true);
        config.setAsyncExecutorNumberOfRetries(0); // Don't retry failed jobs on shutdown
    }

    /**
     * Close the Flowable datasource on application shutdown.
     */
    @PreDestroy
    public void shutdown() {
        if (flowableDataSource != null && !flowableDataSource.isClosed()) {
            System.out.println("[FlowableEventConfig] Closing Flowable HikariCP connection pool...");
            flowableDataSource.close();
            System.out.println("[FlowableEventConfig] Flowable HikariCP connection pool closed");
        }
    }

    /**
     * Creates a separate datasource for Flowable based on configuration properties.
     * IMPORTANT: This also ensures the database exists before creating the
     * connection pool.
     */
    private HikariDataSource createFlowableDataSource() {
        String jdbcUrl = environment.getProperty("flowable.datasource.jdbc-url");
        String username = environment.getProperty("flowable.datasource.username");
        String password = environment.getProperty("flowable.datasource.password");

        System.out.println("DEBUG: [FlowableEventConfig] Loaded flowable.datasource.jdbc-url = " + jdbcUrl);
        System.out.println("DEBUG: [FlowableEventConfig] Loaded flowable.datasource.username = " + username);

        if (jdbcUrl == null || jdbcUrl.isEmpty()) {
            System.out.println("DEBUG: [FlowableEventConfig] JDBC URL is missing, falling back to main DataSource");
            return null; // Fall back to default datasource
        }

        // CRITICAL: Ensure database exists BEFORE creating Hikari pool
        ensureFlowableDatabaseExists(jdbcUrl, username, password);

        HikariConfig hikariConfig = new HikariConfig();
        hikariConfig.setJdbcUrl(jdbcUrl);
        hikariConfig.setUsername(username);
        hikariConfig.setPassword(password);
        hikariConfig.setPoolName("FlowableHikariCP");

        // Apply Hikari pool settings from properties
        hikariConfig
                .setMinimumIdle(environment.getProperty("flowable.datasource.hikari.minimum-idle", Integer.class, 2));
        hikariConfig.setMaximumPoolSize(
                environment.getProperty("flowable.datasource.hikari.maximum-pool-size", Integer.class, 10));
        hikariConfig.setConnectionTimeout(
                environment.getProperty("flowable.datasource.hikari.connection-timeout", Long.class, 30000L));
        hikariConfig.setIdleTimeout(
                environment.getProperty("flowable.datasource.hikari.idle-timeout", Long.class, 600000L));
        hikariConfig.setMaxLifetime(
                environment.getProperty("flowable.datasource.hikari.max-lifetime", Long.class, 1800000L));

        return new HikariDataSource(hikariConfig);
    }

    private void ensureFlowableDatabaseExists(String jdbcUrl, String username, String password) {
        try {
            if (jdbcUrl.startsWith("jdbc:postgresql://")) {
                String remaining = jdbcUrl.substring("jdbc:postgresql://".length());
                int slashIdx = remaining.indexOf('/');
                if (slashIdx == -1)
                    return;

                String serverPart = remaining.substring(0, slashIdx);
                String rest = remaining.substring(slashIdx + 1);
                String dbName = rest.split("\\?")[0];
                String serverUrl = "jdbc:postgresql://" + serverPart + "/postgres";

                try (java.sql.Connection conn = java.sql.DriverManager.getConnection(serverUrl, username, password);
                        java.sql.Statement stmt = conn.createStatement()) {

                    String checkQuery = String.format("SELECT 1 FROM pg_database WHERE datname = '%s'", dbName);
                    java.sql.ResultSet rs = stmt.executeQuery(checkQuery);

                    if (!rs.next()) {
                        String createQuery = String.format("CREATE DATABASE %s OWNER %s", dbName, username);
                        stmt.execute(createQuery);
                        System.out.println("✅ Successfully created Flowable database '" + dbName + "'");
                    }
                    rs.close();
                } catch (Exception e) {
                    System.err.println("⚠️  Could not create Flowable database: " + e.getMessage());
                }
            }
        } catch (Exception e) {
            System.err.println("⚠️  Error ensuring Flowable database exists: " + e.getMessage());
        }
    }
}
