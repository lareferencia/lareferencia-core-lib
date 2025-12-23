/*
 *   Copyright (c) 2013-2025. LA Referencia / Red CLARA and others
 *
 *   This program is free software: you can redistribute it and/or modify
 *   it under the terms of the GNU Affero General Public License as published by
 *   the Free Software Foundation, either version 3 of the License, or
 *   (at your option) any later version.
 *
 *   This program is distributed in the hope that it will be useful,
 *   but WITHOUT ANY WARRANTY; without even the implied warranty of
 *   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *   GNU Affero General Public License for more details.
 *
 *   You should have received a copy of the GNU Affero General Public License
 *   along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 *   This file is part of LA Referencia software platform LRHarvester v4.x
 *   For any further information please contact Lautaro Matas <lmatas@gmail.com>
 */

package org.lareferencia.core.flowable.config;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.flowable.engine.*;
import org.flowable.spring.SpringProcessEngineConfiguration;
import org.lareferencia.core.flowable.listener.ProcessCompletionListener;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;
import org.springframework.core.io.Resource;
import org.springframework.core.io.support.PathMatchingResourcePatternResolver;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.transaction.PlatformTransactionManager;

import jakarta.annotation.PreDestroy;
import java.io.IOException;
import java.util.Collections;

/**
 * Manual Flowable configuration without Spring Boot autoconfiguration.
 * Provides full control over ProcessEngine initialization and all service
 * beans.
 * 
 * <p>
 * This configuration is conditionally enabled via the property:
 * workflow.engine=flowable
 * 
 * @author LA Referencia Team
 */
@Configuration
@ConditionalOnProperty(name = "workflow.engine", havingValue = "flowable")
public class FlowableManualConfiguration {

    @Autowired
    private Environment environment;

    @Autowired
    private ProcessCompletionListener processCompletionListener;

    private HikariDataSource flowableDataSource;

    /**
     * Creates and configures the Flowable ProcessEngineConfiguration.
     * This is the core configuration object that defines all Flowable behavior.
     */
    @Bean
    public SpringProcessEngineConfiguration processEngineConfiguration() throws IOException {
        SpringProcessEngineConfiguration config = new SpringProcessEngineConfiguration();

        // Configure datasource (separate or shared)
        flowableDataSource = createFlowableDataSource();
        if (flowableDataSource != null) {
            config.setDataSource(flowableDataSource);
            config.setTransactionManager(flowableTransactionManager());
        } else {
            throw new IllegalStateException(
                    "Flowable datasource is required. Please configure flowable.datasource.jdbc-url");
        }

        // Database schema management
        String schemaUpdate = environment.getProperty("flowable.database.schema-update", "true");
        config.setDatabaseSchemaUpdate(schemaUpdate);

        // Process definition deployment
        String processLocation = environment.getProperty("flowable.process.definitions.location",
                "classpath*:/processes/**/*.bpmn");
        Resource[] resources = new PathMatchingResourcePatternResolver().getResources(processLocation);
        if (resources.length > 0) {
            config.setDeploymentResources(resources);
        }

        // Async Executor configuration
        boolean asyncExecutorActivate = environment.getProperty("flowable.async-executor-activate", Boolean.class,
                true);
        config.setAsyncExecutorActivate(asyncExecutorActivate);

        if (asyncExecutorActivate) {
            config.setAsyncExecutorNumberOfRetries(
                    environment.getProperty("flowable.async-executor.number-of-retries", Integer.class, 0));
            config.setAsyncExecutorCorePoolSize(
                    environment.getProperty("flowable.async-executor.core-pool-size", Integer.class, 2));
            config.setAsyncExecutorMaxPoolSize(
                    environment.getProperty("flowable.async-executor.max-pool-size", Integer.class, 10));
        }

        // ID Generator
        String idGenerator = environment.getProperty("flowable.id-generator", "uuid");
        if ("uuid".equalsIgnoreCase(idGenerator)) {
            config.setIdGenerator(new org.flowable.common.engine.impl.persistence.StrongUuidGenerator());
        }

        // Event listeners
        config.setEventListeners(Collections.singletonList(processCompletionListener));

        // Additional configuration
        config.setDatabaseType(detectDatabaseType());
        config.setEnableSafeBpmnXml(true);

        System.out.println("✅ [FlowableManualConfiguration] ProcessEngineConfiguration initialized");

        return config;
    }

    /**
     * Creates the ProcessEngine bean from the configuration.
     */
    @Bean
    public ProcessEngine processEngine() throws IOException {
        ProcessEngine engine = processEngineConfiguration().buildProcessEngine();
        System.out.println("✅ [FlowableManualConfiguration] ProcessEngine created: " + engine.getName());
        return engine;
    }

    /**
     * Transaction manager for Flowable operations.
     */
    @Bean(name = "flowableTransactionManager")
    public PlatformTransactionManager flowableTransactionManager() {
        if (flowableDataSource == null) {
            flowableDataSource = createFlowableDataSource();
        }
        return new DataSourceTransactionManager(flowableDataSource);
    }

    // ========== Flowable Service Beans ==========

    @Bean
    public RepositoryService repositoryService(ProcessEngine processEngine) {
        return processEngine.getRepositoryService();
    }

    @Bean
    public RuntimeService runtimeService(ProcessEngine processEngine) {
        return processEngine.getRuntimeService();
    }

    @Bean
    public TaskService taskService(ProcessEngine processEngine) {
        return processEngine.getTaskService();
    }

    @Bean
    public HistoryService historyService(ProcessEngine processEngine) {
        return processEngine.getHistoryService();
    }

    @Bean
    public ManagementService managementService(ProcessEngine processEngine) {
        return processEngine.getManagementService();
    }

    // ========== DataSource Configuration ==========

    /**
     * Creates a separate HikariCP datasource for Flowable.
     * Ensures the database exists before creating the connection pool.
     */
    private HikariDataSource createFlowableDataSource() {
        String jdbcUrl = environment.getProperty("flowable.datasource.jdbc-url");
        String username = environment.getProperty("flowable.datasource.username");
        String password = environment.getProperty("flowable.datasource.password");

        System.out.println("DEBUG: [FlowableManualConfiguration] Loading flowable.datasource.jdbc-url = " + jdbcUrl);
        System.out.println("DEBUG: [FlowableManualConfiguration] Loading flowable.datasource.username = " + username);

        if (jdbcUrl == null || jdbcUrl.isEmpty()) {
            System.err.println("ERROR: [FlowableManualConfiguration] flowable.datasource.jdbc-url is not configured!");
            return null;
        }

        // Ensure database exists before creating pool
        ensureFlowableDatabaseExists(jdbcUrl, username, password);

        HikariConfig hikariConfig = new HikariConfig();
        hikariConfig.setJdbcUrl(jdbcUrl);
        hikariConfig.setUsername(username);
        hikariConfig.setPassword(password);
        hikariConfig.setPoolName("FlowableHikariCP");

        // Apply Hikari pool settings from properties
        hikariConfig.setMinimumIdle(
                environment.getProperty("flowable.datasource.hikari.minimum-idle", Integer.class, 2));
        hikariConfig.setMaximumPoolSize(
                environment.getProperty("flowable.datasource.hikari.maximum-pool-size", Integer.class, 10));
        hikariConfig.setConnectionTimeout(
                environment.getProperty("flowable.datasource.hikari.connection-timeout", Long.class, 30000L));
        hikariConfig.setIdleTimeout(
                environment.getProperty("flowable.datasource.hikari.idle-timeout", Long.class, 600000L));
        hikariConfig.setMaxLifetime(
                environment.getProperty("flowable.datasource.hikari.max-lifetime", Long.class, 1800000L));

        System.out.println("✅ [FlowableManualConfiguration] Created Flowable HikariCP datasource");
        return new HikariDataSource(hikariConfig);
    }

    /**
     * Ensures the Flowable database exists (for PostgreSQL).
     * For other databases, this is a no-op.
     */
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
                        System.out.println("✅ [FlowableManualConfiguration] Successfully created Flowable database '"
                                + dbName + "'");
                    }
                    rs.close();
                } catch (Exception e) {
                    System.err.println(
                            "⚠️  [FlowableManualConfiguration] Could not create Flowable database: " + e.getMessage());
                }
            }
        } catch (Exception e) {
            System.err.println(
                    "⚠️  [FlowableManualConfiguration] Error ensuring Flowable database exists: " + e.getMessage());
        }
    }

    /**
     * Detects database type from JDBC URL.
     */
    private String detectDatabaseType() {
        String jdbcUrl = environment.getProperty("flowable.datasource.jdbc-url", "");

        if (jdbcUrl.contains("postgresql")) {
            return "postgres";
        } else if (jdbcUrl.contains("h2")) {
            return "h2";
        } else if (jdbcUrl.contains("mysql")) {
            return "mysql";
        } else if (jdbcUrl.contains("oracle")) {
            return "oracle";
        } else if (jdbcUrl.contains("sqlserver")) {
            return "mssql";
        } else if (jdbcUrl.contains("sqlite")) {
            return "sqlite";
        }

        return "postgres"; // default
    }

    /**
     * Close the Flowable datasource and ProcessEngine on shutdown.
     */
    @PreDestroy
    public void shutdown() {
        if (flowableDataSource != null && !flowableDataSource.isClosed()) {
            System.out.println("[FlowableManualConfiguration] Closing Flowable HikariCP connection pool...");
            flowableDataSource.close();
            System.out.println("[FlowableManualConfiguration] Flowable HikariCP connection pool closed");
        }
    }
}
