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

package org.lareferencia.core.repository.validation;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.lareferencia.core.metadata.SnapshotMetadata;
import org.lareferencia.core.util.PathUtils;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.sqlite.SQLiteDataSource;

import javax.annotation.PreDestroy;
import javax.sql.DataSource;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Manages SQLite validation database lifecycle per snapshot.
 * 
 * RESPONSIBILITIES:
 * - Create validation.db with dynamic schema based on validator rules
 * - Manage DataSource connections per snapshot
 * - Handle database cleanup and deletion
 * 
 * FILE STRUCTURE:
 * {basePath}/{NETWORK}/snapshots/snapshot_{id}/validation/validation.db
 * 
 * THREAD SAFETY:
 * - Uses ConcurrentHashMap for DataSource caching
 * - Each snapshot has its own independent database
 */
@Component
public class ValidationDatabaseManager {

    private static final Logger logger = LogManager.getLogger(ValidationDatabaseManager.class);

    private static final String VALIDATION_SUBDIR = "validation";
    private static final String DB_FILENAME = "validation.db";

    @Value("${store.basepath:/tmp/data/}")
    private String basePath;

    // Active DataSources per snapshot
    private final Map<Long, SQLiteDataSource> dataSources = new ConcurrentHashMap<>();

    /**
     * Initializes validation database for a new snapshot with dynamic schema.
     * Creates tables based on the rules defined in the validator.
     * 
     * @param snapshotMetadata Snapshot metadata with network info
     * @param ruleIds          List of rule IDs to create columns for
     * @throws IOException if database creation fails
     */
    public void initializeSnapshot(SnapshotMetadata snapshotMetadata, List<Long> ruleIds) throws IOException {
        Long snapshotId = snapshotMetadata.getSnapshotId();
        logger.info("VALIDATION DB: Initializing database for snapshot {}", snapshotId);

        // Create directory structure
        String snapshotPath = PathUtils.getSnapshotPath(basePath, snapshotMetadata);
        Path validationDir = Paths.get(snapshotPath, VALIDATION_SUBDIR);
        Files.createDirectories(validationDir);

        Path dbPath = validationDir.resolve(DB_FILENAME);

        // Delete existing database if present (clean revalidation)
        if (Files.exists(dbPath)) {
            logger.info("VALIDATION DB: Deleting existing database for snapshot {}", snapshotId);
            Files.delete(dbPath);
        }

        // Create SQLite DataSource with WAL mode
        org.sqlite.SQLiteConfig config = new org.sqlite.SQLiteConfig();
        config.setJournalMode(org.sqlite.SQLiteConfig.JournalMode.WAL);
        config.setSynchronous(org.sqlite.SQLiteConfig.SynchronousMode.NORMAL);

        SQLiteDataSource ds = new SQLiteDataSource(config);
        ds.setUrl("jdbc:sqlite:" + dbPath.toAbsolutePath());

        // Create tables with dynamic schema
        createTables(ds, ruleIds);

        // Cache the DataSource
        dataSources.put(snapshotId, ds);

        logger.info("VALIDATION DB: Initialized database for snapshot {} with {} rules (WAL mode)", snapshotId,
                ruleIds.size());
    }

    /**
     * Copies a completed parent's validation database into a new snapshot. The
     * caller must compare validator fingerprints before invoking this method.
     */
    public void copySnapshotDatabase(SnapshotMetadata parent, SnapshotMetadata target) throws IOException {
        Path source = Paths.get(PathUtils.getSnapshotPath(basePath, parent), VALIDATION_SUBDIR, DB_FILENAME);
        Path destinationDir = Paths.get(PathUtils.getSnapshotPath(basePath, target), VALIDATION_SUBDIR);
        Path destination = destinationDir.resolve(DB_FILENAME);
        if (!Files.isRegularFile(source)) {
            throw new IOException("Parent validation database not found: " + source);
        }
        Files.createDirectories(destinationDir);
        closeDataSource(target.getSnapshotId());
        Files.copy(source, destination, StandardCopyOption.REPLACE_EXISTING);
        migrateAndClearChangeType(destination);
        logger.info("VALIDATION DB: copied parent database from snapshot {} to {}",
                parent.getSnapshotId(), target.getSnapshotId());
    }

    private void migrateAndClearChangeType(Path database) throws IOException {
        try (Connection conn = DriverManager.getConnection("jdbc:sqlite:" + database.toAbsolutePath());
                Statement stmt = conn.createStatement()) {
            boolean present = false;
            try (java.sql.ResultSet rs = stmt.executeQuery("PRAGMA table_info(record_validation)")) {
                while (rs.next()) if ("change_type".equalsIgnoreCase(rs.getString("name"))) { present = true; break; }
            }
            if (!present) stmt.execute("ALTER TABLE record_validation ADD COLUMN change_type TEXT CHECK (change_type IS NULL OR change_type IN ('N','U','D'))");
            stmt.executeUpdate("UPDATE record_validation SET change_type = NULL");
        } catch (SQLException e) { throw new IOException("Failed to prepare copied validation database: " + e.getMessage(), e); }
    }

    /**
     * Opens an existing validation database for reading.
     * 
     * @param snapshotMetadata Snapshot metadata
     * @throws IOException if database doesn't exist
     */
    public void openSnapshotForRead(SnapshotMetadata snapshotMetadata) throws IOException {
        Long snapshotId = snapshotMetadata.getSnapshotId();

        if (dataSources.containsKey(snapshotId)) {
            return; // Already open
        }

        String snapshotPath = PathUtils.getSnapshotPath(basePath, snapshotMetadata);
        Path dbPath = Paths.get(snapshotPath, VALIDATION_SUBDIR, DB_FILENAME);

        if (!Files.exists(dbPath)) {
            throw new IOException("Validation database not found for snapshot " + snapshotId + ": " + dbPath);
        }

        // Create SQLite DataSource with WAL mode
        org.sqlite.SQLiteConfig config = new org.sqlite.SQLiteConfig();
        config.setJournalMode(org.sqlite.SQLiteConfig.JournalMode.WAL);
        config.setSynchronous(org.sqlite.SQLiteConfig.SynchronousMode.NORMAL);

        SQLiteDataSource ds = new SQLiteDataSource(config);
        ds.setUrl("jdbc:sqlite:" + dbPath.toAbsolutePath());

        ensureDeletedColumn(ds, snapshotId);
        ensureChangeTypeColumn(ds, snapshotId);
        dataSources.put(snapshotId, ds);
        logger.debug("VALIDATION DB: Opened database for reading - snapshot {} (WAL mode)", snapshotId);
    }

    /** Adds the tombstone flag to databases created before incremental validation. */
    private void ensureDeletedColumn(SQLiteDataSource ds, Long snapshotId) throws IOException {
        try (Connection conn = ds.getConnection(); Statement stmt = conn.createStatement();
                java.sql.ResultSet rs = stmt.executeQuery("PRAGMA table_info(record_validation)")) {
            boolean present = false;
            while (rs.next()) {
                if ("deleted".equalsIgnoreCase(rs.getString("name"))) {
                    present = true;
                    break;
                }
            }
            if (!present) {
                stmt.execute("ALTER TABLE record_validation ADD COLUMN deleted INTEGER NOT NULL DEFAULT 0");
                stmt.execute("CREATE INDEX IF NOT EXISTS idx_rv_deleted ON record_validation(deleted)");
                logger.warn("VALIDATION DB: migrated legacy snapshot {} with deleted column. "
                        + "Run a full revalidation before enabling incremental validation.", snapshotId);
            }
        } catch (SQLException e) {
            throw new IOException("Failed to migrate deleted column: " + e.getMessage(), e);
        }
    }

    private void ensureChangeTypeColumn(SQLiteDataSource ds, Long snapshotId) throws IOException {
        try (Connection conn = ds.getConnection(); Statement stmt = conn.createStatement();
                java.sql.ResultSet rs = stmt.executeQuery("PRAGMA table_info(record_validation)")) {
            boolean present = false;
            while (rs.next()) if ("change_type".equalsIgnoreCase(rs.getString("name"))) { present = true; break; }
            if (!present) {
                stmt.execute("ALTER TABLE record_validation ADD COLUMN change_type TEXT CHECK (change_type IS NULL OR change_type IN ('N','U','D'))");
                logger.warn("VALIDATION DB: migrated legacy snapshot {} with change_type column; legacy rows are treated as full", snapshotId);
            }
        } catch (SQLException e) { throw new IOException("Failed to migrate change_type column: " + e.getMessage(), e); }
    }

    /**
     * Returns the DataSource for a snapshot.
     * 
     * @param snapshotId Snapshot ID
     * @return DataSource or null if not initialized
     */
    public DataSource getDataSource(Long snapshotId) {
        return dataSources.get(snapshotId);
    }

    /**
     * Checks if there's an active DataSource for the snapshot.
     */
    public boolean hasActiveDataSource(Long snapshotId) {
        return dataSources.containsKey(snapshotId);
    }

    /**
     * Closes the DataSource for a snapshot.
     * 
     * @param snapshotId Snapshot ID
     */
    public void closeDataSource(Long snapshotId) {
        SQLiteDataSource ds = dataSources.remove(snapshotId);
        if (ds != null) {
            logger.debug("VALIDATION DB: Closed DataSource for snapshot {}", snapshotId);
        }
    }

    /**
     * Deletes the validation database file.
     * 
     * @param snapshotMetadata Snapshot metadata
     * @throws IOException if deletion fails
     */
    public void deleteDatabase(SnapshotMetadata snapshotMetadata) throws IOException {
        Long snapshotId = snapshotMetadata.getSnapshotId();

        // Close DataSource first
        closeDataSource(snapshotId);

        String snapshotPath = PathUtils.getSnapshotPath(basePath, snapshotMetadata);
        Path dbPath = Paths.get(snapshotPath, VALIDATION_SUBDIR, DB_FILENAME);

        if (Files.exists(dbPath)) {
            Files.delete(dbPath);
            logger.info("VALIDATION DB: Deleted database for snapshot {}", snapshotId);
        }
    }

    /**
     * Creates tables with dynamic schema based on rule IDs.
     */
    private void createTables(SQLiteDataSource ds, List<Long> ruleIds) throws IOException {
        // Build dynamic column definitions for rules
        StringBuilder ruleColumns = new StringBuilder();
        for (Long ruleId : ruleIds) {
            ruleColumns.append(",\n    rule_").append(ruleId).append(" BOOLEAN");
        }

        String createRecordValidationSQL = """
                CREATE TABLE IF NOT EXISTS record_validation (
                    identifier_hash TEXT PRIMARY KEY,
                    identifier TEXT NOT NULL,
                    datestamp TEXT,
                    is_valid BOOLEAN NOT NULL,
                    is_transformed BOOLEAN NOT NULL,
                    published_metadata_hash TEXT,
                    deleted INTEGER NOT NULL DEFAULT 0,
                    change_type TEXT CHECK (change_type IS NULL OR change_type IN ('N','U','D'))%s
                )
                """.formatted(ruleColumns.toString());

        String createRuleOccurrencesSQL = """
                CREATE TABLE IF NOT EXISTS rule_occurrences (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    identifier_hash TEXT NOT NULL,
                    rule_id INTEGER NOT NULL,
                    is_valid BOOLEAN NOT NULL,
                    occurrence_value TEXT,
                    FOREIGN KEY (identifier_hash) REFERENCES record_validation(identifier_hash)
                )
                """;

        String createIndexIdentifierSQL = "CREATE INDEX IF NOT EXISTS idx_rv_identifier ON record_validation(identifier)";
        String createIndexValidSQL = "CREATE INDEX IF NOT EXISTS idx_rv_valid ON record_validation(is_valid)";
        String createIndexTransformedSQL = "CREATE INDEX IF NOT EXISTS idx_rv_transformed ON record_validation(is_transformed)";
        String createIndexRuleSQL = "CREATE INDEX IF NOT EXISTS idx_ro_rule ON rule_occurrences(rule_id, is_valid)";
        String createIndexRecordSQL = "CREATE INDEX IF NOT EXISTS idx_ro_record ON rule_occurrences(identifier_hash)";

        try (Connection conn = ds.getConnection();
                Statement stmt = conn.createStatement()) {

            stmt.execute(createRecordValidationSQL);
            stmt.execute(createRuleOccurrencesSQL);
            stmt.execute(createIndexIdentifierSQL);
            stmt.execute(createIndexValidSQL);
            stmt.execute(createIndexTransformedSQL);
            stmt.execute("CREATE INDEX IF NOT EXISTS idx_rv_deleted ON record_validation(deleted)");
            stmt.execute("CREATE INDEX IF NOT EXISTS idx_rv_change_type ON record_validation(change_type)");
            stmt.execute(createIndexRuleSQL);
            stmt.execute(createIndexRecordSQL);

            logger.debug("VALIDATION DB: Created tables with {} rule columns", ruleIds.size());

        } catch (SQLException e) {
            throw new IOException("Failed to create validation tables: " + e.getMessage(), e);
        }
    }

    /**
     * Returns the base path for validation storage.
     */
    public String getBasePath() {
        return basePath;
    }

    @PreDestroy
    public void cleanup() {
        logger.info("VALIDATION DB: Cleaning up {} active DataSources", dataSources.size());
        dataSources.clear();
    }
}
