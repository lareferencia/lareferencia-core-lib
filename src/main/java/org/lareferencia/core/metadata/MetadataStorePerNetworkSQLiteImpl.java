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
 */
package org.lareferencia.core.metadata;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.lareferencia.core.util.PathUtils;
import org.lareferencia.core.util.hashing.IHashingHelper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
// import org.springframework.stereotype.Component;

/**
 * SQLite-based implementation of IMetadataStore.
 * Creates one SQLite database file per network.
 */
public class MetadataStorePerNetworkSQLiteImpl implements IMetadataStore {

    private static final Logger logger = LogManager.getLogger(MetadataStorePerNetworkSQLiteImpl.class);

    @Value("${store.basepath:/tmp/data}")
    private String basePath;

    @Autowired
    private IHashingHelper hashing;

    // SQLite uses TEXT for CLOBs.
    private static final String CREATE_TABLE_SQL = "CREATE TABLE IF NOT EXISTS metadata_records (hash TEXT PRIMARY KEY, content TEXT)";
    // INSERT OR IGNORE to skip existing records (deduplication)
    private static final String INSERT_SQL = "INSERT OR IGNORE INTO metadata_records (hash, content) VALUES (?, ?)";
    private static final String SELECT_SQL = "SELECT content FROM metadata_records WHERE hash = ?";

    @PostConstruct
    public void init() {
        logger.info("Active Metadata Store: SQLite Database (Per-Network)");
        logger.info("Base path: {}", basePath);

        File baseDir = new File(basePath);
        if (!baseDir.exists()) {
            if (baseDir.mkdirs()) {
                logger.info("Created base directory: {}", basePath);
            } else {
                logger.error("Failed to create base directory: {}", basePath);
            }
        }

        // Ensure JDBC driver is registered (sometimes needed for older environments,
        // though automagic in newer)
        try {
            Class.forName("org.sqlite.JDBC");
        } catch (ClassNotFoundException e) {
            logger.error("SQLite JDBC driver not found", e);
        }
    }

    // Connection cache: Network Acronym -> Connection
    private final Map<String, Connection> connectionCache = new java.util.concurrent.ConcurrentHashMap<>();

    @PreDestroy
    public void closeAllConnections() {
        logger.info("Closing all SQLite connections...");
        for (Connection conn : connectionCache.values()) {
            try {
                if (conn != null && !conn.isClosed()) {
                    conn.close();
                }
            } catch (SQLException e) {
                logger.warn("Error closing connection", e);
            }
        }
        connectionCache.clear();
    }

    private synchronized Connection getConnection(SnapshotMetadata snapshotMetadata) throws SQLException {
        String networkAcronym = "UNKNOWN";
        if (snapshotMetadata != null && snapshotMetadata.getNetwork() != null) {
            networkAcronym = snapshotMetadata.getNetwork().getAcronym();
        }

        Connection conn = connectionCache.get(networkAcronym);
        if (conn == null || conn.isClosed()) {
            // Sanitize acronym for filename
            String safeAcronym = PathUtils.sanitizeNetworkAcronym(networkAcronym);
            String networkDir = basePath + File.separator + safeAcronym;

            // Ensure directory exists
            new File(networkDir).mkdirs();

            String dbPath = networkDir + File.separator + safeAcronym + ".sqlite";

            String jdbcUrl = "jdbc:sqlite:" + dbPath;
            conn = DriverManager.getConnection(jdbcUrl);
            connectionCache.put(networkAcronym, conn);

            // Ensure table exists
            try (Statement stmt = conn.createStatement()) {
                stmt.execute(CREATE_TABLE_SQL);
            }
        }
        return conn;
    }

    @Override
    public String storeAndReturnHash(SnapshotMetadata snapshotMetadata, String metadata) {
        String hash = hashing.calculateHash(metadata);

        try {
            Connection conn = getConnection(snapshotMetadata);

            try (PreparedStatement metadataStmt = conn.prepareStatement(INSERT_SQL)) {
                metadataStmt.setString(1, hash);
                metadataStmt.setString(2, metadata);
                int affectedRows = metadataStmt.executeUpdate();
                if (affectedRows == 0) {
                    logger.debug("Metadata with hash {} already exists, skipping", hash);
                }
            }

            return hash;

        } catch (SQLException e) {
            logger.error("Error storing metadata for hash: {}", hash, e);
            throw new RuntimeException("Failed to store metadata in SQLite", e);
        }
    }

    @Override
    public String getMetadata(SnapshotMetadata snapshotMetadata, String hash) throws MetadataRecordStoreException {
        try {
            Connection conn = getConnection(snapshotMetadata);

            try (PreparedStatement stmt = conn.prepareStatement(SELECT_SQL)) {
                stmt.setString(1, hash);
                try (ResultSet rs = stmt.executeQuery()) {
                    if (rs.next()) {
                        return rs.getString("content");
                    } else {
                        throw new MetadataRecordStoreException("Metadata not found for hash: " + hash);
                    }
                }
            }

        } catch (SQLException e) {
            logger.error("Error retrieving metadata for hash: {}", hash, e);
            throw new MetadataRecordStoreException("Failed to retrieve metadata from SQLite", e);
        }
    }

    @Override
    public Boolean cleanAndOptimizeStore() {
        logger.info("Starting optimization (VACUUM) of SQLite databases in {}", basePath);
        File baseDir = new File(basePath);
        File[] dbFiles = baseDir.listFiles((dir, name) -> name.endsWith(".sqlite"));

        if (dbFiles == null)
            return true;

        boolean success = true;
        for (File dbFile : dbFiles) {
            String jdbcUrl = "jdbc:sqlite:" + dbFile.getAbsolutePath();

            try (Connection conn = DriverManager.getConnection(jdbcUrl);
                    Statement stmt = conn.createStatement()) {
                logger.info("Optimization database: {}", dbFile.getName());
                stmt.execute("VACUUM");
            } catch (SQLException e) {
                logger.warn("Could not optimize database {}: {}", dbFile.getName(), e.getMessage());
            }
        }
        return success;
    }
}
