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

package org.lareferencia.core.repository.catalog;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import javax.sql.DataSource;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.lareferencia.core.metadata.ISnapshotStore;
import org.lareferencia.core.metadata.SnapshotMetadata;
import org.lareferencia.core.util.PathUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.sqlite.SQLiteDataSource;

import jakarta.annotation.PreDestroy;

/**
 * Gestiona bases de datos SQLite de catálogo por snapshot.
 * 
 * RESPONSABILIDADES:
 * - Crear/abrir base de datos SQLite para un snapshot
 * - Copiar catálogo de snapshot anterior (harvesting incremental)
 * - Pool de conexiones ligero (DataSource por snapshot)
 * - Crear esquema al inicializar
 * 
 * ESTRUCTURA DE ARCHIVOS:
 * {basePath}/{NETWORK}/snapshots/snapshot_{id}/catalog/catalog.db
 * 
 * THREAD SAFETY:
 * - ConcurrentHashMap para gestión de DataSources
 * - Cada snapshot tiene su propia base de datos independiente
 * - No compartir conexiones entre threads
 */
@Component
public class CatalogDatabaseManager {

    private static final Logger logger = LogManager.getLogger(CatalogDatabaseManager.class);

    private static final String CATALOG_SUBDIR = "catalog";
    private static final String DB_FILENAME = "catalog.db";

    @Value("${store.basepath:/tmp/data/}")
    private String basePath;

    @Value("${catalog.sqlite.wal-mode:true}")
    private boolean walMode;

    @Autowired
    private ISnapshotStore snapshotStore;

    // DataSources activos por snapshotId
    private final Map<Long, SQLiteDataSource> dataSources = new ConcurrentHashMap<>();

    /**
     * SQL para crear el esquema del catálogo.
     */
    private static final String CREATE_SCHEMA_SQL = """
            CREATE TABLE IF NOT EXISTS oai_record (
                id TEXT PRIMARY KEY,
                identifier TEXT NOT NULL UNIQUE,
                datestamp TEXT NOT NULL,
                original_metadata_hash TEXT,
                deleted INTEGER NOT NULL DEFAULT 0
            );

            CREATE INDEX IF NOT EXISTS idx_deleted ON oai_record(deleted);
            CREATE INDEX IF NOT EXISTS idx_datestamp ON oai_record(datestamp);
            """;

    // ============================================================================
    // INITIALIZATION
    // ============================================================================

    /**
     * Inicializa catálogo para un nuevo snapshot.
     * Si es harvesting incremental (previousSnapshotId != null), copia el catálogo
     * anterior.
     * 
     * @param metadata           Metadata del snapshot nuevo
     * @param previousSnapshotId ID del snapshot anterior (null si full harvesting)
     * @throws IOException si falla la inicialización
     */
    public void initializeSnapshot(SnapshotMetadata metadata, Long previousSnapshotId) throws IOException {
        Long snapshotId = metadata.getSnapshotId();
        logger.info("CATALOG DB: Initializing for snapshot {} (previousSnapshotId={})",
                snapshotId, previousSnapshotId);

        // Crear directorio del catálogo
        Path catalogDir = getCatalogPath(metadata);
        Files.createDirectories(catalogDir);

        Path dbPath = catalogDir.resolve(DB_FILENAME);

        // Si es harvesting incremental, copiar catálogo anterior
        if (previousSnapshotId != null) {
            copyCatalogFromPrevious(metadata, previousSnapshotId, dbPath);
        }

        // Crear y configurar DataSource
        SQLiteDataSource ds = createDataSource(dbPath);
        dataSources.put(snapshotId, ds);

        // Crear esquema si es nueva base de datos
        createSchemaIfNeeded(ds);

        logger.info("CATALOG DB: Snapshot {} initialized at {}", snapshotId, dbPath);
    }

    /**
     * Obtiene DataSource para un snapshot ya inicializado.
     * 
     * @param snapshotId ID del snapshot
     * @return DataSource o null si no existe
     */
    public DataSource getDataSource(Long snapshotId) {
        return dataSources.get(snapshotId);
    }

    /**
     * Abre un catálogo existente para lectura.
     * Usado por ValidationWorker y otros workers que necesitan leer de un catálogo
     * previamente creado durante el harvesting.
     * 
     * @param metadata Metadata del snapshot a abrir
     * @throws IOException si el catálogo no existe o falla la apertura
     */
    public void openSnapshotForRead(SnapshotMetadata metadata) throws IOException {
        Long snapshotId = metadata.getSnapshotId();

        // Si ya está abierto, no hacer nada
        if (dataSources.containsKey(snapshotId)) {
            logger.debug("CATALOG DB: Snapshot {} already open for read", snapshotId);
            return;
        }

        logger.info("CATALOG DB: Opening snapshot {} for read", snapshotId);

        Path catalogDir = getCatalogPath(metadata);
        Path dbPath = catalogDir.resolve(DB_FILENAME);

        if (!Files.exists(dbPath)) {
            throw new IOException("Catalog database not found for snapshot " + snapshotId + " at " + dbPath);
        }

        // Crear DataSource en modo lectura
        SQLiteDataSource ds = createDataSource(dbPath);
        dataSources.put(snapshotId, ds);

        logger.info("CATALOG DB: Snapshot {} opened for read at {}", snapshotId, dbPath);
    }

    /**
     * Verifica si hay un DataSource activo para el snapshot.
     */
    public boolean hasActiveDataSource(Long snapshotId) {
        return dataSources.containsKey(snapshotId);
    }

    // ============================================================================
    // CLEANUP
    // ============================================================================

    /**
     * Closes connections and releases resources for a snapshot.
     * Executes WAL checkpoint before closing to clean up temporary files.
     * 
     * @param snapshotId Snapshot ID
     */
    public void closeDataSource(Long snapshotId) {
        SQLiteDataSource ds = dataSources.remove(snapshotId);
        if (ds != null) {
            // Execute WAL checkpoint to clean up -wal and -shm files
            checkpointWAL(ds, snapshotId);
            logger.info("CATALOG DB: Closed DataSource for snapshot {}", snapshotId);
        }
    }

    /**
     * Executes WAL checkpoint to consolidate changes and clean up temporary files.
     * This reduces disk size and improves subsequent open performance.
     * 
     * @param ds         Snapshot DataSource
     * @param snapshotId Snapshot ID (for logging)
     */
    private void checkpointWAL(SQLiteDataSource ds, Long snapshotId) {
        if (!walMode) {
            return;
        }

        try (Connection conn = ds.getConnection();
                Statement stmt = conn.createStatement()) {

            // TRUNCATE mode: checkpoint and then truncate WAL file to zero bytes
            stmt.execute("PRAGMA wal_checkpoint(TRUNCATE)");
            logger.debug("CATALOG DB: WAL checkpoint completed for snapshot {}", snapshotId);

        } catch (SQLException e) {
            // Checkpoint failure is not critical
            logger.warn("CATALOG DB: WAL checkpoint failed for snapshot {}: {}",
                    snapshotId, e.getMessage());
        }
    }

    /**
     * Elimina la base de datos de un snapshot.
     * 
     * @param metadata Metadata del snapshot
     * @throws IOException si falla la eliminación
     */
    public void deleteDatabase(SnapshotMetadata metadata) throws IOException {
        Long snapshotId = metadata.getSnapshotId();

        // Cerrar DataSource primero
        closeDataSource(snapshotId);

        Path catalogDir = getCatalogPath(metadata);
        Path dbPath = catalogDir.resolve(DB_FILENAME);

        if (Files.exists(dbPath)) {
            Files.delete(dbPath);
            logger.info("CATALOG DB: Deleted database for snapshot {}", snapshotId);
        }

        // También eliminar archivos WAL si existen
        Path walPath = catalogDir.resolve(DB_FILENAME + "-wal");
        Path shmPath = catalogDir.resolve(DB_FILENAME + "-shm");
        Files.deleteIfExists(walPath);
        Files.deleteIfExists(shmPath);
    }

    /**
     * Cierra todos los DataSources activos.
     */
    @PreDestroy
    public void closeAll() {
        logger.info("CATALOG DB: Closing {} active DataSources", dataSources.size());
        dataSources.keySet().forEach(this::closeDataSource);
        dataSources.clear();
    }

    // ============================================================================
    // PRIVATE METHODS
    // ============================================================================

    /**
     * Construye la ruta al directorio del catálogo.
     */
    private Path getCatalogPath(SnapshotMetadata metadata) {
        String snapshotPath = PathUtils.getSnapshotPath(basePath, metadata);
        return Paths.get(snapshotPath, CATALOG_SUBDIR);
    }

    /**
     * Copia catalog.db del snapshot anterior al nuevo.
     */
    private void copyCatalogFromPrevious(SnapshotMetadata newSnapshot,
            Long previousSnapshotId,
            Path targetDbPath) throws IOException {
        // Obtener metadata del snapshot anterior
        SnapshotMetadata previousMetadata = snapshotStore.getSnapshotMetadata(previousSnapshotId);
        if (previousMetadata == null) {
            logger.warn("CATALOG DB: Previous snapshot {} not found, starting fresh", previousSnapshotId);
            return;
        }

        Path previousCatalogDir = getCatalogPath(previousMetadata);
        Path previousDbPath = previousCatalogDir.resolve(DB_FILENAME);

        if (!Files.exists(previousDbPath)) {
            logger.warn("CATALOG DB: Previous catalog not found at {}, starting fresh", previousDbPath);
            return;
        }

        // Copiar archivo completo
        logger.info("CATALOG DB: Copying catalog from snapshot {} to {}",
                previousSnapshotId, newSnapshot.getSnapshotId());
        Files.copy(previousDbPath, targetDbPath, StandardCopyOption.REPLACE_EXISTING);

        logger.info("CATALOG DB: Copied {} bytes from previous catalog", Files.size(targetDbPath));
    }

    /**
     * Crea DataSource SQLite para la ruta especificada.
     */
    private SQLiteDataSource createDataSource(Path dbPath) {
        SQLiteDataSource ds = new SQLiteDataSource();
        ds.setUrl("jdbc:sqlite:" + dbPath.toAbsolutePath());

        // Configuración de rendimiento
        if (walMode) {
            ds.setJournalMode("WAL");
        }
        ds.setSynchronous("NORMAL");

        return ds;
    }

    /**
     * Crea esquema SQL si la tabla no existe.
     */
    private void createSchemaIfNeeded(DataSource ds) {
        try (Connection conn = ds.getConnection();
                Statement stmt = conn.createStatement()) {

            // Ejecutar cada statement por separado
            for (String sql : CREATE_SCHEMA_SQL.split(";")) {
                String trimmed = sql.trim();
                if (!trimmed.isEmpty()) {
                    stmt.execute(trimmed);
                }
            }

            // Configurar PRAGMAs
            if (walMode) {
                stmt.execute("PRAGMA journal_mode=WAL");
            }
            stmt.execute("PRAGMA synchronous=NORMAL");
            stmt.execute("PRAGMA cache_size=10000"); // 10MB cache approx (pages)
            stmt.execute("PRAGMA temp_store=MEMORY");

        } catch (SQLException e) {
            logger.error("CATALOG DB: Error creating schema", e);
            throw new RuntimeException("Failed to create catalog schema", e);
        }
    }
}
