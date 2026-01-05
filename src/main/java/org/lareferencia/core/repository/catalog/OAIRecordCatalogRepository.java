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
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Iterator;
import java.util.List;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import javax.sql.DataSource;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.lareferencia.core.metadata.SnapshotMetadata;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Repository;

/**
 * Repositorio JDBC para catálogo OAI en SQLite.
 * 
 * OPERACIONES PRINCIPALES:
 * - upsertRecord(): INSERT OR REPLACE para harvesting
 * - upsertBatch(): Batch insert con transacción
 * - streamAll(): Streaming de todos los registros para validación
 * - count(): Conteo de registros
 * 
 * THREAD SAFETY:
 * - Cada snapshot tiene su propia conexión via CatalogDatabaseManager
 * - Batch operations usan transacciones
 * - No cachear conexiones entre operaciones
 */
@Repository
public class OAIRecordCatalogRepository {

    private static final Logger logger = LogManager.getLogger(OAIRecordCatalogRepository.class);

    private static final DateTimeFormatter ISO_FORMATTER = DateTimeFormatter.ISO_LOCAL_DATE_TIME;

    @Value("${catalog.batch.size:5000}")
    private int batchSize;

    @Autowired
    private CatalogDatabaseManager dbManager;

    // SQL Statements
    private static final String UPSERT_SQL = """
            INSERT OR REPLACE INTO oai_record (id, identifier, datestamp, original_metadata_hash, deleted)
            VALUES (?, ?, ?, ?, ?)
            """;

    private static final String SELECT_ALL_SQL = """
            SELECT id, identifier, datestamp, original_metadata_hash, deleted
            FROM oai_record
            """;

    private static final String SELECT_NOT_DELETED_SQL = """
            SELECT id, identifier, datestamp, original_metadata_hash, deleted
            FROM oai_record
            WHERE deleted = 0
            """;

    private static final String COUNT_SQL = "SELECT COUNT(*) FROM oai_record";

    private static final String COUNT_NOT_DELETED_SQL = "SELECT COUNT(*) FROM oai_record WHERE deleted = 0";

    // ============================================================================
    // INITIALIZATION
    // ============================================================================

    /**
     * Inicializa snapshot para escritura.
     * Para incremental: copia catálogo anterior.
     * 
     * @param metadata           Metadata del snapshot
     * @param previousSnapshotId ID del snapshot anterior (null para full
     *                           harvesting)
     * @throws IOException si falla la inicialización
     */
    public void initializeSnapshot(SnapshotMetadata metadata, Long previousSnapshotId) throws IOException {
        dbManager.initializeSnapshot(metadata, previousSnapshotId);
        logger.info("CATALOG REPO: Snapshot {} initialized", metadata.getSnapshotId());
    }

    /**
     * Abre un snapshot existente para lectura.
     * Usado por ValidationWorker y otros workers que necesitan leer de un catálogo
     * previamente creado durante el harvesting.
     * 
     * @param metadata Metadata del snapshot a abrir
     * @throws IOException si el catálogo no existe o falla la apertura
     */
    public void openSnapshotForRead(SnapshotMetadata metadata) throws IOException {
        dbManager.openSnapshotForRead(metadata);
        logger.info("CATALOG REPO: Snapshot {} opened for read", metadata.getSnapshotId());
    }

    // ============================================================================
    // WRITE OPERATIONS
    // ============================================================================

    /**
     * INSERT OR REPLACE de un registro.
     * Optimizado: No verifica existencia previa (size se calcula al final).
     * 
     * @param snapshotId ID del snapshot
     * @param record     Registro a insertar/actualizar
     */
    public void upsertRecord(Long snapshotId, OAIRecord record) {
        DataSource ds = dbManager.getDataSource(snapshotId);
        if (ds == null) {
            throw new IllegalStateException("Snapshot " + snapshotId + " not initialized");
        }

        try (Connection conn = ds.getConnection();
                PreparedStatement stmt = conn.prepareStatement(UPSERT_SQL)) {

            setRecordParameters(stmt, record);
            stmt.executeUpdate();

        } catch (SQLException e) {
            logger.error("CATALOG REPO: Error upserting record {}", record.getIdentifier(), e);
            throw new RuntimeException("Failed to upsert record", e);
        }
    }

    /**
     * Batch upsert con transacción.
     * Más eficiente para operaciones masivas.
     * 
     * @param snapshotId ID del snapshot
     * @param records    Lista de registros a insertar/actualizar
     */
    public void upsertBatch(Long snapshotId, List<OAIRecord> records) {
        if (records == null || records.isEmpty()) {
            return;
        }

        DataSource ds = dbManager.getDataSource(snapshotId);
        if (ds == null) {
            throw new IllegalStateException("Snapshot " + snapshotId + " not initialized");
        }

        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);

            try (PreparedStatement stmt = conn.prepareStatement(UPSERT_SQL)) {
                int count = 0;
                for (OAIRecord record : records) {
                    setRecordParameters(stmt, record);
                    stmt.addBatch();
                    count++;

                    // Ejecutar batch cada N registros
                    if (count % batchSize == 0) {
                        stmt.executeBatch();
                        logger.debug("CATALOG REPO: Executed batch of {} records", batchSize);
                    }
                }

                // Ejecutar registros restantes
                if (count % batchSize != 0) {
                    stmt.executeBatch();
                }

                conn.commit();
                logger.debug("CATALOG REPO: Committed {} records", count);

            } catch (SQLException e) {
                conn.rollback();
                throw e;
            }

        } catch (SQLException e) {
            logger.error("CATALOG REPO: Error in batch upsert", e);
            throw new RuntimeException("Failed to upsert batch", e);
        }
    }

    /**
     * Cierra conexión del snapshot.
     * 
     * @param snapshotId ID del snapshot
     */
    public void finalizeSnapshot(Long snapshotId) {
        dbManager.closeDataSource(snapshotId);
        logger.info("CATALOG REPO: Snapshot {} finalized", snapshotId);
    }

    // ============================================================================
    // READ OPERATIONS
    // ============================================================================

    /**
     * Stream de todos los registros (incluyendo deleted).
     * No carga todo en memoria - ideal para datasets grandes.
     * 
     * @param metadata Metadata del snapshot
     * @return Stream de registros
     */
    public Stream<OAIRecord> streamAll(SnapshotMetadata metadata) {
        return streamRecords(metadata, SELECT_ALL_SQL);
    }

    /**
     * Stream de registros no eliminados.
     * 
     * @param metadata Metadata del snapshot
     * @return Stream de registros no eliminados
     */
    public Stream<OAIRecord> streamNotDeleted(SnapshotMetadata metadata) {
        return streamRecords(metadata, SELECT_NOT_DELETED_SQL);
    }

    /**
     * Conteo de registros totales.
     * 
     * @param snapshotId ID del snapshot
     * @return Número total de registros
     */
    public long count(Long snapshotId) {
        return executeCount(snapshotId, COUNT_SQL);
    }

    /**
     * Conteo de registros no eliminados.
     * 
     * @param snapshotId ID del snapshot
     * @return Número de registros no eliminados
     */
    public long countNotDeleted(Long snapshotId) {
        return executeCount(snapshotId, COUNT_NOT_DELETED_SQL);
    }

    /**
     * Verifica si hay un DataSource activo para el snapshot.
     */
    public boolean hasActiveManager(Long snapshotId) {
        return dbManager.hasActiveDataSource(snapshotId);
    }

    // ============================================================================
    // CLEANUP
    // ============================================================================

    /**
     * Elimina la base de datos del snapshot.
     * 
     * @param metadata Metadata del snapshot
     * @throws IOException si falla la eliminación
     */
    public void deleteSnapshot(SnapshotMetadata metadata) throws IOException {
        dbManager.deleteDatabase(metadata);
        logger.info("CATALOG REPO: Deleted catalog for snapshot {}", metadata.getSnapshotId());
    }

    // ============================================================================
    // PRIVATE METHODS
    // ============================================================================

    /**
     * Configura los parámetros del PreparedStatement con los datos del record.
     */
    private void setRecordParameters(PreparedStatement stmt, OAIRecord record) throws SQLException {
        stmt.setString(1, record.getId());
        stmt.setString(2, record.getIdentifier());
        stmt.setString(3, record.getDatestamp() != null
                ? record.getDatestamp().format(ISO_FORMATTER)
                : null);
        stmt.setString(4, record.getOriginalMetadataHash());
        stmt.setInt(5, record.isDeleted() ? 1 : 0);
    }

    /**
     * Convierte un ResultSet row a OAIRecord.
     */
    private OAIRecord mapRowToRecord(ResultSet rs) throws SQLException {
        OAIRecord record = new OAIRecord();
        record.setId(rs.getString("id"));
        record.setIdentifier(rs.getString("identifier"));

        String datestampStr = rs.getString("datestamp");
        if (datestampStr != null) {
            record.setDatestamp(LocalDateTime.parse(datestampStr, ISO_FORMATTER));
        }

        record.setOriginalMetadataHash(rs.getString("original_metadata_hash"));
        record.setDeleted(rs.getInt("deleted") == 1);

        return record;
    }

    /**
     * Ejecuta query de conteo.
     */
    private long executeCount(Long snapshotId, String sql) {
        DataSource ds = dbManager.getDataSource(snapshotId);
        if (ds == null) {
            logger.warn("CATALOG REPO: DataSource not found for snapshot {}", snapshotId);
            return 0;
        }

        try (Connection conn = ds.getConnection();
                PreparedStatement stmt = conn.prepareStatement(sql);
                ResultSet rs = stmt.executeQuery()) {

            if (rs.next()) {
                return rs.getLong(1);
            }
            return 0;

        } catch (SQLException e) {
            logger.error("CATALOG REPO: Error counting records", e);
            throw new RuntimeException("Failed to count records", e);
        }
    }

    /**
     * Crea un Stream desde una query SQL.
     * El Stream debe ser cerrado después de uso para liberar recursos.
     */
    private Stream<OAIRecord> streamRecords(SnapshotMetadata metadata, String sql) {
        Long snapshotId = metadata.getSnapshotId();
        DataSource ds = dbManager.getDataSource(snapshotId);

        if (ds == null) {
            logger.warn("CATALOG REPO: DataSource not found for snapshot {}", snapshotId);
            return Stream.empty();
        }

        try {
            Connection conn = ds.getConnection();
            PreparedStatement stmt = conn.prepareStatement(sql);
            ResultSet rs = stmt.executeQuery();

            // Crear iterator que lee del ResultSet
            Iterator<OAIRecord> iterator = new Iterator<>() {
                private OAIRecord next = null;
                private boolean hasNext = false;
                private boolean done = false;

                @Override
                public boolean hasNext() {
                    if (done)
                        return false;
                    if (hasNext)
                        return true;

                    try {
                        if (rs.next()) {
                            next = mapRowToRecord(rs);
                            hasNext = true;
                            return true;
                        } else {
                            done = true;
                            closeResources();
                            return false;
                        }
                    } catch (SQLException e) {
                        done = true;
                        closeResources();
                        throw new RuntimeException("Error reading records", e);
                    }
                }

                @Override
                public OAIRecord next() {
                    if (!hasNext()) {
                        throw new java.util.NoSuchElementException();
                    }
                    hasNext = false;
                    return next;
                }

                private void closeResources() {
                    try {
                        rs.close();
                    } catch (Exception ignored) {
                    }
                    try {
                        stmt.close();
                    } catch (Exception ignored) {
                    }
                    try {
                        conn.close();
                    } catch (Exception ignored) {
                    }
                }
            };

            // Crear Stream con onClose para liberar recursos
            Spliterator<OAIRecord> spliterator = Spliterators.spliteratorUnknownSize(
                    iterator, Spliterator.ORDERED | Spliterator.NONNULL);

            return StreamSupport.stream(spliterator, false)
                    .onClose(() -> {
                        try {
                            rs.close();
                        } catch (Exception ignored) {
                        }
                        try {
                            stmt.close();
                        } catch (Exception ignored) {
                        }
                        try {
                            conn.close();
                        } catch (Exception ignored) {
                        }
                    });

        } catch (SQLException e) {
            logger.error("CATALOG REPO: Error creating record stream", e);
            throw new RuntimeException("Failed to stream records", e);
        }
    }
}
