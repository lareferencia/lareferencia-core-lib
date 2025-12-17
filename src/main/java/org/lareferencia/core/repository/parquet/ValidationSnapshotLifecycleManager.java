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

package org.lareferencia.core.repository.parquet;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;

import org.apache.hadoop.conf.Configuration;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.lareferencia.core.metadata.ISnapshotStore;
import org.lareferencia.core.metadata.SnapshotMetadata;
import org.lareferencia.core.util.PathUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Comparator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Gestiona el ciclo de vida de escritura de snapshots de validación.
 * 
 * RESPONSABILIDADES:
 * - Inicialización de snapshots (directorios, managers)
 * - Escritura incremental de records
 * - Finalización y cierre de writers
 * - Eliminación de datos de validación
 * 
 * CICLO DE VIDA:
 * 1. initializeSnapshot() → Crea metadata inicial, directorio y managers
 * 2. saveRecordAndFacts() × N → Escritura incremental record por record
 * 3. finalizeSnapshot() → Cierra managers y persiste metadata final
 * 
 * THREAD-SAFETY:
 * - Locks granulares por snapshotId para evitar contención
 * - ConcurrentHashMap para managers y stats
 */
@Component
public class ValidationSnapshotLifecycleManager {

    private static final Logger logger = LogManager.getLogger(ValidationSnapshotLifecycleManager.class);

    @Value("${store.basepath:/tmp/data/}")
    private String basePath;

    @Value("${parquet.validation.records-per-file:100000}")
    private int validationRecordsPerFile;

    @Autowired
    private ISnapshotStore snapshotStore;

    @Autowired
    private SnapshotRecordsCache recordsCache;

    private Configuration hadoopConf;
    private final Map<Long, SnapshotValidationStats> snapshotStatsCache = new ConcurrentHashMap<>();
    private final Map<Long, ValidationRecordManager> recordsManagers = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<Long, Object> snapshotLocks = new ConcurrentHashMap<>();

    @PostConstruct
    public void init() {
        hadoopConf = new Configuration();
        hadoopConf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());

        try {
            Files.createDirectories(Paths.get(basePath));
            logger.info("LIFECYCLE MANAGER: Initialized | BasePath: {} | RecordsPerFile: {}",
                    basePath, validationRecordsPerFile);
        } catch (IOException e) {
            logger.error("Failed to create base path: {}", basePath, e);
        }
    }

    @PreDestroy
    public void cleanup() {
        logger.info("LIFECYCLE MANAGER SHUTDOWN: Closing {} active managers", recordsManagers.size());

        recordsManagers.forEach((snapshotId, manager) -> {
            try {
                manager.close();
                logger.debug("Closed RecordsManager for snapshot {}", snapshotId);
            } catch (IOException e) {
                logger.error("Error closing RecordsManager for snapshot {}", snapshotId, e);
            }
        });

        recordsManagers.clear();
        snapshotStatsCache.clear();
        snapshotLocks.clear();
        logger.info("LIFECYCLE MANAGER SHUTDOWN COMPLETE");
    }

    /**
     * Obtiene el lock para un snapshot específico (crea si no existe)
     */
    private Object getSnapshotLock(Long snapshotId) {
        return snapshotLocks.computeIfAbsent(snapshotId, k -> new Object());
    }

    /**
     * Inicializa un snapshot para escritura.
     * 
     * @param snapshotMetadata metadata del snapshot
     * @throws IOException si hay error
     */
    public void initializeSnapshot(SnapshotMetadata snapshotMetadata) throws IOException {
        Long snapshotId = snapshotMetadata.getSnapshotId();
        logger.debug("INITIALIZE SNAPSHOT: snapshot={}", snapshotId);

        try {
            String snapshotDir = PathUtils.getSnapshotPath(basePath, snapshotMetadata);
            Files.createDirectories(Paths.get(snapshotDir));

            // Limpiar validación anterior si existe
            String validationDir = snapshotDir + "/validation";
            if (Files.exists(Paths.get(validationDir))) {
                logger.debug("Cleaning up previous validation data at: {}", validationDir);
                Files.walk(Paths.get(validationDir))
                        .sorted(Comparator.reverseOrder())
                        .forEach(path -> {
                            try {
                                Files.delete(path);
                                logger.debug("Deleted: {}", path);
                            } catch (IOException e) {
                                logger.error("Failed to delete {}", path, e);
                            }
                        });
            }

            // Invalidar cache
            recordsCache.invalidate(snapshotId);

            // Crear manager para escritura
            ValidationRecordManager recordsManager = ValidationRecordManager.forWriting(
                    basePath, snapshotMetadata, hadoopConf, validationRecordsPerFile);
            recordsManagers.put(snapshotId, recordsManager);
            logger.debug("Created RecordsManager for snapshot {}", snapshotId);

            // Crear stats iniciales
            SnapshotValidationStats snapshotStats = new SnapshotValidationStats(snapshotMetadata);
            SnapshotMetadataManager.writeValidationStats(basePath, snapshotStats);
            snapshotStatsCache.put(snapshotId, snapshotStats);

            logger.debug("Snapshot {} initialized and ready for writes", snapshotId);

        } catch (IOException e) {
            recordsManagers.remove(snapshotId);
            snapshotStatsCache.remove(snapshotId);
            logger.error("Failed to initialize snapshot {}", snapshotId, e);
            throw e;
        }
    }

    /**
     * Guarda un record y sus facts (escritura incremental).
     * 
     * @param snapshotMetadata metadata del snapshot
     * @param record           datos del record
     * @throws IOException si hay error
     */
    public void saveRecordAndFacts(SnapshotMetadata snapshotMetadata, RecordValidation record) throws IOException {
        if (record == null) {
            logger.warn("SAVE: Null record for snapshot {}", snapshotMetadata.getSnapshotId());
            return;
        }

        updateStoredStats(snapshotMetadata.getSnapshotId(), record);
        writeRecord(snapshotMetadata.getSnapshotId(), record);
    }

    /**
     * Actualiza stats de forma acumulativa (thread-safe con lock granular)
     */
    private void updateStoredStats(Long snapshotId, RecordValidation record) {
        synchronized (getSnapshotLock(snapshotId)) {
            SnapshotValidationStats stats = snapshotStatsCache.get(snapshotId);
            if (stats == null) {
                throw new IllegalStateException(
                        "Snapshot " + snapshotId + " not initialized. Call initializeSnapshot() first.");
            }
            stats.updateFromRecord(record);
        }
    }

    /**
     * Escribe un record usando el manager persistente
     */
    private void writeRecord(Long snapshotId, RecordValidation record) throws IOException {
        ValidationRecordManager recordsManager = recordsManagers.get(snapshotId);
        if (recordsManager == null) {
            throw new IllegalStateException(
                    "RecordsManager not found for snapshot " + snapshotId + ". Call initializeSnapshot() first.");
        }
        recordsManager.writeRecord(record);
    }

    /**
     * Finaliza un snapshot, cierra writers y persiste metadata final.
     * 
     * @param snapshotId ID del snapshot
     * @throws IOException si hay error
     */
    public void finalizeSnapshot(Long snapshotId) throws IOException {
        logger.debug("FINALIZE SNAPSHOT: Closing writers for snapshot {}", snapshotId);

        recordsCache.invalidate(snapshotId);

        SnapshotValidationStats stats = snapshotStatsCache.get(snapshotId);
        if (stats != null) {
            SnapshotMetadataManager.writeValidationStats(basePath, stats);
            logger.debug("Final metadata written: total={}, valid={}",
                    stats.getTotalRecords(), stats.getValidRecords());
        }

        ValidationRecordManager recordsManager = recordsManagers.remove(snapshotId);
        if (recordsManager != null) {
            recordsManager.close();
            logger.debug("RecordsManager closed");
        }

        snapshotLocks.remove(snapshotId);
    }

    /**
     * Fuerza el flush de los buffers de escritura.
     * 
     * @param snapshotId ID del snapshot
     * @throws IOException si hay error
     */
    public void flush(Long snapshotId) throws IOException {
        logger.debug("FLUSH requested for snapshot {}", snapshotId);

        ValidationRecordManager recordsManager = recordsManagers.get(snapshotId);
        if (recordsManager != null) {
            recordsManager.flush();
            logger.debug("Flushed RecordsManager for snapshot {}", snapshotId);
        }

        logger.debug("FLUSH COMPLETE for snapshot {}", snapshotId);
    }

    /**
     * Elimina datos de validación de un snapshot.
     * 
     * @param snapshotId ID del snapshot
     * @throws IOException si hay error
     */
    public void deleteSnapshot(Long snapshotId) throws IOException {
        logger.debug("DELETE VALIDATION DATA: snapshot={}", snapshotId);

        recordsCache.invalidate(snapshotId);
        finalizeSnapshot(snapshotId);

        String validationDir = String.format("%s/snapshot_%d/validation", basePath, snapshotId);
        if (Files.exists(Paths.get(validationDir))) {
            Files.walk(Paths.get(validationDir))
                    .sorted(Comparator.reverseOrder())
                    .forEach(path -> {
                        try {
                            Files.delete(path);
                            logger.debug("Deleted validation file: {}", path);
                        } catch (IOException e) {
                            logger.error("Failed to delete {}", path, e);
                        }
                    });
            logger.debug("VALIDATION DATA DELETED: snapshot={}, path={}", snapshotId, validationDir);
        } else {
            logger.warn("VALIDATION DATA NOT FOUND: snapshot={}, path={}", snapshotId, validationDir);
        }

        snapshotStatsCache.remove(snapshotId);
    }

    /**
     * Elimina solo archivos Parquet de validación (preserva validation_stats.json).
     * 
     * @param snapshotId ID del snapshot
     * @throws IOException si hay error
     */
    public void deleteParquetForSnapshot(Long snapshotId) throws IOException {
        logger.debug("Deleting validation parquet files for snapshot {}", snapshotId);

        SnapshotMetadata metadata = snapshotStore.getSnapshotMetadata(snapshotId);
        if (metadata == null) {
            logger.warn("No metadata found for snapshot {}, skipping validation parquet delete", snapshotId);
            return;
        }

        try (ValidationRecordManager manager = ValidationRecordManager.forReading(basePath, metadata, hadoopConf)) {
            manager.deleteParquetFiles();
        }
        logger.debug("Validation parquet deletion completed for snapshot {}", snapshotId);
    }

    // ========================================
    // GETTERS para acceso desde otras clases
    // ========================================

    public String getBasePath() {
        return basePath;
    }

    public Configuration getHadoopConf() {
        return hadoopConf;
    }
}
