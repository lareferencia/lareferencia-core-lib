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
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.lareferencia.core.metadata.ISnapshotStore;
import org.lareferencia.core.metadata.SnapshotMetadata;
import org.lareferencia.core.repository.parquet.OAIRecordManager;
import org.lareferencia.core.util.PathUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

/**
 * Service para migrar catálogo de formato Parquet a SQLite.
 * 
 * <p>
 * Lee registros del catálogo Parquet legacy usando {@link OAIRecordManager}
 * y los escribe en el nuevo formato SQLite usando
 * {@link OAIRecordCatalogRepository}.
 * </p>
 * 
 * <h2>Uso:</h2>
 * 
 * <pre>
 * MigrationResult result = migrationService.migrate(snapshotId, false);
 * System.out.println("Migrated " + result.recordsMigrated() + " records");
 * </pre>
 * 
 * <h2>Notas:</h2>
 * <ul>
 * <li>Los archivos Parquet originales NO se eliminan</li>
 * <li>Si ya existe catalog.db, la migración falla (no sobrescribe)</li>
 * <li>Conversión: Boolean deleted → boolean deleted (null → false)</li>
 * </ul>
 */
@Service
public class CatalogMigrationService {

    private static final Logger logger = LogManager.getLogger(CatalogMigrationService.class);

    private static final String CATALOG_SUBDIR = "catalog";
    private static final String SQLITE_DB_FILE = "catalog.db";
    private static final String PARQUET_FILE_PREFIX = "oai_records_batch_";

    @Value("${store.basepath:/tmp/data/}")
    private String basePath;

    @Value("${catalog.migration.batch-size:5000}")
    private int batchSize;

    @Autowired
    private ISnapshotStore snapshotStore;

    @Autowired
    private OAIRecordCatalogRepository sqliteRepository;

    private Configuration hadoopConf;

    /**
     * Resultado de una migración.
     */
    public record MigrationResult(
            long recordsMigrated,
            long recordsSkipped,
            long batchesProcessed,
            boolean success,
            String message) {
    }

    /**
     * Inicializa la configuración de Hadoop para lectura de Parquet.
     */
    private Configuration getHadoopConf() {
        if (hadoopConf == null) {
            hadoopConf = new Configuration();
            hadoopConf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
        }
        return hadoopConf;
    }

    /**
     * Migra el catálogo de un snapshot de Parquet a SQLite.
     * 
     * @param snapshotId ID del snapshot a migrar
     * @param dryRun     si true, solo cuenta registros sin migrar
     * @return resultado de la migración con estadísticas
     * @throws IOException si hay error de I/O
     */
    public MigrationResult migrate(Long snapshotId, boolean dryRun) throws IOException {
        logger.info("CATALOG MIGRATION: Starting for snapshot {} (dryRun={})", snapshotId, dryRun);

        // 1. Obtener metadata del snapshot
        SnapshotMetadata metadata = snapshotStore.getSnapshotMetadata(snapshotId);
        if (metadata == null) {
            return new MigrationResult(0, 0, 0, false,
                    "Snapshot " + snapshotId + " not found");
        }

        // 2. Verificar que existen archivos Parquet
        Path catalogPath = getCatalogPath(metadata);
        if (!hasParquetFiles(catalogPath)) {
            return new MigrationResult(0, 0, 0, false,
                    "No Parquet files found in " + catalogPath);
        }

        // 3. Verificar que NO existe catalog.db (evitar sobrescribir)
        Path sqliteDbPath = catalogPath.resolve(SQLITE_DB_FILE);
        if (Files.exists(sqliteDbPath)) {
            return new MigrationResult(0, 0, 0, false,
                    "SQLite catalog already exists at " + sqliteDbPath
                            + ". Delete it first if you want to re-migrate.");
        }

        // 4. Si es dry-run, solo contar
        if (dryRun) {
            long count = countParquetRecords(metadata);
            return new MigrationResult(count, 0, 0, true,
                    "Dry run: Would migrate " + count + " records");
        }

        // 5. Ejecutar migración
        return executeMigration(metadata);
    }

    /**
     * Ejecuta la migración real de Parquet a SQLite.
     */
    private MigrationResult executeMigration(SnapshotMetadata metadata) throws IOException {
        Long snapshotId = metadata.getSnapshotId();
        long recordsMigrated = 0;
        long recordsSkipped = 0;
        long batchesProcessed = 0;

        try {
            // Inicializar SQLite (sin copiar de anterior)
            sqliteRepository.initializeSnapshot(metadata, null);

            // Leer de Parquet y escribir a SQLite en batches
            try (OAIRecordManager parquetReader = OAIRecordManager.forReading(
                    basePath, metadata, getHadoopConf())) {

                Iterator<org.lareferencia.core.repository.parquet.OAIRecord> iterator = parquetReader.iterator();
                List<OAIRecord> batch = new ArrayList<>(batchSize);

                while (iterator.hasNext()) {
                    org.lareferencia.core.repository.parquet.OAIRecord parquetRecord = iterator.next();

                    // Convertir Parquet OAIRecord → SQLite OAIRecord
                    OAIRecord sqliteRecord = convertRecord(parquetRecord);

                    if (sqliteRecord != null) {
                        batch.add(sqliteRecord);
                        recordsMigrated++;
                    } else {
                        recordsSkipped++;
                    }

                    // Flush batch cuando alcanza el tamaño
                    if (batch.size() >= batchSize) {
                        sqliteRepository.upsertBatch(snapshotId, batch);
                        batchesProcessed++;
                        logger.debug("CATALOG MIGRATION: Processed batch {} ({} records total)",
                                batchesProcessed, recordsMigrated);
                        batch.clear();
                    }
                }

                // Flush registros restantes
                if (!batch.isEmpty()) {
                    sqliteRepository.upsertBatch(snapshotId, batch);
                    batchesProcessed++;
                }
            }

            // Finalizar snapshot SQLite
            sqliteRepository.finalizeSnapshot(snapshotId);

            logger.info("CATALOG MIGRATION: Completed for snapshot {} - {} records migrated in {} batches",
                    snapshotId, recordsMigrated, batchesProcessed);

            return new MigrationResult(recordsMigrated, recordsSkipped, batchesProcessed, true,
                    "Successfully migrated " + recordsMigrated + " records");

        } catch (Exception e) {
            logger.error("CATALOG MIGRATION: Failed for snapshot {}", snapshotId, e);
            // Intentar limpiar el SQLite parcial
            try {
                sqliteRepository.finalizeSnapshot(snapshotId);
            } catch (Exception ignored) {
            }
            return new MigrationResult(recordsMigrated, recordsSkipped, batchesProcessed, false,
                    "Migration failed: " + e.getMessage());
        }
    }

    /**
     * Convierte un OAIRecord de Parquet al formato SQLite.
     * 
     * @param parquetRecord registro en formato Parquet
     * @return registro en formato SQLite, o null si es inválido
     */
    private OAIRecord convertRecord(org.lareferencia.core.repository.parquet.OAIRecord parquetRecord) {
        if (parquetRecord == null || parquetRecord.getIdentifier() == null) {
            return null;
        }

        // Conversión directa - los campos son compatibles
        // Nota: Boolean deleted (nullable) → boolean deleted (null = false)
        boolean deleted = parquetRecord.getDeleted() != null && parquetRecord.getDeleted();

        return new OAIRecord(
                parquetRecord.getId(),
                parquetRecord.getIdentifier(),
                parquetRecord.getDatestamp(),
                parquetRecord.getOriginalMetadataHash(),
                deleted);
    }

    /**
     * Cuenta registros en el catálogo Parquet sin migrar.
     */
    private long countParquetRecords(SnapshotMetadata metadata) throws IOException {
        long count = 0;
        try (OAIRecordManager reader = OAIRecordManager.forReading(
                basePath, metadata, getHadoopConf())) {
            Iterator<org.lareferencia.core.repository.parquet.OAIRecord> iterator = reader.iterator();
            while (iterator.hasNext()) {
                iterator.next();
                count++;
            }
        }
        return count;
    }

    /**
     * Verifica si existen archivos Parquet en el directorio del catálogo.
     */
    private boolean hasParquetFiles(Path catalogPath) {
        if (!Files.exists(catalogPath)) {
            return false;
        }
        try {
            return Files.list(catalogPath)
                    .anyMatch(p -> p.getFileName().toString().startsWith(PARQUET_FILE_PREFIX)
                            && p.getFileName().toString().endsWith(".parquet"));
        } catch (IOException e) {
            return false;
        }
    }

    /**
     * Obtiene la ruta al directorio del catálogo para un snapshot.
     */
    private Path getCatalogPath(SnapshotMetadata metadata) {
        String snapshotPath = PathUtils.getSnapshotPath(basePath, metadata);
        return Paths.get(snapshotPath, CATALOG_SUBDIR);
    }
}
