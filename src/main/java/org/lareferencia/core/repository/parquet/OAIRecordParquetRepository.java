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
import org.lareferencia.core.repository.parquet.OAIRecord;
import org.lareferencia.core.metadata.SnapshotMetadata;
import org.lareferencia.core.util.PathUtils;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Repository;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * REPOSITORIO PARQUET PARA CATÁLOGO OAI
 * 
 * ARQUITECTURA:
 * =============
 * - Gestiona OAIRecordManager de forma centralizada
 * - Provee API de alto nivel para workers y servicios
 * - Maneja ciclo de vida completo de managers (init/write/close)
 * - Thread-safe con managers persistentes por snapshot
 * 
 * FUNCIONALIDADES:
 * ================
 * 1. ESCRITURA:
 *    - initializeSnapshot(): Crea manager para escritura
 *    - saveRecord(): Guarda OAIRecord individual (buffered)
 *    - finalizeSnapshot(): Cierra manager y hace flush final
 *    - flush(): Fuerza escritura de buffer a disco
 * 
 * 2. LECTURA:
 *    - readAllRecords(): Carga todos los records (para datasets pequeños)
 *    - iterateRecords(): Streaming lazy (recomendado para grandes)
 *    - countRecords(): Cuenta sin cargar en memoria
 *    - queryRecords(): Paginación y filtrado (futuro)
 * 
 * 3. GESTIÓN:
 *    - deleteSnapshot(): Elimina directorio completo
 *    - cleanup(): Cierra todos los managers activos
 * 
 * CICLO DE VIDA TÍPICO:
 * =====================
 * 1. initializeSnapshot(snapshotId) → Crea manager
 * 2. saveRecord(snapshotId, record) × N → Escritura buffered
 * 3. flush(snapshotId) (opcional) → Persistencia periódica
 * 4. finalizeSnapshot(snapshotId) → Cierra y flush final
 * 
 * DIFERENCIAS CON ValidationStatParquetRepository:
 * ================================================
 * - NO mantiene metadata en memoria (solo el manager)
 * - NO calcula estadísticas (catálogo simple)
 * - Enfocado en CRUD básico de records OAI
 * 
 * THREAD SAFETY:
 * ==============
 * - Managers persistentes en ConcurrentHashMap
 * - Múltiples threads pueden llamar saveRecord() concurrentemente
 * - Sincronización manejada por OAIRecordManager internamente
 * 
 * @author LA Referencia Team
 */
@Repository
public class OAIRecordParquetRepository {

    private static final Logger logger = LogManager.getLogger(OAIRecordParquetRepository.class);

    @Value("${store.basepath:/tmp/data/}")
    private String basePath;

    @Value("${parquet.catalog.records-per-file:10000}")
    private int recordsPerFile;

    @Value("${parquet.compression:SNAPPY}")
    private String compressionCodec;

    @Value("${parquet.page.size:1048576}") // 1 MB
    private int pageSize;

    @Value("${parquet.enable.dictionary:true}")
    private boolean enableDictionary;

    private Configuration hadoopConf;
    
    // MANAGER PERSISTENTE: Se reutiliza entre llamadas para aprovechar buffer de 10k
    private final Map<Long, OAIRecordManager> recordManagers = new ConcurrentHashMap<>();

    @PostConstruct
    public void init() {
        hadoopConf = new Configuration();
        hadoopConf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
        
        // Aplicar configuración desde properties
        hadoopConf.set("parquet.compression", compressionCodec);
        hadoopConf.set("parquet.page.size", String.valueOf(pageSize));
        hadoopConf.set("parquet.enable.dictionary", String.valueOf(enableDictionary));
        
        try {
            Files.createDirectories(Paths.get(basePath));
            logger.info("OAI RECORD REPOSITORY INITIALIZED: basePath={}, compression={}, pageSize={}", 
                       basePath, compressionCodec, pageSize);
        } catch (IOException e) {
            logger.error("Failed to create base path: {}", basePath, e);
        }
    }

    @PreDestroy
    public void cleanup() {
        // Cerrar todos los managers persistentes
        logger.info("REPOSITORY SHUTDOWN: Closing {} active managers", recordManagers.size());
        
        recordManagers.forEach((snapshotId, manager) -> {
            try {
                manager.close();
                logger.info("SHUTDOWN: Closed manager for snapshot {}", snapshotId);
            } catch (IOException e) {
                logger.error("SHUTDOWN: Error closing manager for snapshot {}", snapshotId, e);
            }
        });
        
        recordManagers.clear();
        logger.info("REPOSITORY SHUTDOWN COMPLETE");
    }

    /**
     * Inicializa un snapshot para escritura.
     * 
     * CICLO DE VIDA:
     * 1. initializeSnapshot() → Crea directorio y manager
     * 2. saveRecord() × N → Escritura incremental record por record
     * 3. finalizeSnapshot() → Cierra manager y flush final
     * 
     * @param snapshotId ID del snapshot
     * @throws IOException si hay error
     */
    public void initializeSnapshot(SnapshotMetadata snapshotMetadata) throws IOException {
        Long snapshotId = snapshotMetadata.getSnapshotId();
        logger.info("INITIALIZE SNAPSHOT: snapshot={}, network={}", snapshotId, snapshotMetadata.getNetworkAcronym());
        
        try {
            // Crear directorio del snapshot usando PathUtils
            String snapshotDir = PathUtils.getSnapshotPath(basePath, snapshotMetadata);
            Files.createDirectories(Paths.get(snapshotDir));
            
            // Determinar flush threshold basado en configuración
            int flushThreshold = determineFlushThreshold(snapshotId);
            
            // Crear manager para escritura con threshold configurado
            OAIRecordManager manager = OAIRecordManager.forWriting(basePath, snapshotMetadata, hadoopConf, flushThreshold);
            recordManagers.put(snapshotId, manager);
            
            logger.info("SNAPSHOT INITIALIZED: snapshot={}, network={}, path={}, flushThreshold={}", 
                       snapshotId, snapshotMetadata.getNetworkAcronym(), snapshotDir, flushThreshold);
            
        } catch (IOException e) {
            logger.error("INITIALIZATION FAILED: snapshot={}, error={}", snapshotId, e.getMessage());
            // Limpiar manager si falló
            recordManagers.remove(snapshotId);
            throw e;
        }
    }

    /**
     * Guarda un record en Parquet (LLAMADA INDIVIDUAL - TRANSPARENTE)
     * 
     * THREAD-SAFE: Múltiples threads pueden llamar concurrentemente.
     * Los writers internos manejan sincronización con synchronized.
     * 
     * BUFFERING AUTOMÁTICO:
     * - Acumula en memoria hasta 10k registros
     * - Auto-flush al alcanzar umbral
     * - Crea archivos batch_N.parquet automáticamente
     * 
     * @param snapshotId ID del snapshot
     * @param record datos del record OAI
     * @throws IOException si hay error
     */
    public void saveRecord(Long snapshotId, OAIRecord record) throws IOException {
        
        if (record == null) {
            throw new IllegalArgumentException("Record cannot be null");
        }
        
        // Obtener manager persistente (creado en initializeSnapshot)
        OAIRecordManager manager = recordManagers.get(snapshotId);
        if (manager == null) {
            throw new IllegalStateException("Snapshot " + snapshotId + " not initialized. Call initializeSnapshot() first.");
        }
        
        // Escribir record (synchronized internamente en el manager)
        manager.writeRecord(record);
    }

    /**
     * Cierra el manager de un snapshot específico y libera recursos.
     * 
     * CICLO DE VIDA:
     * 1. initializeSnapshot() → Crea manager
     * 2. saveRecord() × N → Escritura incremental
     * 3. finalizeSnapshot() → Cierra manager y flush final
     * 
     * Debe llamarse cuando se termina de procesar completamente un snapshot.
     * 
     * IMPORTANTE: Después de cerrar, cualquier nueva escritura requiere initializeSnapshot().
     * 
     * @param snapshotId el ID del snapshot a finalizar
     * @throws IOException si hay error al cerrar el manager
     */
    public void finalizeSnapshot(Long snapshotId) throws IOException {
        logger.info("FINALIZE SNAPSHOT: Closing manager for snapshot {}", snapshotId);
        
        // Cerrar manager
        OAIRecordManager manager = recordManagers.remove(snapshotId);
        if (manager != null) {
            manager.close();
            logger.info("SNAPSHOT FINALIZED: {} (manager closed, {} records written in {} batches)", 
                       snapshotId, manager.getTotalRecordsWritten(), manager.getBatchCount());
        } else {
            logger.warn("FINALIZE: No active manager found for snapshot {}", snapshotId);
        }
    }

    /**
     * Fuerza el flush de los buffers de escritura para un snapshot específico.
     * 
     * Los managers mantienen buffers de 10k registros para eficiencia.
     * Este método fuerza la escritura de cualquier dato pendiente en los buffers.
     * 
     * CUÁNDO USAR:
     * - Antes de leer datos que acabas de escribir
     * - Para garantizar durabilidad en puntos críticos
     * - Al finalizar un lote de procesamiento
     * 
     * NOTA: finalizeSnapshot() ya hace flush automáticamente al cerrar
     * 
     * @param snapshotId el ID del snapshot a hacer flush
     * @throws IOException si hay error en la operación de flush
     */
    public void flush(Long snapshotId) throws IOException {
        logger.debug("FLUSH requested for snapshot {}", snapshotId);
        
        OAIRecordManager manager = recordManagers.get(snapshotId);
        if (manager != null) {
            manager.flush();
            logger.debug("FLUSH completed for snapshot {} ({} records written)", 
                        snapshotId, manager.getTotalRecordsWritten());
        } else {
            logger.warn("FLUSH: No active manager found for snapshot {}", snapshotId);
        }
    }

    /**
     * Elimina todos los datos de un snapshot (directorio completo).
     * Cierra el manager si está activo y elimina el directorio del snapshot.
     *
     * @param snapshotId el ID del snapshot a eliminar
     * @throws IOException si falla la eliminación
     */
    public void deleteSnapshot(Long snapshotId) throws IOException {
        logger.info("DELETE SNAPSHOT: {}", snapshotId);

        // Finalizar snapshot para cerrar managers
        if (recordManagers.containsKey(snapshotId)) {
            finalizeSnapshot(snapshotId);
        }

        // Eliminar directorio del snapshot
        String snapshotDir = String.format("%s/snapshot_%d", basePath, snapshotId);
        if (Files.exists(Paths.get(snapshotDir))) {
            try {
                Files.walk(Paths.get(snapshotDir))
                    .sorted(java.util.Comparator.reverseOrder())
                    .forEach(path -> {
                        try {
                            Files.delete(path);
                        } catch (IOException e) {
                            logger.error("Failed to delete: {}", path, e);
                        }
                    });
                logger.info("SNAPSHOT DELETED: {}", snapshotId);
            } catch (IOException e) {
                logger.error("Failed to delete snapshot directory: {}", snapshotDir, e);
                throw e;
            }
        }
    }

    /**
     * Lee todos los records de un snapshot.
     * NOTA: Para datasets grandes, usar iterateRecords() en su lugar.
     * 
     * @param snapshotMetadata metadata del snapshot
     * @return lista de todos los records
     * @throws IOException si hay error
     */
    public List<OAIRecord> readAllRecords(SnapshotMetadata snapshotMetadata) throws IOException {
        logger.debug("READ ALL RECORDS: snapshot={}, network={}", 
                    snapshotMetadata.getSnapshotId(), snapshotMetadata.getNetworkAcronym());
        
        try (OAIRecordManager reader = OAIRecordManager.forReading(basePath, snapshotMetadata, hadoopConf)) {
            return reader.readAll();
        }
    }

    /**
     * Retorna un iterable para streaming lazy sobre records de un snapshot.
     * RECOMENDADO para datasets grandes que no caben en memoria.
     * 
     * Ejemplo de uso:
     * <pre>
     * for (OAIRecord record : repository.iterateRecords(snapshotMetadata)) {
     *     // Procesar record sin cargar todo en memoria
     *     processRecord(record);
     * }
     * </pre>
     * 
     * @param snapshotMetadata metadata del snapshot
     * @return iterable lazy sobre records
     * @throws IOException si hay error
     */
    public Iterable<OAIRecord> iterateRecords(SnapshotMetadata snapshotMetadata) throws IOException {
        logger.debug("ITERATE RECORDS: snapshot={}, network={}", 
                    snapshotMetadata.getSnapshotId(), snapshotMetadata.getNetworkAcronym());
        return OAIRecordManager.iterate(basePath, snapshotMetadata, hadoopConf);
    }

    /**
     * Cuenta el total de records en un snapshot sin cargarlos en memoria.
     * 
     * @param snapshotMetadata metadata del snapshot
     * @return número total de records
     * @throws IOException si hay error
     */
    public long countRecords(SnapshotMetadata snapshotMetadata) throws IOException {
        logger.debug("COUNT RECORDS: snapshot={}, network={}", 
                    snapshotMetadata.getSnapshotId(), snapshotMetadata.getNetworkAcronym());
        
        try (OAIRecordManager reader = OAIRecordManager.forReading(basePath, snapshotMetadata, hadoopConf)) {
            return reader.countRecords();
        }
    }

    /**
     * Obtiene información del manager activo de un snapshot.
     * Útil para debugging y monitoreo.
     * 
     * @param snapshotId ID del snapshot
     * @return mapa con información del manager, o null si no hay manager activo
     */
    public Map<String, Object> getManagerInfo(Long snapshotId) {
        OAIRecordManager manager = recordManagers.get(snapshotId);
        if (manager == null) {
            return null;
        }
        
        return Map.of(
            "snapshotId", snapshotId,
            "recordsWritten", manager.getTotalRecordsWritten(),
            "batchCount", manager.getBatchCount(),
            "isActive", true
        );
    }

    /**
     * Verifica si un snapshot tiene un manager activo.
     * 
     * @param snapshotId ID del snapshot
     * @return true si hay manager activo
     */
    public boolean hasActiveManager(Long snapshotId) {
        return recordManagers.containsKey(snapshotId);
    }

    /**
     * Retorna el total de managers activos.
     * Útil para monitoreo de recursos.
     * 
     * @return número de managers activos
     */
    public int getActiveManagerCount() {
        return recordManagers.size();
    }

    /**
     * Retorna la configuración Hadoop usada por el repositorio.
     * Útil para tests y debugging.
     * 
     * @return configuración Hadoop
     */
    public Configuration getHadoopConfiguration() {
        return hadoopConf;
    }

    /**
     * Retorna la ruta base configurada.
     * 
     * @return ruta base del repositorio
     */
    public String getBasePath() {
        return basePath;
    }
    
    /**
     * Retorna el flush threshold configurado.
     * 
     * @param snapshotId ID del snapshot (no usado, se mantiene por compatibilidad)
     * @return threshold de flush a usar
     */
    private int determineFlushThreshold(Long snapshotId) {
        logger.debug("Using configured flush threshold: {}", recordsPerFile);
        return recordsPerFile;
    }
}
