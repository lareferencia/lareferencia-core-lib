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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroup;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Types;
import org.lareferencia.core.metadata.SnapshotMetadata;
import org.lareferencia.core.util.PathUtils;

import java.io.IOException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

/**
 * OAI RECORD MANAGER: Gestiona lectura y escritura del catálogo inmutable de registros OAI en Parquet.
 * 
 * ARQUITECTURA:
 * - CATÁLOGO INMUTABLE: Los registros se escriben UNA SOLA VEZ durante harvesting
 * - SIN ACTUALIZACIONES: Una vez escrito, nunca se modifica
 * - SIN ESTADO DE VALIDACIÓN: Solo datos del harvesting (validación está en RecordValidation)
 * - ORGANIZADO POR SNAPSHOT: Cada snapshot tiene su directorio independiente
 * 
 * ESTRUCTURA DE ARCHIVOS:
 * {basePath}/{NETWORK}/snapshots/snapshot_{id}/catalog/oai_records_batch_*.parquet
 * 
 * FUNCIONALIDADES:
 * - ESCRITURA: Buffer interno inteligente con auto-flush
 * - LECTURA: Streaming sobre múltiples archivos batch con iterator lazy
 * - SCHEMA: Simplificado solo con campos del catálogo OAI
 * 
 * THREAD SAFETY:
 * - ESCRITURA: ParquetWriter NO es thread-safe - usar synchronized
 * - LECTURA: Cada manager es independiente - crear nueva instancia por thread
 * - NO compartir instancias entre threads
 * - Repository.getIterator() crea nueva instancia automáticamente
 * 
 * ESTRATEGIA DE BATCHING (ESCRITURA):
 * - Buffer interno: Acumula registros en memoria
 * - Flush automático: Cuando alcanza umbral (configurable mediante `parquet.catalog.records-per-file`, por defecto 100000)
 * - Flush manual: Mediante flush() para garantizar persistencia
 * - Archivos múltiples: Cada flush crea un archivo oai_records_batch_XXXXX.parquet
 * 
 * ESTRATEGIA MULTI-ARCHIVO (LECTURA):
 * - Lee TODOS los archivos oai_records_batch_*.parquet de un snapshot
 * - Procesa batches en orden numérico (batch_1, batch_2, ...)
 * - Transparente: El caller no sabe que hay múltiples archivos
 * - Streaming: Lee un archivo a la vez (no carga todo en memoria)
 * - Iterator: Soporta iteración lazy sin cargar todo el dataset
 * 
 * EJEMPLOS DE USO:
 * 
 * 1. ESCRITURA (Harvesting):
 * <pre>
 * try (OAIRecordManager writer = OAIRecordManager.forWriting(basePath, snapshotMetadata, conf)) {
 *     for (OAIRecord record : harvestedRecords) {
 *         writer.writeRecord(record);
 *     }
 *     writer.flush(); // Garantizar persistencia
 * }
 * </pre>
 * 
 * 2. LECTURA LAZY (RECOMENDADO - Thread-safe):
 * <pre>
 * // Cada thread obtiene su propia instancia
 * try (OAIRecordManager reader = repository.getIterator(snapshotMetadata)) {
 *     Iterator&lt;OAIRecord&gt; iterator = reader.iterator();
 *     while (iterator.hasNext()) {
 *         OAIRecord record = iterator.next();
 *         validateRecord(record);
 *     }
 * }
 * </pre>
 * 
 * 3. LECTURA CON FOR-EACH:
 * <pre>
 * try (OAIRecordManager reader = OAIRecordManager.forReading(basePath, snapshotMetadata, conf)) {
 *     for (OAIRecord record : reader) {
 *         processRecord(record);
 *     }
 * }
 * </pre>
 */
public final class OAIRecordManager implements AutoCloseable, Iterable<OAIRecord> {
    
    private static final Logger logger = LogManager.getLogger(OAIRecordManager.class);
    
    // Umbrales para flush automático (escritura) - valor por defecto
    private static final int DEFAULT_FLUSH_THRESHOLD_RECORDS = 100000;
    
    // Subdirectorio para catálogo de registros OAI
    private static final String CATALOG_SUBDIR = "catalog";
    private static final String FILE_PREFIX = "oai_records_batch_";
    
    /**
     * ESQUEMA PARQUET DEL CATÁLOGO OAI:
     * - id: STRING (required) - Hash MD5 del identifier (garantiza unicidad)
     * - identifier: STRING (required) - Identificador OAI único del registro
     * - datestamp: TIMESTAMP (required) - Fecha de última modificación según OAI-PMH
     * - original_metadata_hash: STRING (required) - Hash MD5 del XML original cosechado
     * - deleted: BOOLEAN (required) - Flag que indica si el registro fue eliminado
     */
    private static final MessageType SCHEMA = Types.buildMessage()
        .required(PrimitiveType.PrimitiveTypeName.BINARY)
            .as(LogicalTypeAnnotation.stringType())
            .named("id")
        .required(PrimitiveType.PrimitiveTypeName.BINARY)
            .as(LogicalTypeAnnotation.stringType())
            .named("identifier")
        .required(PrimitiveType.PrimitiveTypeName.INT64)
            .as(LogicalTypeAnnotation.timestampType(true, LogicalTypeAnnotation.TimeUnit.MILLIS))
            .named("datestamp")
        .required(PrimitiveType.PrimitiveTypeName.BINARY)
            .as(LogicalTypeAnnotation.stringType())
            .named("original_metadata_hash")
        .required(PrimitiveType.PrimitiveTypeName.BOOLEAN)
            .named("deleted")
        .named("OAIRecordCatalog");
    
    private final String basePath;
    private final SnapshotMetadata snapshotMetadata;
    private final Long snapshotId;
    private final Configuration hadoopConf;
    private final int flushThreshold;  // Configurable flush threshold
    
    // Estado de ESCRITURA
    private ParquetWriter<Group> currentWriter;
    private long recordsInCurrentBatch = 0;
    private long totalRecordsWritten = 0;
    private int batchNumber = 0;
    
    // Estado de LECTURA (SIN CACHE - readers se crean/cierran on-demand)
    private List<Path> batchFiles;
    private int currentBatchIndex = 0;
    private long recordsRead = 0;
    
    // Cache temporal para iteración (solo durante un ciclo de iteración)
    private List<OAIRecord> currentBatchRecords = null;
    private int currentRecordIndex = 0;
    
    private OAIRecordManager(String basePath, SnapshotMetadata snapshotMetadata, Configuration hadoopConf, int flushThreshold) {
        if (snapshotMetadata == null) {
            throw new IllegalArgumentException("snapshotMetadata cannot be null");
        }
        if (snapshotMetadata.getSnapshotId() == null) {
            throw new IllegalArgumentException("snapshotMetadata.snapshotId cannot be null");
        }
        
        this.basePath = basePath;
        this.snapshotMetadata = snapshotMetadata;
        this.snapshotId = snapshotMetadata.getSnapshotId();
        this.hadoopConf = hadoopConf;
        this.flushThreshold = flushThreshold;
        this.currentWriter = null;
        this.batchFiles = null;
        this.currentBatchRecords = null;
    }
    
    // ============================================================================
    // FACTORY METHODS
    // ============================================================================
    
    /**
     * Crea un manager para ESCRITURA con buffer interno.
     * Gestiona automáticamente múltiples archivos batch según performance.
     * 
     * @param basePath ruta base (ej: /data/parquet)
     * @param snapshotMetadata metadata del snapshot (contiene snapshotId y networkAcronym)
     * @param hadoopConf configuración Hadoop
     * @return manager listo para escritura
     * @throws IOException si falla
     */
    public static OAIRecordManager forWriting(String basePath, SnapshotMetadata snapshotMetadata, Configuration hadoopConf) 
            throws IOException {
        return forWriting(basePath, snapshotMetadata, hadoopConf, DEFAULT_FLUSH_THRESHOLD_RECORDS);
    }
    
    /**
     * Crea un manager para ESCRITURA con flush threshold personalizado.
     * 
     * @param basePath ruta base (ej: /data/parquet)
     * @param snapshotMetadata metadata del snapshot (contiene snapshotId y networkAcronym)
     * @param hadoopConf configuración Hadoop
     * @param flushThreshold número de registros antes de hacer flush automático
     * @return manager listo para escritura
     * @throws IOException si falla
     */
    public static OAIRecordManager forWriting(String basePath, SnapshotMetadata snapshotMetadata, Configuration hadoopConf, int flushThreshold) 
            throws IOException {
        logger.info("OAI RECORD MANAGER: Creating writer for snapshot {} network {} (flushThreshold={})", 
                   snapshotMetadata.getSnapshotId(), snapshotMetadata.getNetwork().getAcronym(), flushThreshold);
        return new OAIRecordManager(basePath, snapshotMetadata, hadoopConf, flushThreshold);
    }
    
    /**
     * Crea un manager para LECTURA que lee TODOS los archivos batch de un snapshot.
     * Busca automáticamente todos los oai_records_batch_*.parquet
     * 
     * THREAD-SAFE: Cada llamada crea una NUEVA instancia.
     * Múltiples threads pueden leer el mismo snapshot concurrentemente.
     * 
     * OPCIONES DE LECTURA:
     * - iterator(): Iteración lazy (RECOMENDADO para datasets grandes)
     * - Iterable: Usar for-each sobre la instancia
     * 
     * @param basePath ruta base (ej: /data/parquet)
     * @param snapshotMetadata metadata del snapshot (contiene snapshotId y networkAcronym)
     * @param hadoopConf configuración Hadoop
     * @return manager listo para lectura
     * @throws IOException si falla
     */
    public static OAIRecordManager forReading(String basePath, SnapshotMetadata snapshotMetadata, Configuration hadoopConf) 
            throws IOException {
        OAIRecordManager manager = new OAIRecordManager(basePath, snapshotMetadata, hadoopConf, DEFAULT_FLUSH_THRESHOLD_RECORDS);
        manager.initializeReader();
        return manager;
    }
    
    // ============================================================================
    // ESCRITURA - WRITE OPERATIONS
    // ============================================================================
    
    /**
     * Crea un nuevo archivo batch y su writer.
     * Llamado automáticamente cuando se necesita flush.
     */
    private void createNewBatchWriter() throws IOException {
        if (currentWriter != null) {
            currentWriter.close();
        }
        
        batchNumber++;
        String snapshotPath = PathUtils.getSnapshotPath(basePath, snapshotMetadata);
        String batchPath = snapshotPath + "/" + CATALOG_SUBDIR + "/" + FILE_PREFIX + batchNumber + ".parquet";
        Path path = new Path(batchPath);
        
        logger.info("OAI RECORD MANAGER: Creating batch file #{} at {}", batchNumber, batchPath);
        
        Configuration conf = new Configuration(hadoopConf);
        org.apache.parquet.hadoop.example.GroupWriteSupport.setSchema(SCHEMA, conf);
        
        currentWriter = ExampleParquetWriter.builder(path)
            .withConf(conf)
            .withWriteMode(org.apache.parquet.hadoop.ParquetFileWriter.Mode.CREATE)
            .withCompressionCodec(CompressionCodecName.SNAPPY)
            .withDictionaryEncoding(true)
            .withPageSize(1 << 20)        // 1 MB
            .withRowGroupSize(128L << 20) // 128 MB
            .build();
        
        recordsInCurrentBatch = 0;
    }
    
    /**
     * Escribe un record completo (1 fila) con auto-flush inteligente.
     * Crea automáticamente nuevo archivo batch cuando alcanza umbrales.
     * 
     * INMUTABILIDAD: Los records escritos NUNCA se actualizan.
     * THREAD-SAFE: synchronized para acceso concurrente seguro
     * 
     * @param record datos del record OAI
     * @throws IOException si falla
     */
    public synchronized void writeRecord(OAIRecord record) throws IOException {
        if (record == null) {
            logger.warn("OAI RECORD MANAGER: Null record, skipping");
            return;
        }
        
        // Validar campos requeridos
        if (record.getId() == null || record.getIdentifier() == null || 
            record.getDatestamp() == null || record.getOriginalMetadataHash() == null ||
            record.getDeleted() == null) {
            logger.error("OAI RECORD MANAGER: Invalid record (missing required fields), skipping: {}", record);
            return;
        }
        
        // Crear primer writer o verificar si necesita flush automático
        if (currentWriter == null) {
            createNewBatchWriter();
        } else if (recordsInCurrentBatch >= flushThreshold) {
            logger.info("OAI RECORD MANAGER: Auto-flush triggered at {} records (threshold={})", 
                       recordsInCurrentBatch, flushThreshold);
            flush();
            // Crear nuevo writer inmediatamente después del auto-flush
            createNewBatchWriter();
        }
        
        Group group = new SimpleGroup(SCHEMA);
        
        // Campos requeridos
        group.append("id", record.getId().trim());
        group.append("identifier", record.getIdentifier().trim());
        group.append("datestamp", Timestamp.valueOf(record.getDatestamp()).getTime());
        group.append("original_metadata_hash", record.getOriginalMetadataHash().trim());
        group.append("deleted", record.getDeleted());
        
        currentWriter.write(group);
        recordsInCurrentBatch++;
        totalRecordsWritten++;
        
        if (flushThreshold > 0 && totalRecordsWritten % flushThreshold == 0) {
            logger.debug("OAI RECORD MANAGER: Written {} records total ({} in current batch)", 
                        totalRecordsWritten, recordsInCurrentBatch);
        }
    }
    
    /**
     * Fuerza escritura del batch actual a disco y crea nuevo writer.
     * IMPORTANTE: Debe llamarse desde el Worker antes de confirmar snapshot.
     * 
     * THREAD-SAFE: synchronized para acceso concurrente seguro
     * 
     * @throws IOException si falla
     */
    public synchronized void flush() throws IOException {
        if (currentWriter != null && recordsInCurrentBatch > 0) {
            logger.info("OAI RECORD MANAGER: Flushing batch #{} with {} records", batchNumber, recordsInCurrentBatch);
            currentWriter.close();
            currentWriter = null;
            recordsInCurrentBatch = 0;
            // NO crear nuevo writer aquí - se creará en el próximo writeRecord() si es necesario
        }
    }
    
    // ============================================================================
    // LECTURA - READ OPERATIONS
    // ============================================================================
    
    /**
     * Inicializa el lector buscando todos los archivos batch del snapshot.
     */
    private void initializeReader() throws IOException {
        String snapshotPath = PathUtils.getSnapshotPath(basePath, snapshotMetadata);
        String catalogDir = snapshotPath + "/" + CATALOG_SUBDIR;
        Path catalogPath = new Path(catalogDir);
        
        FileSystem fs = FileSystem.get(hadoopConf);
        
        // Buscar todos los archivos oai_records_batch_*.parquet
        PathFilter batchFilter = path -> path.getName().startsWith(FILE_PREFIX) 
                                       && path.getName().endsWith(".parquet");
        
        FileStatus[] batchStatuses = fs.listStatus(catalogPath, batchFilter);
        
        if (batchStatuses == null || batchStatuses.length == 0) {
            logger.warn("OAI RECORD MANAGER: No batch files found for snapshot {}", snapshotId);
            batchFiles = new ArrayList<>();
            return;
        }
        
        // Ordenar por nombre (batch_1, batch_2, batch_3, ...)
        Arrays.sort(batchStatuses, Comparator.comparing(fs1 -> fs1.getPath().getName()));
        
        batchFiles = new ArrayList<>();
        for (FileStatus status : batchStatuses) {
            batchFiles.add(status.getPath());
        }
        
        logger.info("OAI RECORD MANAGER: Found {} batch files for snapshot {}", batchFiles.size(), snapshotId);
    }
    
    /**
     * Lee el siguiente archivo batch COMPLETO sin cachear el reader.
     * Abre, lee todo el batch, cierra inmediatamente.
     * SIN CACHE: cada batch se lee en una operación atómica.
     */
    private boolean loadNextBatch() throws IOException {
        if (batchFiles == null || currentBatchIndex >= batchFiles.size()) {
            return false;  // No hay más archivos
        }
        
        Path batchPath = batchFiles.get(currentBatchIndex);
        logger.debug("OAI RECORD MANAGER: Loading batch file {}/{}: {} (NO CACHE)", 
                    currentBatchIndex + 1, batchFiles.size(), batchPath.getName());
        
        // Leer TODO el batch de una vez (sin cachear reader)
        List<OAIRecord> batchRecords = new ArrayList<>();
        try (ParquetReader<Group> reader = ParquetReader.builder(new GroupReadSupport(), batchPath)
                .withConf(hadoopConf)
                .build()) {
            
            Group group;
            while ((group = reader.read()) != null) {
                batchRecords.add(convertGroupToRecord(group));
            }
        }
        
        logger.debug("OAI RECORD MANAGER: Loaded {} records from batch {} (reader closed)", 
                    batchRecords.size(), batchPath.getName());
        
        currentBatchRecords = batchRecords;
        currentRecordIndex = 0;
        currentBatchIndex++;
        
        return true;
    }
    
    // ============================================================================
    // ITERADOR - LAZY ITERATION (RECOMENDADO PARA DATASETS GRANDES)
    // ============================================================================
    
    /**
     * Retorna un iterator que lee records de forma lazy (streaming).
     * NO carga todo el dataset en memoria - ideal para datasets grandes.
     * 
     * THREAD-SAFE: Cada instancia de manager tiene su propio estado.
     * No compartir managers entre threads.
     * 
     * @return iterator lazy sobre todos los records
     */
    @Override
    public Iterator<OAIRecord> iterator() {
        return new RecordIterator();
    }
    
    /**
     * Iterador interno que lee records de forma lazy desde archivos Parquet.
     * Maneja automáticamente múltiples archivos batch de forma transparente.
     */
    private class RecordIterator implements Iterator<OAIRecord> {
        
        private OAIRecord nextRecord;
        private boolean hasNextComputed = false;
        private boolean iteratorExhausted = false;
        
        @Override
        public boolean hasNext() {
            if (iteratorExhausted) {
                return false;
            }
            
            if (!hasNextComputed) {
                try {
                    nextRecord = readNextRecord();
                    hasNextComputed = true;
                    if (nextRecord == null) {
                        iteratorExhausted = true;
                    }
                } catch (IOException e) {
                    logger.error("Error reading next record in iterator", e);
                    iteratorExhausted = true;
                    nextRecord = null;
                }
            }
            
            return nextRecord != null;
        }
        
        @Override
        public OAIRecord next() {
            if (!hasNext()) {
                throw new NoSuchElementException("No more records available");
            }
            
            OAIRecord current = nextRecord;
            nextRecord = null;
            hasNextComputed = false;
            
            return current;
        }
    }
    
    /**
     * Lee siguiente record (usado internamente por iterator).
     * Automáticamente lee de todos los archivos batch.
     * 
     * @return record o null si EOF
     * @throws IOException si falla
     */
    private OAIRecord readNextRecord() throws IOException {
        while (true) {
            // Si no hay batch cargado o se agotó el actual, cargar siguiente
            if (currentBatchRecords == null || currentRecordIndex >= currentBatchRecords.size()) {
                if (!loadNextBatch()) {
                    return null;  // No hay más archivos
                }
            }
            
            // Retornar siguiente record del batch actual
            if (currentRecordIndex < currentBatchRecords.size()) {
                recordsRead++;
                return currentBatchRecords.get(currentRecordIndex++);
            }
        }
    }
    
    /**
     * Convierte Group de Parquet a OAIRecord
     */
    private OAIRecord convertGroupToRecord(Group group) {
        OAIRecord record = new OAIRecord();
        
        // Campos requeridos
        record.setId(group.getString("id", 0));
        record.setIdentifier(group.getString("identifier", 0));
        
        // Convertir timestamp a LocalDateTime
        long timestampMillis = group.getLong("datestamp", 0);
        record.setDatestamp(new Timestamp(timestampMillis).toLocalDateTime());
        
        record.setOriginalMetadataHash(group.getString("original_metadata_hash", 0));
        record.setDeleted(group.getBoolean("deleted", 0));
        
        return record;
    }
    
    // ============================================================================
    // UTILITIES
    // ============================================================================
    
    /**
     * Retorna número total de records escritos (todas las batches)
     */
    public long getTotalRecordsWritten() {
        return totalRecordsWritten;
    }
    
    /**
     * Retorna número de archivos batch creados
     */
    public int getBatchCount() {
        return batchNumber;
    }
    
    /**
     * Retorna número de records leídos (de todos los archivos batch)
     */
    public long getRecordsRead() {
        return recordsRead;
    }
    
    @Override
    public void close() throws IOException {
        // Cerrar writer si está activo (flush final de datos pendientes)
        if (currentWriter != null) {
            if (recordsInCurrentBatch > 0) {
                logger.info("OAI RECORD MANAGER: Final flush - closing batch #{} with {} records", 
                           batchNumber, recordsInCurrentBatch);
                currentWriter.close();
            } else {
                // Writer existe pero no tiene datos - solo cerrarlo
                currentWriter.close();
            }
            currentWriter = null;
            logger.info("OAI RECORD MANAGER: Closed writer. Total: {} records in {} batch files", 
                        totalRecordsWritten, batchNumber);
        }
        
        // Liberar batch en memoria (no hay reader persistente que cerrar)
        currentBatchRecords = null;
        currentRecordIndex = 0;
        
        if (batchFiles != null && !batchFiles.isEmpty()) {
            logger.info("OAI RECORD MANAGER: Closed (NO CACHE). Total records read: {} from {} batch files", 
                        recordsRead, batchFiles.size());
        }
    }
}
