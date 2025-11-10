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

package org.lareferencia.backend.repositories.parquet;

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
import org.lareferencia.backend.domain.parquet.OAIRecord;

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
 * FUNCIONALIDADES:
 * - ESCRITURA: Buffer interno inteligente con auto-flush
 * - LECTURA: Streaming sobre múltiples archivos batch con iterator lazy
 * - SCHEMA: Simplificado solo con campos del catálogo OAI
 * 
 * THREAD SAFETY:
 * - ParquetWriter/Reader NO son thread-safe
 * - Usar synchronized en operaciones de escritura
 * - Un manager por snapshot (no compartir entre snapshots)
 * 
 * ESTRATEGIA DE BATCHING (ESCRITURA):
 * - Buffer interno: Acumula registros en memoria
 * - Flush automático: Cuando alcanza umbral (10,000 registros)
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
 * ESTRUCTURA DE ARCHIVOS:
 * /data/parquet/snapshot_{id}/catalog/oai_records_batch_1.parquet
 * /data/parquet/snapshot_{id}/catalog/oai_records_batch_2.parquet
 * ...
 * 
 * EJEMPLOS DE USO:
 * 
 * 1. ESCRITURA (Harvesting):
 * <pre>
 * try (OAIRecordManager writer = OAIRecordManager.forWriting(basePath, snapshotId, conf)) {
 *     for (OAIRecord record : harvestedRecords) {
 *         writer.writeRecord(record);
 *     }
 *     writer.flush(); // Garantizar persistencia
 * }
 * </pre>
 * 
 * 2. LECTURA LAZY (RECOMENDADO para datasets grandes):
 * <pre>
 * try (OAIRecordManager reader = OAIRecordManager.forReading(basePath, snapshotId, conf)) {
 *     for (OAIRecord record : reader) {
 *         // Procesa record sin cargar todo en memoria
 *         validateRecord(record);
 *     }
 * }
 * </pre>
 * 
 * 3. LECTURA CON CONSUMER:
 * <pre>
 * try (OAIRecordManager reader = OAIRecordManager.forReading(basePath, snapshotId, conf)) {
 *     reader.processRecords(record -> {
 *         // Lógica de procesamiento
 *         indexRecord(record);
 *     });
 * }
 * </pre>
 * 
 * 4. CONTADOR SIN CARGAR EN MEMORIA:
 * <pre>
 * try (OAIRecordManager reader = OAIRecordManager.forReading(basePath, snapshotId, conf)) {
 *     long totalRecords = reader.countRecords();
 *     System.out.println("Total records in catalog: " + totalRecords);
 * }
 * </pre>
 */
public final class OAIRecordManager implements AutoCloseable, Iterable<OAIRecord> {
    
    private static final Logger logger = LogManager.getLogger(OAIRecordManager.class);
    
    // Umbrales para flush automático (escritura)
    private static final int FLUSH_THRESHOLD_RECORDS = 10000;  // Flush cada 10k records
    
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
    private final Long snapshotId;
    private final Configuration hadoopConf;
    
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
    
    private OAIRecordManager(String basePath, Long snapshotId, Configuration hadoopConf) {
        this.basePath = basePath;
        this.snapshotId = snapshotId;
        this.hadoopConf = hadoopConf;
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
     * @param snapshotId ID del snapshot
     * @param hadoopConf configuración Hadoop
     * @return manager listo para escritura
     * @throws IOException si falla
     */
    public static OAIRecordManager forWriting(String basePath, Long snapshotId, Configuration hadoopConf) 
            throws IOException {
        logger.info("OAI RECORD MANAGER: Creating writer for snapshot {}", snapshotId);
        return new OAIRecordManager(basePath, snapshotId, hadoopConf);
    }
    
    /**
     * Crea un manager para LECTURA que lee TODOS los archivos batch de un snapshot.
     * Busca automáticamente todos los oai_records_batch_*.parquet
     * 
     * OPCIONES DE LECTURA:
     * - readNext(): Lee record por record (streaming manual)
     * - readAll(): Carga todos los records en memoria (solo para datasets pequeños)
     * - iterator(): Iteración lazy (RECOMENDADO para datasets grandes)
     * - processRecords(): Procesa con Consumer (streaming funcional)
     * 
     * @param basePath ruta base (ej: /data/parquet)
     * @param snapshotId ID del snapshot
     * @param hadoopConf configuración Hadoop
     * @return manager listo para lectura
     * @throws IOException si falla
     */
    public static OAIRecordManager forReading(String basePath, Long snapshotId, Configuration hadoopConf) 
            throws IOException {
        OAIRecordManager manager = new OAIRecordManager(basePath, snapshotId, hadoopConf);
        manager.initializeReader();
        return manager;
    }
    
    /**
     * Método de conveniencia para iterar sobre records de forma lazy.
     * Equivalente a: OAIRecordManager.forReading(...).iterator()
     * 
     * Ejemplo de uso:
     * <pre>
     * for (OAIRecord record : OAIRecordManager.iterate(basePath, snapshotId, hadoopConf)) {
     *     // Procesar record sin cargar todo en memoria
     *     validateRecord(record);
     * }
     * </pre>
     * 
     * @param basePath ruta base
     * @param snapshotId ID del snapshot
     * @param hadoopConf configuración Hadoop
     * @return iterable lazy sobre todos los records
     * @throws IOException si falla
     */
    public static Iterable<OAIRecord> iterate(String basePath, Long snapshotId, Configuration hadoopConf) 
            throws IOException {
        return forReading(basePath, snapshotId, hadoopConf);
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
        String batchPath = basePath + "/snapshot_" + snapshotId + "/" + CATALOG_SUBDIR + "/" 
                         + FILE_PREFIX + batchNumber + ".parquet";
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
        } else if (recordsInCurrentBatch >= FLUSH_THRESHOLD_RECORDS) {
            logger.info("OAI RECORD MANAGER: Auto-flush triggered at {} records", recordsInCurrentBatch);
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
        
        if (totalRecordsWritten % 10000 == 0) {
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
        String catalogDir = basePath + "/snapshot_" + snapshotId + "/" + CATALOG_SUBDIR;
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
    
    /**
     * Lee siguiente record (automáticamente de todos los archivos batch).
     * SIN CACHE: cada batch se carga completo, se itera en memoria, luego se libera.
     * 
     * @return record o null si EOF (todos los archivos procesados)
     * @throws IOException si falla
     */
    public OAIRecord readNext() throws IOException {
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
     * Lee todos los records de los archivos batch.
     * NOTA: Para datasets grandes, usar iterator() en su lugar para evitar cargar todo en memoria.
     * 
     * @return lista de records
     * @throws IOException si falla
     */
    public List<OAIRecord> readAll() throws IOException {
        List<OAIRecord> records = new ArrayList<>();
        OAIRecord record;
        
        while ((record = readNext()) != null) {
            records.add(record);
        }
        
        logger.info("OAI RECORD MANAGER: Read {} records total", recordsRead);
        return records;
    }
    
    // ============================================================================
    // ITERADOR - LAZY ITERATION (RECOMENDADO PARA DATASETS GRANDES)
    // ============================================================================
    
    /**
     * Retorna un iterator que lee records de forma lazy (streaming).
     * NO carga todo el dataset en memoria - ideal para datasets grandes.
     * 
     * IMPORTANTE: Solo un iterator activo por vez. Llamar reset() para crear nuevo iterator.
     * 
     * @return iterator lazy sobre todos los records
     */
    @Override
    public Iterator<OAIRecord> iterator() {
        return new RecordIterator();
    }
    
    /**
     * Reinicia el lector para permitir una nueva iteración desde el principio.
     * Útil cuando se necesita iterar múltiples veces sobre el mismo dataset.
     * 
     * @throws IOException si falla al reinicializar
     */
    public void reset() throws IOException {
        // Liberar batch actual en memoria
        currentBatchRecords = null;
        currentRecordIndex = 0;
        
        // Reiniciar contadores y estado
        currentBatchIndex = 0;
        recordsRead = 0;
        
        logger.debug("OAI RECORD MANAGER: Reset completed for snapshot {} (NO CACHE)", snapshotId);
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
                    nextRecord = readNext();
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
     * Cuenta el total de records sin cargarlos en memoria.
     * Itera sobre todos los records pero solo cuenta, no los almacena.
     * 
     * @return número total de records en todos los archivos batch
     */
    public long countRecords() {
        long count = 0;
        for (@SuppressWarnings("unused") OAIRecord record : this) {
            count++;
        }
        return count;
    }
    
    /**
     * Procesa records de forma streaming usando un Consumer.
     * Permite procesar datasets grandes sin cargar todo en memoria.
     * 
     * Ejemplo de uso:
     * <pre>
     * manager.processRecords(record -> {
     *     // Procesar cada record individualmente
     *     System.out.println("Processing: " + record.getId());
     * });
     * </pre>
     * 
     * @param processor función que procesa cada record
     */
    public void processRecords(java.util.function.Consumer<OAIRecord> processor) {
        for (OAIRecord record : this) {
            processor.accept(record);
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
