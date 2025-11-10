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
import org.lareferencia.backend.domain.parquet.RecordValidation;
import org.lareferencia.backend.domain.parquet.RuleFact;
import org.lareferencia.backend.domain.parquet.ValidationIndex;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

/**
 * RECORDS MANAGER: Gestiona lectura y escritura de records en archivos Parquet.
 * 
 * FUNCIONALIDADES:
 * - ESCRITURA: Buffer interno inteligente con auto-flush
 * - LECTURA: Streaming sobre múltiples archivos batch con iterator lazy
 * - SCHEMA: Unificado con RuleFacts anidados
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
 * - Archivos múltiples: Cada flush crea un archivo batch_XXXXX.parquet
 * 
 * ESTRATEGIA MULTI-ARCHIVO (LECTURA):
 * - Lee TODOS los archivos records_batch_*.parquet de un snapshot
 * - Procesa batches en orden numérico (batch_1, batch_2, ...)
 * - Transparente: El caller no sabe que hay múltiples archivos
 * - Streaming: Lee un archivo a la vez (no carga todo en memoria)
 * - Iterator: Soporta iteración lazy sin cargar todo el dataset
 * 
 * EJEMPLOS DE USO:
 * 
 * 1. ESCRITURA:
 * <pre>
 * try (ValidationRecordManager writer = ValidationRecordManager.forWriting(basePath, snapshotId, conf)) {
 *     for (RecordValidation record : records) {
 *         writer.writeRecord(record);
 *     }
 *     writer.flush(); // Garantizar persistencia
 * }
 * </pre>
 * 
 * 2. LECTURA LAZY (RECOMENDADO para datasets grandes):
 * <pre>
 * try (ValidationRecordManager reader = ValidationRecordManager.forReading(basePath, snapshotId, conf)) {
 *     for (RecordValidation record : reader) {
 *         // Procesa record sin cargar todo en memoria
 *         processRecord(record);
 *     }
 * }
 * </pre>
 * 
 * 3. LECTURA CON CONSUMER:
 * <pre>
 * try (ValidationRecordManager reader = ValidationRecordManager.forReading(basePath, snapshotId, conf)) {
 *     reader.processRecords(record -> {
 *         // Lógica de procesamiento
 *         updateStatistics(record);
 *     });
 * }
 * </pre>
 * 
 * 4. CONTADOR SIN CARGAR EN MEMORIA:
 * <pre>
 * try (ValidationRecordManager reader = ValidationRecordManager.forReading(basePath, snapshotId, conf)) {
 *     long totalRecords = reader.countRecords();
 *     System.out.println("Total records: " + totalRecords);
 * }
 * </pre>
 */
public final class ValidationRecordManager implements AutoCloseable, Iterable<RecordValidation> {
    
    private static final Logger logger = LogManager.getLogger(ValidationRecordManager.class);
    
    // Umbrales para flush automático (escritura)
    private static final int FLUSH_THRESHOLD_RECORDS = 10000;  // Flush cada 10k records
    
    /**
     * ESQUEMA CON RULE FACTS ANIDADOS Y CAMPOS NUEVOS:
     * - Campos básicos del record (id, identifier, record_id, record_is_valid, is_transformed)
     * - published_metadata_hash: Hash del XML a indexar
     * - Lista de rule_facts (opcional) con estructura anidada
     */
    private static final MessageType SCHEMA = Types.buildMessage()
        .required(PrimitiveType.PrimitiveTypeName.BINARY)
            .as(LogicalTypeAnnotation.stringType())
            .named("id")
        .required(PrimitiveType.PrimitiveTypeName.BINARY)
            .as(LogicalTypeAnnotation.stringType())
            .named("identifier")
        .required(PrimitiveType.PrimitiveTypeName.BINARY)
            .as(LogicalTypeAnnotation.stringType())
            .named("record_id")
        .required(PrimitiveType.PrimitiveTypeName.BOOLEAN)
            .named("record_is_valid")
        .required(PrimitiveType.PrimitiveTypeName.BOOLEAN)
            .named("is_transformed")
        .optional(PrimitiveType.PrimitiveTypeName.BINARY)
            .as(LogicalTypeAnnotation.stringType())
            .named("published_metadata_hash")
        // Lista de rule facts (opcional) - cada fact es un grupo con sus campos
        .optionalGroup()
            .repeatedGroup()
                .required(PrimitiveType.PrimitiveTypeName.INT32)
                    .named("rule_id")
                .optionalList()
                    .optionalElement(PrimitiveType.PrimitiveTypeName.BINARY)
                    .as(LogicalTypeAnnotation.stringType())
                    .named("valid_occurrences")
                .optionalList()
                    .optionalElement(PrimitiveType.PrimitiveTypeName.BINARY)
                    .as(LogicalTypeAnnotation.stringType())
                    .named("invalid_occurrences")
                .required(PrimitiveType.PrimitiveTypeName.BOOLEAN)
                    .named("is_valid")
                .named("fact")
            .named("rule_facts_list")
        .named("RecordValidation");
    
    /**
     * SCHEMA LIGHTWEIGHT INDEX: Solo campos esenciales para carga en memoria.
     * 
     * Este schema se usa para persistir ValidationIndex en paralelo.
     * Archivo único: validation_index.parquet (no batches)
     * 
     * Campos (solo 5 esenciales):
     * - record_id: Hash MD5 que referencia a OAIRecord
     * - identifier: Identificador OAI (denormalizado)
     * - record_is_valid: Boolean validación
     * - is_transformed: Boolean transformación
     * - published_metadata_hash: Hash XML a indexar (opcional)
     */
    private static final MessageType INDEX_SCHEMA = Types.buildMessage()
        .required(PrimitiveType.PrimitiveTypeName.BINARY)
            .as(LogicalTypeAnnotation.stringType())
            .named("record_id")
        .required(PrimitiveType.PrimitiveTypeName.BINARY)
            .as(LogicalTypeAnnotation.stringType())
            .named("identifier")
        .required(PrimitiveType.PrimitiveTypeName.BOOLEAN)
            .named("record_is_valid")
        .required(PrimitiveType.PrimitiveTypeName.BOOLEAN)
            .named("is_transformed")
        .optional(PrimitiveType.PrimitiveTypeName.BINARY)
            .as(LogicalTypeAnnotation.stringType())
            .named("published_metadata_hash")
        .named("ValidationIndex");
    
    private final String basePath;
    private final Long snapshotId;
    private final Configuration hadoopConf;
    
    // Estado de ESCRITURA
    private ParquetWriter<Group> currentWriter;
    private long recordsInCurrentBatch = 0;
    private long totalRecordsWritten = 0;
    private int batchNumber = 0;
    
    // Buffer para escritura del índice ligero en paralelo
    private List<ValidationIndex> indexBuffer;
    
    // Estado de LECTURA (SIN CACHE - readers se crean/cierran on-demand)
    private List<Path> batchFiles;
    private int currentBatchIndex = 0;
    private long recordsRead = 0;
    
    // Cache temporal para iteración (solo durante un ciclo de iteración)
    private List<RecordValidation> currentBatchRecords = null;
    private int currentRecordIndex = 0;
    
    private ValidationRecordManager(String basePath, Long snapshotId, Configuration hadoopConf) {
        this.basePath = basePath;
        this.snapshotId = snapshotId;
        this.hadoopConf = hadoopConf;
        this.currentWriter = null;
        this.batchFiles = null;
        this.currentBatchRecords = null;
        this.indexBuffer = new ArrayList<>();
    }
    
    // ============================================================================
    // FACTORY METHODS
    // ============================================================================
    
    /**
     * Crea un manager para ESCRITURA con buffer interno.
     * Gestiona automáticamente múltiples archivos batch según performance.
     * 
     * @param basePath ruta base (ej: /data/validation-stats)
     * @param snapshotId ID del snapshot
     * @param hadoopConf configuración Hadoop
     * @return manager listo para escritura
     * @throws IOException si falla
     */
    public static ValidationRecordManager forWriting(String basePath, Long snapshotId, Configuration hadoopConf) 
            throws IOException {
        logger.info("RECORDS MANAGER: Creating writer for snapshot {}", snapshotId);
        return new ValidationRecordManager(basePath, snapshotId, hadoopConf);
    }
    
    /**
     * Crea un manager para LECTURA que lee TODOS los archivos batch de un snapshot.
     * Busca automáticamente todos los records_batch_*.parquet
     * 
     * OPCIONES DE LECTURA:
     * - readNext(): Lee record por record (streaming manual)
     * - readAll(): Carga todos los records en memoria (solo para datasets pequeños)
     * - iterator(): Iteración lazy (RECOMENDADO para datasets grandes)
     * - processRecords(): Procesa con Consumer (streaming funcional)
     * 
     * @param basePath ruta base (ej: /data/validation-stats)
     * @param snapshotId ID del snapshot
     * @param hadoopConf configuración Hadoop
     * @return manager listo para lectura
     * @throws IOException si falla
     */
    public static ValidationRecordManager forReading(String basePath, Long snapshotId, Configuration hadoopConf) 
            throws IOException {
        ValidationRecordManager manager = new ValidationRecordManager(basePath, snapshotId, hadoopConf);
        manager.initializeReader();
        return manager;
    }
    
    /**
     * Método de conveniencia para iterar sobre records de forma lazy.
     * Equivalente a: ValidationRecordManager.forReading(...).iterator()
     * 
     * Ejemplo de uso:
     * <pre>
     * for (RecordValidation record : ValidationRecordManager.iterate(basePath, snapshotId, hadoopConf)) {
     *     // Procesar record sin cargar todo en memoria
     *     processRecord(record);
     * }
     * </pre>
     * 
     * @param basePath ruta base
     * @param snapshotId ID del snapshot
     * @param hadoopConf configuración Hadoop
     * @return iterable lazy sobre todos los records
     * @throws IOException si falla
     */
    public static Iterable<RecordValidation> iterate(String basePath, Long snapshotId, Configuration hadoopConf) 
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
        String batchPath = basePath + "/snapshot_" + snapshotId + "/records_batch_" + batchNumber + ".parquet";
        Path path = new Path(batchPath);
        
        logger.info("RECORDS MANAGER: Creating batch file #{} at {}", batchNumber, batchPath);
        
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
     * THREAD-SAFE: synchronized para acceso concurrente seguro
     * 
     * @param record datos del record
     * @throws IOException si falla
     */
    public synchronized void writeRecord(RecordValidation record) throws IOException {
        if (record == null) {
            logger.warn("RECORDS MANAGER: Null record, skipping");
            return;
        }
        
        // Validar campos requeridos
        if (record.getId() == null || record.getIdentifier() == null || 
            record.getRecordIsValid() == null || record.getIsTransformed() == null ||
            record.getRecordId() == null) {
            logger.error("RECORDS MANAGER: Invalid record (missing required fields), skipping");
            return;
        }
        
        // Crear primer writer o verificar si necesita flush automático
        if (currentWriter == null) {
            createNewBatchWriter();
        } else if (recordsInCurrentBatch >= FLUSH_THRESHOLD_RECORDS) {
            logger.info("RECORDS MANAGER: Auto-flush triggered at {} records", recordsInCurrentBatch);
            flush();
            // Crear nuevo writer inmediatamente después del auto-flush
            createNewBatchWriter();
        }
        
        Group group = new SimpleGroup(SCHEMA);
        
        // Campos requeridos
        group.append("id", record.getId().trim());
        group.append("identifier", record.getIdentifier().trim());
        group.append("record_id", record.getRecordId().trim());
        group.append("record_is_valid", record.getRecordIsValid());
        group.append("is_transformed", record.getIsTransformed());
        
        // published_metadata_hash (opcional)
        if (record.getPublishedMetadataHash() != null) {
            group.append("published_metadata_hash", record.getPublishedMetadataHash().trim());
        }
        
        // Agregar rule facts si existen
        List<RuleFact> ruleFacts = record.getRuleFacts();
        if (ruleFacts != null && !ruleFacts.isEmpty()) {
            Group ruleFactsGroup = group.addGroup("rule_facts_list");
            
            for (RuleFact fact : ruleFacts) {
                if (fact == null || fact.getRuleId() == null || fact.getIsValid() == null) {
                    continue; // Skip invalid facts
                }
                
                Group factGroup = ruleFactsGroup.addGroup("fact");
                factGroup.append("rule_id", fact.getRuleId());
                
                // Agregar listas de occurrences con estructura LIST estándar de Parquet
                if (fact.getValidOccurrences() != null && !fact.getValidOccurrences().isEmpty()) {
                    Group validOccListWrapper = factGroup.addGroup("valid_occurrences");
                    for (String occurrence : fact.getValidOccurrences()) {
                        if (occurrence != null && !occurrence.isEmpty()) {
                            Group listItem = validOccListWrapper.addGroup("list");
                            listItem.append("element", occurrence);
                        }
                    }
                }
                
                if (fact.getInvalidOccurrences() != null && !fact.getInvalidOccurrences().isEmpty()) {
                    Group invalidOccListWrapper = factGroup.addGroup("invalid_occurrences");
                    for (String occurrence : fact.getInvalidOccurrences()) {
                        if (occurrence != null && !occurrence.isEmpty()) {
                            Group listItem = invalidOccListWrapper.addGroup("list");
                            listItem.append("element", occurrence);
                        }
                    }
                }
                
                factGroup.append("is_valid", fact.getIsValid());
            }
        }
        
        currentWriter.write(group);
        recordsInCurrentBatch++;
        totalRecordsWritten++;
        
        // Agregar al buffer del índice ligero en paralelo
        indexBuffer.add(ValidationIndex.fromRecordValidation(record));
        
        if (totalRecordsWritten % 10000 == 0) {
            logger.debug("RECORDS MANAGER: Written {} records total ({} in current batch)", 
                        totalRecordsWritten, recordsInCurrentBatch);
        }
    }
    
    /**
     * Fuerza escritura del batch actual a disco y crea nuevo writer.
     * IMPORTANTE: Debe llamarse desde el Service antes de confirmar transacción.
     * 
     * THREAD-SAFE: synchronized para acceso concurrente seguro
     * 
     * @throws IOException si falla
     */
    public synchronized void flush() throws IOException {
        if (currentWriter != null && recordsInCurrentBatch > 0) {
            logger.info("RECORDS MANAGER: Flushing batch #{} with {} records", batchNumber, recordsInCurrentBatch);
            currentWriter.close();
            currentWriter = null;
            recordsInCurrentBatch = 0;
            // NO crear nuevo writer aquí - se creará en el próximo writeRecord() si es necesario
        }
        
        // Escribir índice ligero si hay datos acumulados
        if (!indexBuffer.isEmpty()) {
            writeIndexFile();
        }
    }
    
    /**
     * Escribe el archivo del índice ligero.
     * Se sobrescribe completamente en cada flush (archivo único).
     * 
     * Ruta: /snapshot_{id}/validation_index.parquet
     */
    private void writeIndexFile() throws IOException {
        String indexPath = basePath + "/snapshot_" + snapshotId + "/validation_index.parquet";
        Path path = new Path(indexPath);
        
        logger.info("RECORDS MANAGER: Writing lightweight index with {} records to {}", 
                   indexBuffer.size(), indexPath);
        
        Configuration conf = new Configuration(hadoopConf);
        org.apache.parquet.hadoop.example.GroupWriteSupport.setSchema(INDEX_SCHEMA, conf);
        
        try (ParquetWriter<Group> indexWriter = ExampleParquetWriter.builder(path)
                .withConf(conf)
                .withWriteMode(org.apache.parquet.hadoop.ParquetFileWriter.Mode.OVERWRITE)
                .withCompressionCodec(CompressionCodecName.SNAPPY)
                .withDictionaryEncoding(true)
                .withPageSize(1 << 20)
                .withRowGroupSize(128L << 20)
                .build()) {
            
            for (ValidationIndex indexRecord : indexBuffer) {
                Group group = new SimpleGroup(INDEX_SCHEMA);
                
                group.append("record_id", indexRecord.getRecordId().trim());
                group.append("identifier", indexRecord.getIdentifier().trim());
                group.append("record_is_valid", indexRecord.getRecordIsValid());
                group.append("is_transformed", indexRecord.getIsTransformed());
                
                if (indexRecord.getPublishedMetadataHash() != null) {
                    group.append("published_metadata_hash", indexRecord.getPublishedMetadataHash().trim());
                }
                
                indexWriter.write(group);
            }
        }
        
        logger.info("RECORDS MANAGER: Index file written successfully ({} records)", indexBuffer.size());
    }
    
    // ============================================================================
    // LECTURA - READ OPERATIONS
    // ============================================================================
    
    /**
     * Inicializa el lector buscando todos los archivos batch del snapshot.
     */
    private void initializeReader() throws IOException {
        String snapshotDir = basePath + "/snapshot_" + snapshotId;
        Path snapshotPath = new Path(snapshotDir);
        
        FileSystem fs = FileSystem.get(hadoopConf);
        
        // Buscar todos los archivos records_batch_*.parquet
        PathFilter batchFilter = path -> path.getName().startsWith("records_batch_") 
                                       && path.getName().endsWith(".parquet");
        
        FileStatus[] batchStatuses = fs.listStatus(snapshotPath, batchFilter);
        
        if (batchStatuses == null || batchStatuses.length == 0) {
            logger.warn("RECORDS MANAGER: No batch files found for snapshot {}", snapshotId);
            batchFiles = new ArrayList<>();
            return;
        }
        
        // Ordenar por nombre (batch_1, batch_2, batch_3, ...)
        Arrays.sort(batchStatuses, Comparator.comparing(fs1 -> fs1.getPath().getName()));
        
        batchFiles = new ArrayList<>();
        for (FileStatus status : batchStatuses) {
            batchFiles.add(status.getPath());
        }
        
        logger.info("RECORDS MANAGER: Found {} batch files for snapshot {}", batchFiles.size(), snapshotId);
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
        logger.debug("RECORDS MANAGER: Loading batch file {}/{}: {} (NO CACHE)", 
                    currentBatchIndex + 1, batchFiles.size(), batchPath.getName());
        
        // Leer TODO el batch de una vez (sin cachear reader)
        List<RecordValidation> batchRecords = new ArrayList<>();
        try (ParquetReader<Group> reader = ParquetReader.builder(new GroupReadSupport(), batchPath)
                .withConf(hadoopConf)
                .build()) {
            
            Group group;
            while ((group = reader.read()) != null) {
                batchRecords.add(convertGroupToRecord(group));
            }
        }
        
        logger.debug("RECORDS MANAGER: Loaded {} records from batch {} (reader closed)", 
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
    public RecordValidation readNext() throws IOException {
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
    public List<RecordValidation> readAll() throws IOException {
        List<RecordValidation> records = new ArrayList<>();
        RecordValidation record;
        
        while ((record = readNext()) != null) {
            records.add(record);
        }
        
        logger.info("RECORDS MANAGER: Read {} records total", recordsRead);
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
    public Iterator<RecordValidation> iterator() {
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
        
        logger.debug("RECORDS MANAGER: Reset completed for snapshot {} (NO CACHE)", snapshotId);
    }
    
    /**
     * Iterador interno que lee records de forma lazy desde archivos Parquet.
     * Maneja automáticamente múltiples archivos batch de forma transparente.
     */
    private class RecordIterator implements Iterator<RecordValidation> {
        
        private RecordValidation nextRecord;
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
        public RecordValidation next() {
            if (!hasNext()) {
                throw new NoSuchElementException("No more records available");
            }
            
            RecordValidation current = nextRecord;
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
        for (@SuppressWarnings("unused") RecordValidation record : this) {
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
    public void processRecords(java.util.function.Consumer<RecordValidation> processor) {
        for (RecordValidation record : this) {
            processor.accept(record);
        }
    }
    
    /**
     * Convierte Group de Parquet a RecordValidation con RuleFacts anidados
     */
    private RecordValidation convertGroupToRecord(Group group) {
        RecordValidation record = new RecordValidation();
        
        // Campos requeridos
        record.setId(group.getString("id", 0));
        record.setIdentifier(group.getString("identifier", 0));
        record.setRecordId(group.getString("record_id", 0));
        record.setRecordIsValid(group.getBoolean("record_is_valid", 0));
        record.setIsTransformed(group.getBoolean("is_transformed", 0));
        
        // published_metadata_hash (opcional)
        try {
            int publishedHashIndex = group.getType().getFieldIndex("published_metadata_hash");
            if (publishedHashIndex >= 0 && group.getFieldRepetitionCount(publishedHashIndex) > 0) {
                record.setPublishedMetadataHash(group.getString("published_metadata_hash", 0));
            }
        } catch (Exception e) {
            // Campo opcional, ignorar si no existe
        }
        
        // Leer rule facts si existen
        try {
            int fieldIndex = group.getType().getFieldIndex("rule_facts_list");
            if (fieldIndex >= 0 && group.getFieldRepetitionCount(fieldIndex) > 0) {
                Group ruleFactsGroup = group.getGroup("rule_facts_list", 0);
                int factCount = ruleFactsGroup.getFieldRepetitionCount("fact");
                
                for (int i = 0; i < factCount; i++) {
                    Group factGroup = ruleFactsGroup.getGroup("fact", i);
                    
                    RuleFact fact = new RuleFact();
                    fact.setRuleId(factGroup.getInteger("rule_id", 0));
                    fact.setIsValid(factGroup.getBoolean("is_valid", 0));
                    
                    // Leer valid occurrences como lista (con grupo 'list' intermedio)
                    int validOccIndex = factGroup.getType().getFieldIndex("valid_occurrences");
                    if (validOccIndex >= 0 && factGroup.getFieldRepetitionCount(validOccIndex) > 0) {
                        Group validOccWrapper = factGroup.getGroup("valid_occurrences", 0);
                        int listCount = validOccWrapper.getFieldRepetitionCount("list");
                        List<String> validOccurrences = new ArrayList<>();
                        for (int j = 0; j < listCount; j++) {
                            Group listItem = validOccWrapper.getGroup("list", j);
                            validOccurrences.add(listItem.getString("element", 0));
                        }
                        fact.setValidOccurrences(validOccurrences);
                    }
                    
                    // Leer invalid occurrences como lista (con grupo 'list' intermedio)
                    int invalidOccIndex = factGroup.getType().getFieldIndex("invalid_occurrences");
                    if (invalidOccIndex >= 0 && factGroup.getFieldRepetitionCount(invalidOccIndex) > 0) {
                        Group invalidOccWrapper = factGroup.getGroup("invalid_occurrences", 0);
                        int listCount = invalidOccWrapper.getFieldRepetitionCount("list");
                        List<String> invalidOccurrences = new ArrayList<>();
                        for (int j = 0; j < listCount; j++) {
                            Group listItem = invalidOccWrapper.getGroup("list", j);
                            invalidOccurrences.add(listItem.getString("element", 0));
                        }
                        fact.setInvalidOccurrences(invalidOccurrences);
                    }
                    
                    record.addRuleFact(fact);
                }
            }
        } catch (Exception e) {
            // Si hay error leyendo rule facts, solo log y continuar con el record
            logger.warn("Error reading rule facts for record {}: {}", record.getId(), e.getMessage());
        }
        
        return record;
    }
    
    // ============================================================================
    // LIGHTWEIGHT INDEX - CARGA ON-DEMAND
    // ============================================================================
    
    /**
     * Carga el índice ligero completo en memoria (ON-DEMAND).
     * 
     * USO EN WORKERS:
     * - Solo se carga cuando se necesita (no hay caché interno)
     * - Retorna lista completa para queries rápidas en memoria
     * - Ideal para filtrados, búsquedas, estadísticas
     * 
     * TAMAÑO:
     * - ~35 bytes/record comprimido
     * - 10M records = ~350 MB en memoria
     * - Viable para carga completa en workers
     * 
     * Ejemplo de uso:
     * <pre>
     * // Cargar índice cuando se necesita
     * List<ValidationIndex> index = manager.loadLightweightIndex();
     * 
     * // Queries rápidas
     * long validCount = index.stream()
     *     .filter(ValidationIndex::getRecordIsValid)
     *     .count();
     * 
     * // Búsqueda por identifier
     * ValidationIndex record = index.stream()
     *     .filter(r -> r.getIdentifier().equals(targetId))
     *     .findFirst()
     *     .orElse(null);
     * </pre>
     * 
     * @return lista completa de ValidationIndex
     * @throws IOException si falla la lectura
     */
    public List<ValidationIndex> loadLightweightIndex() throws IOException {
        String indexPath = basePath + "/snapshot_" + snapshotId + "/validation_index.parquet";
        Path path = new Path(indexPath);
        
        logger.info("RECORDS MANAGER: Loading lightweight index from {}", indexPath);
        
        List<ValidationIndex> indexRecords = new ArrayList<>();
        
        Configuration conf = new Configuration(hadoopConf);
        
        try (ParquetReader<Group> reader = ParquetReader.builder(new GroupReadSupport(), path)
                .withConf(conf)
                .build()) {
            
            Group group;
            while ((group = reader.read()) != null) {
                ValidationIndex indexRecord = new ValidationIndex();
                
                indexRecord.setRecordId(group.getString("record_id", 0));
                indexRecord.setIdentifier(group.getString("identifier", 0));
                indexRecord.setRecordIsValid(group.getBoolean("record_is_valid", 0));
                indexRecord.setIsTransformed(group.getBoolean("is_transformed", 0));
                
                // published_metadata_hash (opcional)
                try {
                    int hashIndex = group.getType().getFieldIndex("published_metadata_hash");
                    if (hashIndex >= 0 && group.getFieldRepetitionCount(hashIndex) > 0) {
                        indexRecord.setPublishedMetadataHash(group.getString("published_metadata_hash", 0));
                    }
                } catch (Exception e) {
                    // Campo opcional, ignorar si no existe
                }
                
                indexRecords.add(indexRecord);
            }
        }
        
        logger.info("RECORDS MANAGER: Loaded {} index records", indexRecords.size());
        return indexRecords;
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
                logger.info("RECORDS MANAGER: Final flush - closing batch #{} with {} records", 
                           batchNumber, recordsInCurrentBatch);
                currentWriter.close();
            } else {
                // Writer existe pero no tiene datos - solo cerrarlo
                currentWriter.close();
            }
            currentWriter = null;
            logger.info("RECORDS MANAGER: Closed writer. Total: {} records in {} batch files", 
                        totalRecordsWritten, batchNumber);
        }
        
        // Escribir índice final si hay datos pendientes
        if (!indexBuffer.isEmpty()) {
            logger.info("RECORDS MANAGER: Writing final index with {} records", indexBuffer.size());
            writeIndexFile();
            indexBuffer.clear();
        }
        
        // Liberar batch en memoria (no hay reader persistente que cerrar)
        currentBatchRecords = null;
        currentRecordIndex = 0;
        
        if (batchFiles != null && !batchFiles.isEmpty()) {
            logger.info("RECORDS MANAGER: Closed (NO CACHE). Total records read: {} from {} batch files", 
                        recordsRead, batchFiles.size());
        }
    }
}
