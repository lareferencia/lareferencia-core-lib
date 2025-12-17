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
import org.lareferencia.core.metadata.RecordStatus;
import org.lareferencia.core.metadata.SnapshotMetadata;
import org.lareferencia.core.util.PathUtils;

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
 * ESTRUCTURA DE ARCHIVOS (NUEVA):
 * {basePath}/{NETWORK}/snapshots/snapshot_{id}/validation/records_batch_*.parquet
 * {basePath}/{NETWORK}/snapshots/snapshot_{id}/validation/validation_index.parquet
 * {basePath}/{NETWORK}/snapshots/snapshot_{id}/validation/validation-stats.json
 * 
 * THREAD SAFETY:
 * - ParquetWriter/Reader NO son thread-safe
 * - Usar synchronized en operaciones de escritura
 * - Un manager por snapshot (no compartir entre snapshots)
 * 
 * ESTRATEGIA DE BATCHING (ESCRITURA):
 * - Buffer interno: Acumula registros en memoria
 * - Flush automático: Cuando alcanza umbral (configurable mediante `parquet.validation.records-per-file`, por defecto 100000)
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
    
    // Constantes importadas de ParquetConstants para mantener compatibilidad interna
    private static final String VALIDATION_SUBDIR = ParquetConstants.VALIDATION_SUBDIR;
    private static final String BATCH_FILE_PREFIX = ParquetConstants.VALIDATION_BATCH_PREFIX;
    private static final String BATCH_FILE_SUFFIX = ParquetConstants.PARQUET_SUFFIX;
    private static final String INDEX_FILE_NAME = ParquetConstants.VALIDATION_INDEX_FILE;
    
    // Umbrales para flush automático (escritura)
    private static final int DEFAULT_FLUSH_THRESHOLD_RECORDS = ParquetConstants.DEFAULT_FLUSH_THRESHOLD;
    
    /**
     * ESQUEMA CON RULE FACTS ANIDADOS:
     * - Campos básicos del record (identifier, record_id, datestamp, record_is_valid, is_transformed)
     * - published_metadata_hash: Hash del XML a indexar
     * - Lista de rule_facts (opcional) con estructura anidada
     * 
     * NOTA: No se persiste campo "id" separado - recordId es la PK única
     */
    private static final MessageType SCHEMA = Types.buildMessage()
        .required(PrimitiveType.PrimitiveTypeName.BINARY)
            .as(LogicalTypeAnnotation.stringType())
            .named("identifier")
        .required(PrimitiveType.PrimitiveTypeName.INT64)
            .as(LogicalTypeAnnotation.timestampType(true, LogicalTypeAnnotation.TimeUnit.MILLIS))
            .named("datestamp")
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
     * SCHEMA LIGHTWEIGHT INDEX: Proyección ligera de RecordValidation para índice en memoria.
     * 
     * Este schema se usa para persistir índice ligero en paralelo.
     * Archivo único: validation_index.parquet (no batches)
     * 
     * Campos (solo 6 esenciales - sin rule_facts):
     * - record_id: Hash MD5 que referencia a OAIRecord (PK)
     * - identifier: Identificador OAI (denormalizado)
     * - datestamp: Fecha de última modificación (denormalizado)
     * - record_is_valid: Boolean validación
     * - is_transformed: Boolean transformación
     * - published_metadata_hash: Hash XML a indexar (opcional)
     * 
     * NOTA: Este schema es compatible con RecordValidation usando proyección de columnas.
     */
    private static final MessageType INDEX_SCHEMA = Types.buildMessage()
        .required(PrimitiveType.PrimitiveTypeName.BINARY)
            .as(LogicalTypeAnnotation.stringType())
            .named("identifier")
        .required(PrimitiveType.PrimitiveTypeName.INT64)
            .as(LogicalTypeAnnotation.timestampType(true, LogicalTypeAnnotation.TimeUnit.MILLIS))
            .named("datestamp")
        .required(PrimitiveType.PrimitiveTypeName.BOOLEAN)
            .named("record_is_valid")
        .required(PrimitiveType.PrimitiveTypeName.BOOLEAN)
            .named("is_transformed")
        .optional(PrimitiveType.PrimitiveTypeName.BINARY)
            .as(LogicalTypeAnnotation.stringType())
            .named("published_metadata_hash")
        .named("RecordValidation");
    
    private final String basePath;
    private final Long snapshotId;
    private final SnapshotMetadata snapshotMetadata;
    private final Configuration hadoopConf;
    private final int flushThreshold;  // Configurable flush threshold (records per file)

    // Estado de ESCRITURA
    private ParquetWriter<Group> currentWriter;
    private long recordsInCurrentBatch = 0;
    private long totalRecordsWritten = 0;
    private int batchNumber = 0;
    
    // Buffer para escritura del índice ligero en paralelo (RecordValidation sin id/ruleFacts)
    private List<RecordValidation> indexBuffer;
    
    // Estado de LECTURA (SIN CACHE - readers se crean/cierran on-demand)
    private List<Path> batchFiles;
    private int currentBatchIndex = 0;
    
    // Cache temporal para iteración (solo durante un ciclo de iteración)
    private List<RecordValidation> currentBatchRecords = null;
    private int currentRecordIndex = 0;
    
    private ValidationRecordManager(String basePath, SnapshotMetadata snapshotMetadata, Configuration hadoopConf, int flushThreshold) {
        if (snapshotMetadata == null || snapshotMetadata.getSnapshotId() == null) {
            throw new IllegalArgumentException("SnapshotMetadata and snapshotId cannot be null");
        }

        this.basePath = basePath;
        this.snapshotMetadata = snapshotMetadata;
        this.snapshotId = snapshotMetadata.getSnapshotId();
        this.hadoopConf = hadoopConf;
        this.flushThreshold = flushThreshold;
        this.currentWriter = null;
        this.batchFiles = null;
        this.currentBatchRecords = null;
        this.indexBuffer = new ArrayList<>();
    }

    private ValidationRecordManager(String basePath, SnapshotMetadata snapshotMetadata, Configuration hadoopConf) {
        this(basePath, snapshotMetadata, hadoopConf, DEFAULT_FLUSH_THRESHOLD_RECORDS);
    }
    
    // ============================================================================
    // FACTORY METHODS
    // ============================================================================
    
    /**
     * Crea un manager para ESCRITURA con flush threshold personalizado.
     *
     * @param basePath ruta base (ej: /data/validation-stats)
     * @param snapshotMetadata metadata del snapshot (contiene snapshotId y networkAcronym)
     * @param hadoopConf configuración Hadoop
     * @param flushThreshold número de registros antes de hacer flush automático
     * @return manager listo para escritura
     * @throws IOException si falla
     */
    public static ValidationRecordManager forWriting(String basePath, SnapshotMetadata snapshotMetadata, Configuration hadoopConf, int flushThreshold)
            throws IOException {
        logger.debug("RECORDS MANAGER: Creating writer for snapshot {} (network: {}) (flushThreshold={})",
            snapshotMetadata.getSnapshotId(), snapshotMetadata.getNetwork().getAcronym(), flushThreshold);
        return new ValidationRecordManager(basePath, snapshotMetadata, hadoopConf, flushThreshold);
    }

    /**
     * Crea un manager para ESCRITURA con buffer interno (default threshold).
     * Gestiona automáticamente múltiples archivos batch según performance.
     *
     * @param basePath ruta base (ej: /data/validation-stats)
     * @param snapshotMetadata metadata del snapshot (contiene snapshotId y networkAcronym)
     * @param hadoopConf configuración Hadoop
     * @return manager listo para escritura
     * @throws IOException si falla
     */
    public static ValidationRecordManager forWriting(String basePath, SnapshotMetadata snapshotMetadata, Configuration hadoopConf)
            throws IOException {
        return forWriting(basePath, snapshotMetadata, hadoopConf, DEFAULT_FLUSH_THRESHOLD_RECORDS);
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
     * @param snapshotMetadata metadata del snapshot (contiene snapshotId y networkAcronym)
     * @param hadoopConf configuración Hadoop
     * @return manager listo para lectura
     * @throws IOException si falla
     */
    /**
     * Crea un manager para LECTURA que lee TODOS los archivos batch de un snapshot.
     * Busca automáticamente todos los records_batch_*.parquet
     *
     * @param basePath ruta base (ej: /data/validation-stats)
     * @param snapshotMetadata metadata del snapshot (contiene snapshotId y networkAcronym)
     * @param hadoopConf configuración Hadoop
     * @return manager listo para lectura
     * @throws IOException si falla
     */
    public static ValidationRecordManager forReading(String basePath, SnapshotMetadata snapshotMetadata, Configuration hadoopConf)
            throws IOException {
        ValidationRecordManager manager = new ValidationRecordManager(basePath, snapshotMetadata, hadoopConf);
        manager.initializeReader();
        return manager;
    }
    
    /**
     * Método de conveniencia para iterar sobre records de forma lazy (modo completo con ruleFacts).
     * 
     * THREAD-SAFE: Cada llamada crea una NUEVA instancia de manager.
     * Múltiples threads pueden iterar el mismo snapshot sin interferencia.
     * 
     * @param basePath ruta base
     * @param snapshotMetadata metadata del snapshot
     * @param hadoopConf configuración Hadoop
     * @return iterator lazy sobre todos los records (completos, thread-safe)
     * @throws IOException si falla
     */
    public static Iterator<RecordValidation> iterate(String basePath, SnapshotMetadata snapshotMetadata, Configuration hadoopConf) 
            throws IOException {
        // ✅ THREAD-SAFE: Crear NUEVA instancia para cada llamada
        ValidationRecordManager manager = forReading(basePath, snapshotMetadata, hadoopConf);
        return manager.iterator();
    }
    
        /**
     * Método de conveniencia para iterar sobre records de forma lazy (modo ligero sin ruleFacts).
     * 
     * THREAD-SAFE: Cada llamada crea una NUEVA instancia de manager.
     * Múltiples threads pueden iterar el mismo snapshot sin interferencia.
     * 
     * ÍNDICE LIGERO:
     * - RecordValidation sin ruleFacts (solo campos esenciales)
     * - ~35 bytes/record comprimido
     * - Carga completa desde archivo validation_index.parquet
     * 
     * VENTAJAS vs modo completo:
     * - Sin ruleFacts: más rápido y menos memoria
     * - Ideal para queries, filtrados, estadísticas básicas
     * - Usa proyección de columnas Parquet para eficiencia
     * 
     * Ejemplo de uso:
     * <pre>
     * Iterator<RecordValidation> iterator = ValidationRecordManager.iterateLightweight(basePath, snapshotMetadata, hadoopConf);
     * while (iterator.hasNext()) {
     *     RecordValidation record = iterator.next();
     *     // Procesar record sin ruleFacts
     *     processRecord(record);
     * }
     * </pre>
     * 
     * @param basePath ruta base
     * @param snapshotMetadata metadata del snapshot
     * @param hadoopConf configuración Hadoop
     * @return iterator sobre todos los records (ligeros sin ruleFacts, thread-safe)
     * @throws IOException si falla
     */
    public static Iterator<RecordValidation> iterateLightweight(String basePath, SnapshotMetadata snapshotMetadata, RecordStatus status, Configuration hadoopConf) 
            throws IOException {
        // ✅ THREAD-SAFE: Crear NUEVA instancia para cada llamada
        ValidationRecordManager manager = forReading(basePath, snapshotMetadata, hadoopConf);
        
        // Cargar índice ligero completo y convertir a iterator
        List<RecordValidation> lightRecords = manager.loadLightweightIndex(status);
        
        return lightRecords.iterator();
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
        String batchPath = snapshotPath + "/" + VALIDATION_SUBDIR + "/" + BATCH_FILE_PREFIX + batchNumber + BATCH_FILE_SUFFIX;
        Path path = new Path(batchPath);
        
        logger.debug("RECORDS MANAGER: Creating batch file #{} at {}", batchNumber, batchPath);
        
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
        
        // Validar campos requeridos (recordId es la PK única)
        if (record.getIdentifier() == null || 
            record.getRecordIsValid() == null || record.getIsTransformed() == null ||
            record.getRecordId() == null) {
            logger.error("RECORDS MANAGER: Invalid record (missing required fields), skipping");
            return;
        }
        
        // Crear primer writer o verificar si necesita flush automático
        if (currentWriter == null) {
            createNewBatchWriter();
        } else if (flushThreshold > 0 && recordsInCurrentBatch >= flushThreshold) {
            logger.debug("RECORDS MANAGER: Auto-flush triggered at {} records (threshold={})", recordsInCurrentBatch, flushThreshold);
            flush();
            // Crear nuevo writer inmediatamente después del auto-flush
            createNewBatchWriter();
        }
        
        // Crear Group y escribir a Parquet
        Group group = createGroupFromRecord(record);
        currentWriter.write(group);
        recordsInCurrentBatch++;
        totalRecordsWritten++;
        
        // Agregar al buffer del índice ligero (solo campos esenciales, sin ruleFacts)
        indexBuffer.add(createLightweightRecord(record));
        
        if (flushThreshold > 0 && totalRecordsWritten % flushThreshold == 0) {
            logger.debug("RECORDS MANAGER: Written {} records total ({} in current batch)", 
                        totalRecordsWritten, recordsInCurrentBatch);
        }
    }

    /**
     * Crea un Group Parquet a partir de un RecordValidation.
     */
    private Group createGroupFromRecord(RecordValidation record) {
        Group group = new SimpleGroup(SCHEMA);
        
        // Campos requeridos
        group.append("identifier", record.getIdentifier().trim());
        
        // datestamp (opcional pero recomendado)
        if (record.getDatestamp() != null) {
            long epochMillis = record.getDatestamp()
                .atZone(java.time.ZoneId.systemDefault())
                .toInstant()
                .toEpochMilli();
            group.append("datestamp", epochMillis);
        }
        
        group.append("record_is_valid", record.getRecordIsValid());
        group.append("is_transformed", record.getIsTransformed());
        
        // published_metadata_hash (opcional)
        if (record.getPublishedMetadataHash() != null) {
            group.append("published_metadata_hash", record.getPublishedMetadataHash().trim());
        }
        
        // Agregar rule facts si existen
        addRuleFactsToGroup(group, record.getRuleFacts());
        
        return group;
    }

    /**
     * Agrega RuleFacts a un Group Parquet.
     */
    private void addRuleFactsToGroup(Group group, List<RuleFact> ruleFacts) {
        if (ruleFacts == null || ruleFacts.isEmpty()) {
            return;
        }
        
        Group ruleFactsGroup = group.addGroup("rule_facts_list");
        
        for (RuleFact fact : ruleFacts) {
            if (fact == null || fact.getRuleId() == null || fact.getIsValid() == null) {
                continue;
            }
            
            Group factGroup = ruleFactsGroup.addGroup("fact");
            factGroup.append("rule_id", fact.getRuleId());
            
            addOccurrencesToFact(factGroup, "valid_occurrences", fact.getValidOccurrences());
            addOccurrencesToFact(factGroup, "invalid_occurrences", fact.getInvalidOccurrences());
            
            factGroup.append("is_valid", fact.getIsValid());
        }
    }

    /**
     * Agrega lista de occurrences a un fact Group.
     */
    private void addOccurrencesToFact(Group factGroup, String fieldName, List<String> occurrences) {
        if (occurrences == null || occurrences.isEmpty()) {
            return;
        }
        
        Group occListWrapper = factGroup.addGroup(fieldName);
        for (String occurrence : occurrences) {
            if (occurrence != null && !occurrence.isEmpty()) {
                Group listItem = occListWrapper.addGroup("list");
                listItem.append("element", occurrence);
            }
        }
    }

    /**
     * Crea una versión ligera del record (sin ruleFacts) para el índice.
     */
    private RecordValidation createLightweightRecord(RecordValidation record) {
        return new RecordValidation(
            record.getIdentifier(),
            record.getDatestamp(),
            record.getRecordIsValid(),
            record.getIsTransformed(),
            record.getPublishedMetadataHash(),
            null
        );
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
            logger.debug("RECORDS MANAGER: Flushing batch #{} with {} records", batchNumber, recordsInCurrentBatch);
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
     * Escribe el archivo del índice ligero (RecordValidation sin id/ruleFacts).
     * Se sobrescribe completamente en cada flush (archivo único).
     * 
     * Ruta: /{NETWORK}/snapshots/snapshot_{id}/validation/validation_index.parquet
     */
    private void writeIndexFile() throws IOException {
        String snapshotPath = PathUtils.getSnapshotPath(basePath, snapshotMetadata);
        String indexPath = snapshotPath + "/" + VALIDATION_SUBDIR + "/" + INDEX_FILE_NAME;
        Path path = new Path(indexPath);
        
        logger.debug("RECORDS MANAGER: Writing lightweight index with {} records to {}", 
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
            
            for (RecordValidation indexRecord : indexBuffer) {
                Group group = new SimpleGroup(INDEX_SCHEMA);
                
                group.append("identifier", indexRecord.getIdentifier().trim());
                
                // datestamp (opcional)
                if (indexRecord.getDatestamp() != null) {
                    long epochMillis = indexRecord.getDatestamp()
                        .atZone(java.time.ZoneId.systemDefault())
                        .toInstant()
                        .toEpochMilli();
                    group.append("datestamp", epochMillis);
                }
                
                group.append("record_is_valid", indexRecord.getRecordIsValid());
                group.append("is_transformed", indexRecord.getIsTransformed());
                
                if (indexRecord.getPublishedMetadataHash() != null) {
                    group.append("published_metadata_hash", indexRecord.getPublishedMetadataHash().trim());
                }
                
                indexWriter.write(group);
            }
        }
        
        logger.debug("RECORDS MANAGER: Index file written successfully ({} records)", indexBuffer.size());
    }
    
    // ============================================================================
    // LECTURA - READ OPERATIONS
    // ============================================================================
    
    /**
     * Inicializa el lector buscando todos los archivos batch del snapshot.
     */
    private void initializeReader() throws IOException {
        String snapshotPath = PathUtils.getSnapshotPath(basePath, snapshotMetadata);
        String validationDir = snapshotPath + "/" + VALIDATION_SUBDIR;
        Path validationPath = new Path(validationDir);
        
        FileSystem fs = FileSystem.get(hadoopConf);
        
        if (!fs.exists(validationPath)) {
            logger.warn("RECORDS MANAGER: Validation directory does not exist for snapshot {}: {}", snapshotId, validationPath);
            batchFiles = new ArrayList<>();
            return;
        }
        
        // Filter only batch files
        PathFilter batchFilter = path -> path.getName().startsWith(BATCH_FILE_PREFIX) 
                                       && path.getName().endsWith(BATCH_FILE_SUFFIX);
        
        FileStatus[] batchStatuses = fs.listStatus(validationPath, batchFilter);
        
        if (batchStatuses == null || batchStatuses.length == 0) {
            logger.warn("RECORDS MANAGER: No batch files found for snapshot {}", snapshotId);
            batchFiles = new ArrayList<>();
            return;
        }
        
        // Ordenar por nombre
        Arrays.sort(batchStatuses, Comparator.comparing(status -> status.getPath().getName()));
        
        batchFiles = new ArrayList<>();
        for (FileStatus status : batchStatuses) {
            batchFiles.add(status.getPath());
        }
        
        logger.debug("RECORDS MANAGER: Found {} batch files for snapshot {}", batchFiles.size(), snapshotId);
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
     * PRIVADO: Solo usado internamente por el iterator.
     * 
     * @return record o null si EOF (todos los archivos procesados)
     * @throws IOException si falla
     */
    private RecordValidation readNext() throws IOException {
        while (true) {
            // Si no hay batch cargado o se agotó el actual, cargar siguiente
            if (currentBatchRecords == null || currentRecordIndex >= currentBatchRecords.size()) {
                if (!loadNextBatch()) {
                    return null;  // No hay más archivos
                }
            }
            
            // Retornar siguiente record del batch actual
            if (currentRecordIndex < currentBatchRecords.size()) {
                return currentBatchRecords.get(currentRecordIndex++);
            }
        }
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
     * Iterador interno que lee records de forma lazy desde archivos Parquet.
     * Maneja automáticamente múltiples archivos batch de forma transparente.
     */
    private class RecordIterator implements Iterator<RecordValidation> {
        
        private RecordValidation nextRecord;
        private boolean hasNextComputed = false;
        private boolean iteratorExhausted = false;
        private IOException lastError = null;
        
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
                    lastError = e;
                    throw new java.io.UncheckedIOException("Error reading record from Parquet", e);
                }
            }
            
            return nextRecord != null;
        }
        
        @Override
        public RecordValidation next() {
            if (!hasNext()) {
                if (lastError != null) {
                    throw new java.io.UncheckedIOException("Iterator failed due to I/O error", lastError);
                }
                throw new NoSuchElementException("No more records available");
            }
            
            RecordValidation current = nextRecord;
            nextRecord = null;
            hasNextComputed = false;
            
            return current;
        }
    }
    
    /**
     * Convierte Group de Parquet a RecordValidation con RuleFacts anidados
     */
    private RecordValidation convertGroupToRecord(Group group) {
        RecordValidation record = new RecordValidation();
        
        // Campos requeridos (sin id - recordId es la PK)
        record.setIdentifier(group.getString("identifier", 0));
        
        // datestamp (REQUERIDO - tanto en SCHEMA como en INDEX_SCHEMA)
        long epochMillis = group.getLong("datestamp", 0);
        record.setDatestamp(
            java.time.LocalDateTime.ofInstant(
                java.time.Instant.ofEpochMilli(epochMillis),
                java.time.ZoneId.systemDefault()
            )
        );
        
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
            logger.warn("Error reading rule facts for record {}: {}", record.getRecordId(), e.getMessage());
        }
        
        return record;
    }
    
    // ============================================================================
    // LIGHTWEIGHT INDEX - CARGA ON-DEMAND
    // ============================================================================
    
    /**
     * Carga el índice ligero completo en memoria (ON-DEMAND).
     * 
     * ARQUITECTURA:
     * - Retorna RecordValidation con solo campos esenciales (sin id, sin ruleFacts)
     * - Usa proyección de columnas Parquet para eficiencia
     * - No hay caché interno - se carga cuando se necesita
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
     * List<RecordValidation> index = manager.loadLightweightIndex();
     * 
     * // Queries rápidas
     * long validCount = index.stream()
     *     .filter(RecordValidation::getRecordIsValid)
     *     .count();
     * 
     * // Búsqueda por identifier
     * RecordValidation record = index.stream()
     *     .filter(r -> r.getIdentifier().equals(targetId))
     *     .findFirst()
     *     .orElse(null);
     * </pre>
     * 
     * @param status Filtro por estado (VALID, INVALID, UNTESTED=all)
     * @return lista completa de RecordValidation ligeros (sin id, sin ruleFacts)
     * @throws IOException si falla la lectura
     */
    public List<RecordValidation> loadLightweightIndex(RecordStatus status) throws IOException {
        String snapshotPath = PathUtils.getSnapshotPath(basePath, snapshotMetadata);
        String indexPath = snapshotPath + "/" + VALIDATION_SUBDIR + "/" + INDEX_FILE_NAME;
        Path path = new Path(indexPath);
        
        logger.debug("RECORDS MANAGER: Loading lightweight index from {}", indexPath);
        
        List<RecordValidation> indexRecords = new ArrayList<>();
        
        Configuration conf = new Configuration(hadoopConf);
        
        try (ParquetReader<Group> reader = ParquetReader.builder(new GroupReadSupport(), path)
                .withConf(conf)
                .build()) {
            
            Group group;
            while ((group = reader.read()) != null) {
                boolean isValid = group.getBoolean("record_is_valid", 0);

                // Filtrar según RecordStatus solicitado
                // UNTESTED o null = retornar todos (sin filtro)
                // VALID = solo records válidos (isValid == true)
                // INVALID = solo records inválidos (isValid == false)
                // DELETED = ignorado (no aplica para validación)
                if (status != null && status != RecordStatus.UNTESTED) {
                    if (status == RecordStatus.VALID && !isValid) {
                        continue; // Pide VALID pero el record es inválido
                    }
                    if (status == RecordStatus.INVALID && isValid) {
                        continue; // Pide INVALID pero el record es válido
                    }
                }
                
                RecordValidation indexRecord = new RecordValidation();
                
                // Solo campos del índice ligero (sin id, sin ruleFacts)
                indexRecord.setIdentifier(group.getString("identifier", 0));
                indexRecord.setRecordIsValid(group.getBoolean("record_is_valid", 0));
                indexRecord.setIsTransformed(group.getBoolean("is_transformed", 0));
                
                // datestamp (ahora requerido)
                try {
                    long epochMillis = group.getLong("datestamp", 0);
                    indexRecord.setDatestamp(
                        java.time.LocalDateTime.ofInstant(
                            java.time.Instant.ofEpochMilli(epochMillis),
                            java.time.ZoneId.systemDefault()
                        )
                    );
                } catch (Exception e) {
                    logger.warn("Error reading datestamp for record {}: {}", indexRecord.getRecordId(), e.getMessage());
                }
                
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
        
        logger.debug("RECORDS MANAGER: Loaded {} index records", indexRecords.size());
        return indexRecords;
    }
    
    // ============================================================================
    // UTILITIES & CLEANUP
    // ============================================================================
    
    @Override
    public void close() throws IOException {
        // Cerrar writer si está activo (flush final de datos pendientes)
        if (currentWriter != null) {
            if (recordsInCurrentBatch > 0) {
                logger.debug("RECORDS MANAGER: Final flush - closing batch #{} with {} records", 
                           batchNumber, recordsInCurrentBatch);
                currentWriter.close();
            } else {
                // Writer existe pero no tiene datos - solo cerrarlo
                currentWriter.close();
            }
            currentWriter = null;
            logger.debug("RECORDS MANAGER: Closed writer. Total: {} records in {} batch files", 
                        totalRecordsWritten, batchNumber);
        }
        
        // Escribir índice final si hay datos pendientes
        if (!indexBuffer.isEmpty()) {
            logger.debug("RECORDS MANAGER: Writing final index with {} records", indexBuffer.size());
            writeIndexFile();
            indexBuffer.clear();
        }
        
        // Liberar batch en memoria (no hay reader persistente que cerrar)
        currentBatchRecords = null;
        currentRecordIndex = 0;
        
        if (batchFiles != null && !batchFiles.isEmpty()) {
            logger.debug("RECORDS MANAGER: Closed reader for {} batch files (NO CACHE)", batchFiles.size());
        }
    }
    
    /**
     * Deletes only Parquet files from validation directory for this snapshot.
     * Preserves validation_stats.json and other non-Parquet files.
     * 
     * @throws IOException if deletion fails
     */
    public void deleteParquetFiles() throws IOException {
        String snapshotPath = PathUtils.getSnapshotPath(basePath, snapshotMetadata);
        Path validationPath = new Path(snapshotPath + "/" + VALIDATION_SUBDIR);
        FileSystem fs = FileSystem.get(hadoopConf);
        
        if (!fs.exists(validationPath)) {
            logger.debug("RECORDS MANAGER: Validation directory does not exist for snapshot {}: {}", snapshotId, validationPath);
            return;
        }
        
        // Filter only .parquet files
        PathFilter parquetFilter = p -> p.getName().endsWith(".parquet");
        FileStatus[] parquetFiles = fs.listStatus(validationPath, parquetFilter);
        
        int deletedCount = 0;
        for (FileStatus file : parquetFiles) {
            if (fs.delete(file.getPath(), false)) {
                deletedCount++;
                logger.debug("RECORDS MANAGER: Deleted {}", file.getPath().getName());
            } else {
                logger.warn("RECORDS MANAGER: Failed to delete {}", file.getPath().getName());
            }
        }
        
        logger.debug("RECORDS MANAGER: Deleted {} parquet files from validation dir {}. Preserved validation_stats.json and other non-parquet files.", 
                    deletedCount, validationPath);
    }
}
