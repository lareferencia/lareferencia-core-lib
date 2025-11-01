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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

/**
 * RECORDS MANAGER: Gestiona lectura y escritura de records en archivos Parquet.
 * 
 * FUNCIONALIDADES:
 * - ESCRITURA: Buffer interno inteligente con auto-flush
 * - LECTURA: Streaming sobre múltiples archivos batch
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
 */
public final class ValidationRecordManager implements AutoCloseable {
    
    private static final Logger logger = LogManager.getLogger(ValidationRecordManager.class);
    
    // Umbrales para flush automático (escritura)
    private static final int FLUSH_THRESHOLD_RECORDS = 10000;  // Flush cada 10k records
    
    /**
     * ESQUEMA CON RULE FACTS ANIDADOS:
     * - Campos básicos del record (id, identifier, record_is_valid, is_transformed)
     * - Lista de rule_facts (opcional) con estructura anidada
     */
    private static final MessageType SCHEMA = Types.buildMessage()
        .required(PrimitiveType.PrimitiveTypeName.BINARY)
            .as(LogicalTypeAnnotation.stringType())
            .named("id")
        .required(PrimitiveType.PrimitiveTypeName.BINARY)
            .as(LogicalTypeAnnotation.stringType())
            .named("identifier")
        .required(PrimitiveType.PrimitiveTypeName.BOOLEAN)
            .named("record_is_valid")
        .required(PrimitiveType.PrimitiveTypeName.BOOLEAN)
            .named("is_transformed")
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
    
    private final String basePath;
    private final Long snapshotId;
    private final Configuration hadoopConf;
    
    // Estado de ESCRITURA
    private ParquetWriter<Group> currentWriter;
    private long recordsInCurrentBatch = 0;
    private long totalRecordsWritten = 0;
    private int batchNumber = 0;
    
    // Estado de LECTURA
    private List<Path> batchFiles;
    private int currentBatchIndex = 0;
    private ParquetReader<Group> currentReader;
    private long recordsRead = 0;
    
    private ValidationRecordManager(String basePath, Long snapshotId, Configuration hadoopConf) {
        this.basePath = basePath;
        this.snapshotId = snapshotId;
        this.hadoopConf = hadoopConf;
        this.currentWriter = null;
        this.currentReader = null;
        this.batchFiles = null;
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
            record.getRecordIsValid() == null || record.getIsTransformed() == null) {
            logger.error("RECORDS MANAGER: Invalid record (missing required fields), skipping");
            return;
        }
        
        // Crear primer writer o verificar si necesita flush automático
        if (currentWriter == null) {
            createNewBatchWriter();
        } else if (recordsInCurrentBatch >= FLUSH_THRESHOLD_RECORDS) {
            logger.info("RECORDS MANAGER: Auto-flush triggered at {} records", recordsInCurrentBatch);
            flush();
        }
        
        Group group = new SimpleGroup(SCHEMA);
        
        // Campos requeridos
        group.append("id", record.getId().trim());
        group.append("identifier", record.getIdentifier().trim());
        group.append("record_is_valid", record.getRecordIsValid());
        group.append("is_transformed", record.getIsTransformed());
        
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
            // Crear inmediatamente un nuevo writer para continuar escribiendo
            createNewBatchWriter();
        }
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
     * Abre el siguiente archivo batch.
     * Llamado automáticamente cuando se agota el archivo actual.
     */
    private boolean openNextBatch() throws IOException {
        if (currentReader != null) {
            currentReader.close();
            currentReader = null;
        }
        
        if (batchFiles == null || currentBatchIndex >= batchFiles.size()) {
            return false;  // No hay más archivos
        }
        
        Path batchPath = batchFiles.get(currentBatchIndex);
        logger.debug("RECORDS MANAGER: Opening batch file {}/{}: {}", 
                    currentBatchIndex + 1, batchFiles.size(), batchPath.getName());
        
        currentReader = ParquetReader.builder(new GroupReadSupport(), batchPath)
            .withConf(hadoopConf)
            .build();
        
        currentBatchIndex++;
        return true;
    }
    
    /**
     * Lee siguiente record (automáticamente de todos los archivos batch).
     * Transparente: El caller no sabe que hay múltiples archivos.
     * 
     * @return record o null si EOF (todos los archivos procesados)
     * @throws IOException si falla
     */
    public RecordValidation readNext() throws IOException {
        while (true) {
            // Si no hay reader actual, abrir primer/siguiente batch
            if (currentReader == null) {
                if (!openNextBatch()) {
                    return null;  // No hay más archivos
                }
            }
            
            // Intentar leer del batch actual
            Group group = currentReader.read();
            
            if (group != null) {
                recordsRead++;
                return convertGroupToRecord(group);
            }
            
            // EOF del batch actual, cerrar y continuar con siguiente
            currentReader.close();
            currentReader = null;
        }
    }
    
    /**
     * Lee todos los records de los archivos batch.
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
    
    /**
     * Convierte Group de Parquet a RecordValidation con RuleFacts anidados
     */
    private RecordValidation convertGroupToRecord(Group group) {
        RecordValidation record = new RecordValidation();
        
        // Campos requeridos
        record.setId(group.getString("id", 0));
        record.setIdentifier(group.getString("identifier", 0));
        record.setRecordIsValid(group.getBoolean("record_is_valid", 0));
        record.setIsTransformed(group.getBoolean("is_transformed", 0));
        
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
        // Cerrar writer si está activo
        if (currentWriter != null) {
            flush();
            logger.info("RECORDS MANAGER: Closed writer. Total: {} records in {} batch files", 
                        totalRecordsWritten, batchNumber);
        }
        
        // Cerrar reader si está activo
        if (currentReader != null) {
            currentReader.close();
            currentReader = null;
            logger.info("RECORDS MANAGER: Closed reader. Total records read: {} from {} batch files", 
                        recordsRead, batchFiles != null ? batchFiles.size() : 0);
        }
    }
}
