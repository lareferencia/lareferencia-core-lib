/*
 *   Copyright (c) 2013-2022. LA Referencia / Red CLARA and    @PostConstruct
    public void init() {
        try {
            // Verificar si existe el directorio
            Path dirPath = Paths.get(parquetBasePath);
            if (!Files.exists(dirPath)) {
                Files.createDirectories(dirPath);
                logger.info("Creado directorio para archivos Parquet: {}", parquetBasePath);
            }
            
            logger.info("ValidationStatParquetRepository inicializado - listo para usar en: {}", parquetBasePath);
        } catch (Exception e) {
            logger.error("Error inicializando ValidationStatParquetRepository", e);
            throw new RuntimeException("Error inicializando repositorio Parquet", e);
        }
    }his program is free software: you can redistribute it and/or modify
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
import org.apache.hadoop.fs.Path;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.lareferencia.backend.domain.parquet.ValidationStatObservationParquet;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Repository;

import javax.annotation.PostConstruct;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Repositorio para datos de validación en Parquet con persistencia real.
 * Utiliza Apache Parquet para almacenamiento eficiente en disco.
 */
@Repository
public class ValidationStatParquetRepository {

    private static final Logger logger = LogManager.getLogger(ValidationStatParquetRepository.class);

    @Value("${validation.stats.parquet.path:/tmp/validation-stats-parquet}")
    private String parquetBasePath;

    private Schema avroSchema;
    private Configuration hadoopConf;
    
    @PostConstruct
    public void init() throws IOException {
        // Inicializar configuración de Hadoop
        hadoopConf = new Configuration();
        hadoopConf.set("fs.defaultFS", "file:///");
        
        // Crear directorio base si no existe
        Files.createDirectories(Paths.get(parquetBasePath));
        
        // Definir el esquema Avro para los datos de validación
        initializeAvroSchema();
        
        logger.info("ValidationStatParquetRepository inicializado - listo para usar en: {}", parquetBasePath);
    }
    
    private void initializeAvroSchema() {
        // Definir el esquema Avro para ValidationStatObservation
        String schemaJson = "{\n" +
            "  \"type\": \"record\",\n" +
            "  \"name\": \"ValidationStatObservation\",\n" +
            "  \"namespace\": \"org.lareferencia.backend.domain.parquet\",\n" +
            "  \"fields\": [\n" +
            "    {\"name\": \"id\", \"type\": \"string\"},\n" +
            "    {\"name\": \"identifier\", \"type\": \"string\"},\n" +
            "    {\"name\": \"snapshotID\", \"type\": \"long\"},\n" +
            "    {\"name\": \"origin\", \"type\": \"string\"},\n" +
            "    {\"name\": \"setSpec\", \"type\": [\"null\", \"string\"], \"default\": null},\n" +
            "    {\"name\": \"metadataPrefix\", \"type\": [\"null\", \"string\"], \"default\": null},\n" +
            "    {\"name\": \"networkAcronym\", \"type\": [\"null\", \"string\"], \"default\": null},\n" +
            "    {\"name\": \"repositoryName\", \"type\": [\"null\", \"string\"], \"default\": null},\n" +
            "    {\"name\": \"institutionName\", \"type\": [\"null\", \"string\"], \"default\": null},\n" +
            "    {\"name\": \"isValid\", \"type\": \"boolean\"},\n" +
            "    {\"name\": \"isTransformed\", \"type\": \"boolean\"},\n" +
            "    {\"name\": \"validOccurrencesByRuleID\", \"type\": [\"null\", {\"type\": \"map\", \"values\": {\"type\": \"array\", \"items\": \"string\"}}], \"default\": null},\n" +
            "    {\"name\": \"invalidOccurrencesByRuleID\", \"type\": [\"null\", {\"type\": \"map\", \"values\": {\"type\": \"array\", \"items\": \"string\"}}], \"default\": null},\n" +
            "    {\"name\": \"validRulesIDList\", \"type\": [\"null\", {\"type\": \"array\", \"items\": \"string\"}], \"default\": null},\n" +
            "    {\"name\": \"invalidRulesIDList\", \"type\": [\"null\", {\"type\": \"array\", \"items\": \"string\"}], \"default\": null}\n" +
            "  ]\n" +
            "}";
        
        avroSchema = new Schema.Parser().parse(schemaJson);
    }
    
    private String getParquetFilePath(Long snapshotId) {
        return parquetBasePath + "/snapshot_" + snapshotId + ".parquet";
    }
    
    private GenericRecord toGenericRecord(ValidationStatObservationParquet observation) {
        GenericRecord record = new GenericData.Record(avroSchema);
        record.put("id", observation.getId());
        record.put("identifier", observation.getIdentifier());
        record.put("snapshotID", observation.getSnapshotID());
        record.put("origin", observation.getOrigin());
        record.put("setSpec", observation.getSetSpec());
        record.put("metadataPrefix", observation.getMetadataPrefix());
        record.put("networkAcronym", observation.getNetworkAcronym());
        record.put("repositoryName", observation.getRepositoryName());
        record.put("institutionName", observation.getInstitutionName());
        record.put("isValid", observation.getIsValid());
        record.put("isTransformed", observation.getIsTransformed());
        record.put("validOccurrencesByRuleID", observation.getValidOccurrencesByRuleID());
        record.put("invalidOccurrencesByRuleID", observation.getInvalidOccurrencesByRuleID());
        record.put("validRulesIDList", observation.getValidRulesIDList());
        record.put("invalidRulesIDList", observation.getInvalidRulesIDList());
        return record;
    }
    
    /**
     * Convierte un objeto Avro a String (maneja Utf8 y String)
     */
    private String avroToString(Object obj) {
        if (obj == null) {
            return null;
        }
        return obj.toString(); // Funciona tanto para String como para Utf8
    }
    
    /**
     * Convierte una lista de Avro (con posibles elementos Utf8) a Lista de String
     */
    @SuppressWarnings("unchecked")
    private List<String> convertStringList(Object obj) {
        if (obj == null) {
            return null;
        }
        List<Object> avroList = (List<Object>) obj;
        return avroList.stream()
                .map(this::avroToString)
                .collect(Collectors.toList());
    }
    
    /**
     * Convierte un Map de Avro (con keys y valores Utf8) a Map<String, List<String>>
     */
    @SuppressWarnings("unchecked")
    private Map<String, List<String>> convertMapWithStringLists(Object obj) {
        if (obj == null) {
            return null;
        }
        Map<Object, Object> avroMap = (Map<Object, Object>) obj;
        Map<String, List<String>> result = new HashMap<>();
        
        for (Map.Entry<Object, Object> entry : avroMap.entrySet()) {
            String key = avroToString(entry.getKey());
            List<String> value = convertStringList(entry.getValue());
            result.put(key, value);
        }
        
        return result;
    }
    
    @SuppressWarnings("unchecked")
    private ValidationStatObservationParquet fromGenericRecord(GenericRecord record) {
        ValidationStatObservationParquet observation = new ValidationStatObservationParquet();
        observation.setId(avroToString(record.get("id")));
        observation.setIdentifier(avroToString(record.get("identifier")));
        observation.setSnapshotID((Long) record.get("snapshotID"));
        observation.setOrigin(avroToString(record.get("origin")));
        observation.setSetSpec(avroToString(record.get("setSpec")));
        observation.setMetadataPrefix(avroToString(record.get("metadataPrefix")));
        observation.setNetworkAcronym(avroToString(record.get("networkAcronym")));
        observation.setRepositoryName(avroToString(record.get("repositoryName")));
        observation.setInstitutionName(avroToString(record.get("institutionName")));
        observation.setIsValid((Boolean) record.get("isValid"));
        observation.setIsTransformed((Boolean) record.get("isTransformed"));
        observation.setValidOccurrencesByRuleID(convertMapWithStringLists(record.get("validOccurrencesByRuleID")));
        observation.setInvalidOccurrencesByRuleID(convertMapWithStringLists(record.get("invalidOccurrencesByRuleID")));
        observation.setValidRulesIDList(convertStringList(record.get("validRulesIDList")));
        observation.setInvalidRulesIDList(convertStringList(record.get("invalidRulesIDList")));
        return observation;
    }

    /**
     * Busca todas las observaciones por snapshot ID desde archivo Parquet
     */
    public List<ValidationStatObservationParquet> findBySnapshotId(Long snapshotId) throws IOException {
        String filePath = getParquetFilePath(snapshotId);
        File file = new File(filePath);
        
        if (!file.exists()) {
            logger.debug("Archivo Parquet no existe para snapshot: {}", snapshotId);
            return Collections.emptyList();
        }
        
        List<ValidationStatObservationParquet> observations = new ArrayList<>();
        
        try (ParquetReader<GenericRecord> reader = AvroParquetReader.<GenericRecord>builder(new Path(filePath))
                .withConf(hadoopConf)
                .build()) {
            
            GenericRecord record;
            while ((record = reader.read()) != null) {
                observations.add(fromGenericRecord(record));
            }
        }
        
        logger.debug("Leídas {} observaciones para snapshot: {}", observations.size(), snapshotId);
        return observations;
    }

    /**
     * Implementa paginación simple
     */
    public List<ValidationStatObservationParquet> findBySnapshotIdWithPagination(Long snapshotId, int page, int size) throws IOException {
        List<ValidationStatObservationParquet> observations = findBySnapshotId(snapshotId);
        
        int start = page * size;
        int end = Math.min(start + size, observations.size());
        
        if (start >= observations.size()) {
            return Collections.emptyList();
        }
        
        return observations.subList(start, end);
    }

    /**
     * Cuenta observaciones por snapshot ID
     */
    public long countBySnapshotId(Long snapshotId) throws IOException {
        return findBySnapshotId(snapshotId).size();
    }

    /**
     * Guarda todas las observaciones en archivos Parquet por snapshot
     */
    public void saveAll(List<ValidationStatObservationParquet> observations) throws IOException {
        if (observations == null || observations.isEmpty()) {
            return;
        }
        
        // Agrupar por snapshot ID
        Map<Long, List<ValidationStatObservationParquet>> groupedBySnapshot = observations.stream()
            .collect(Collectors.groupingBy(ValidationStatObservationParquet::getSnapshotID));
        
        // Escribir cada grupo en su archivo Parquet correspondiente
        for (Map.Entry<Long, List<ValidationStatObservationParquet>> entry : groupedBySnapshot.entrySet()) {
            Long snapshotId = entry.getKey();
            List<ValidationStatObservationParquet> snapshotObservations = entry.getValue();
            
            saveObservationsToParquet(snapshotId, snapshotObservations);
        }
        
        logger.info("Guardadas {} observaciones en archivos Parquet", observations.size());
    }
    
    private void saveObservationsToParquet(Long snapshotId, List<ValidationStatObservationParquet> observations) throws IOException {
        String filePath = getParquetFilePath(snapshotId);
        
        // Obtener observaciones existentes si el archivo ya existe
        List<ValidationStatObservationParquet> existingObservations = new ArrayList<>();
        File parquetFile = new File(filePath);
        
        if (parquetFile.exists()) {
            existingObservations = findBySnapshotId(snapshotId);
        }
        
        // Combinar observaciones existentes con las nuevas
        Set<String> existingIds = existingObservations.stream()
            .map(ValidationStatObservationParquet::getId)
            .collect(Collectors.toSet());
        
        List<ValidationStatObservationParquet> allObservations = new ArrayList<>(existingObservations);
        
        // Agregar solo las nuevas observaciones (evitar duplicados)
        for (ValidationStatObservationParquet obs : observations) {
            if (!existingIds.contains(obs.getId())) {
                allObservations.add(obs);
            }
        }
        
        // Escribir todas las observaciones al archivo Parquet
        try (ParquetWriter<GenericRecord> writer = AvroParquetWriter.<GenericRecord>builder(new Path(filePath))
                .withSchema(avroSchema)
                .withConf(hadoopConf)
                .withCompressionCodec(CompressionCodecName.SNAPPY)
                .withWriteMode(ParquetFileWriter.Mode.OVERWRITE)
                .build()) {
            
            for (ValidationStatObservationParquet observation : allObservations) {
                GenericRecord record = toGenericRecord(observation);
                writer.write(record);
            }
        }
        
        logger.debug("Guardadas {} observaciones para snapshot {} en archivo Parquet", allObservations.size(), snapshotId);
    }

    /**
     * Elimina observaciones por snapshot ID (elimina el archivo Parquet)
     */
    public void deleteBySnapshotId(Long snapshotId) throws IOException {
        String filePath = getParquetFilePath(snapshotId);
        File file = new File(filePath);
        
        if (file.exists()) {
            boolean deleted = file.delete();
            if (deleted) {
                logger.info("Eliminado archivo Parquet para snapshot: {}", snapshotId);
            } else {
                logger.warn("No se pudo eliminar archivo Parquet para snapshot: {}", snapshotId);
            }
        }
    }

    /**
     * Elimina observación específica por ID
     */
    public void deleteById(String id, Long snapshotId) throws IOException {
        List<ValidationStatObservationParquet> observations = findBySnapshotId(snapshotId);
        
        List<ValidationStatObservationParquet> filteredObservations = observations.stream()
            .filter(obs -> !id.equals(obs.getId()))
            .collect(Collectors.toList());
        
        if (filteredObservations.size() < observations.size()) {
            // Re-escribir el archivo sin la observación eliminada
            String filePath = getParquetFilePath(snapshotId);
            writeObservationsToParquet(filePath, filteredObservations);
            logger.info("Eliminada observación {} del snapshot {}", id, snapshotId);
        }
    }

    /**
     * Copia datos de un snapshot a otro
     */
    public void copySnapshotData(Long originalSnapshotId, Long newSnapshotId) throws IOException {
        List<ValidationStatObservationParquet> originalData = findBySnapshotId(originalSnapshotId);
        
        if (!originalData.isEmpty()) {
            List<ValidationStatObservationParquet> copiedData = new ArrayList<>();
            for (ValidationStatObservationParquet obs : originalData) {
                ValidationStatObservationParquet copy = new ValidationStatObservationParquet();
                copyObservation(obs, copy);
                copy.setSnapshotID(newSnapshotId);
                copiedData.add(copy);
            }
            
            saveObservationsToParquet(newSnapshotId, copiedData);
            logger.info("Copiados datos del snapshot {} al snapshot {}", originalSnapshotId, newSnapshotId);
        }
    }
    
    private void writeObservationsToParquet(String filePath, List<ValidationStatObservationParquet> observations) throws IOException {
        try (ParquetWriter<GenericRecord> writer = AvroParquetWriter.<GenericRecord>builder(new Path(filePath))
                .withSchema(avroSchema)
                .withConf(hadoopConf)
                .withCompressionCodec(CompressionCodecName.SNAPPY)
                .withWriteMode(ParquetFileWriter.Mode.OVERWRITE)
                .build()) {
            
            for (ValidationStatObservationParquet observation : observations) {
                GenericRecord record = toGenericRecord(observation);
                writer.write(record);
            }
        }
    }

    /**
     * Obtiene estadísticas agregadas por snapshot ID
     */
    public Map<String, Object> getAggregatedStats(Long snapshotId) throws IOException {
        List<ValidationStatObservationParquet> observations = findBySnapshotId(snapshotId);
        
        Map<String, Object> stats = new HashMap<>();
        
        long totalCount = observations.size();
        long validCount = observations.stream().mapToLong(obs -> obs.getIsValid() ? 1L : 0L).sum();
        long transformedCount = observations.stream().mapToLong(obs -> obs.getIsTransformed() ? 1L : 0L).sum();
        
        // Contar reglas válidas e inválidas
        Map<String, Long> validRuleCounts = new HashMap<>();
        Map<String, Long> invalidRuleCounts = new HashMap<>();
        
        for (ValidationStatObservationParquet obs : observations) {
            if (obs.getValidRulesIDList() != null) {
                for (String ruleId : obs.getValidRulesIDList()) {
                    validRuleCounts.put(ruleId, validRuleCounts.getOrDefault(ruleId, 0L) + 1);
                }
            }
            if (obs.getInvalidRulesIDList() != null) {
                for (String ruleId : obs.getInvalidRulesIDList()) {
                    invalidRuleCounts.put(ruleId, invalidRuleCounts.getOrDefault(ruleId, 0L) + 1);
                }
            }
        }
        
        stats.put("totalCount", totalCount);
        stats.put("validCount", validCount);
        stats.put("transformedCount", transformedCount);
        stats.put("validRuleCounts", validRuleCounts);
        stats.put("invalidRuleCounts", invalidRuleCounts);
        
        return stats;
    }

    /**
     * Obtiene conteos de ocurrencias de reglas
     */
    public Map<String, Long> getRuleOccurrenceCounts(Long snapshotId, String ruleId, boolean valid) throws IOException {
        List<ValidationStatObservationParquet> observations = findBySnapshotId(snapshotId);
        Map<String, Long> occurrenceCounts = new HashMap<>();
        
        for (ValidationStatObservationParquet obs : observations) {
            Map<String, List<String>> occurrenceMap = valid ? 
                obs.getValidOccurrencesByRuleID() : 
                obs.getInvalidOccurrencesByRuleID();
                
            if (occurrenceMap != null && occurrenceMap.containsKey(ruleId)) {
                List<String> occurrences = occurrenceMap.get(ruleId);
                if (occurrences != null) {
                    for (String occurrence : occurrences) {
                        occurrenceCounts.put(occurrence, occurrenceCounts.getOrDefault(occurrence, 0L) + 1);
                    }
                }
            }
        }
        
        return occurrenceCounts;
    }

    private void copyObservation(ValidationStatObservationParquet source, ValidationStatObservationParquet target) {
        target.setId(source.getId());
        target.setIdentifier(source.getIdentifier());
        target.setSnapshotID(source.getSnapshotID());
        target.setOrigin(source.getOrigin());
        target.setSetSpec(source.getSetSpec());
        target.setMetadataPrefix(source.getMetadataPrefix());
        target.setNetworkAcronym(source.getNetworkAcronym());
        target.setRepositoryName(source.getRepositoryName());
        target.setInstitutionName(source.getInstitutionName());
        target.setIsValid(source.getIsValid());
        target.setIsTransformed(source.getIsTransformed());
        target.setValidOccurrencesByRuleID(source.getValidOccurrencesByRuleID());
        target.setInvalidOccurrencesByRuleID(source.getInvalidOccurrencesByRuleID());
        target.setValidRulesIDList(source.getValidRulesIDList());
        target.setInvalidRulesIDList(source.getInvalidRulesIDList());
    }
}
