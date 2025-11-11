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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.lareferencia.core.metadata.SnapshotMetadata;
import org.lareferencia.core.util.PathUtils;
import org.lareferencia.backend.domain.parquet.SnapshotValidationStats;
import org.lareferencia.backend.domain.Network;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * SNAPSHOT METADATA MANAGER: Lectura/escritura de metadata de snapshot en JSON.
 * 
 * PROPÓSITO:
 * - Almacenar datos de snapshot UNA SOLA VEZ (no replicados en cada record)
 * - Formato JSON legible y fácil de inspeccionar
 * - Consultas ultra-rápidas (<1ms) para estadísticas agregadas
 * 
 * UBICACIÓN (NUEVA ESTRUCTURA):
 * - {baseDir}/{NETWORK}/snapshots/snapshot_{snapshotId}/metadata.json
 * 
 * CONTENIDO:
 * - snapshot_id, network, total_records, valid_records, invalid_records, created_at
 */
public final class SnapshotMetadataManager {
    
    private static final Logger logger = LogManager.getLogger(SnapshotMetadataManager.class);
    private static final ObjectMapper mapper = createMapper();
    
    private static final String VALIDATION_SUBDIR = "validation";
    private static final String METADATA_FILENAME = "metadata.json";
    private static final String VALIDATION_STATS_FILENAME = "validation-stats.json";
    
    /**
     * Mixin para serialización de Network: ignora relaciones JPA lazy
     */
    @JsonIgnoreProperties(value = {
        "snapshots", 
        "prevalidator", 
        "validator", 
        "transformer", 
        "secondaryTransformer"
    }, ignoreUnknown = true)
    private abstract static class NetworkSerializationMixin {
    }
    
    /**
     * Crea ObjectMapper configurado para serialización de metadata con Network
     */
    private static ObjectMapper createMapper() {
        ObjectMapper mapper = new ObjectMapper()
                .enable(SerializationFeature.INDENT_OUTPUT)
                .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
                .configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false)
                // Permitir deserialización sin setters (Lombok @Getter sin @Setter)
                .configure(DeserializationFeature.FAIL_ON_MISSING_CREATOR_PROPERTIES, false);
        
        // Configurar mixin para ignorar relaciones lazy de Network
        mapper.addMixIn(Network.class, NetworkSerializationMixin.class);
        
        return mapper;
    }
    
    /**
     * Escribe metadata de snapshot en JSON
     * 
     * @param baseDir directorio base de almacenamiento
     * @param metadata datos del snapshot
     * @throws IOException si falla la escritura
     */
    public static void writeMetadata(String baseDir, SnapshotMetadata metadata) throws IOException {
        if (metadata == null || metadata.getSnapshotId() == null) {
            throw new IllegalArgumentException("Metadata y snapshotId no pueden ser null");
        }

        String snapshotDir = PathUtils.getSnapshotPath(baseDir, metadata);
        Path dirPath = Paths.get(snapshotDir);
        
        // Crear directorio si no existe
        if (!Files.exists(dirPath)) {
            Files.createDirectories(dirPath);
            logger.debug("Created snapshot directory: {}", snapshotDir);
        }
        
        // Escribir JSON
        String metadataPath = String.format("%s/%s", snapshotDir, METADATA_FILENAME);
        mapper.writeValue(new File(metadataPath), metadata);
        
        logger.info("METADATA WRITTEN: snapshot={}, network={}, path={}", 
            metadata.getSnapshotId(), metadata.getNetworkAcronym(), metadataPath);
    }
    
    /**
     * Lee metadata de snapshot desde JSON
     * 
     * @param baseDir directorio base de almacenamiento
     * @param snapshotMetadata metadata del snapshot (necesita snapshotId y networkAcronym)
     * @return metadata del snapshot o null si no existe
     * @throws IOException si falla la lectura
     */
    public static SnapshotMetadata readMetadata(String baseDir, SnapshotMetadata snapshotMetadata) throws IOException {
        if (snapshotMetadata == null || snapshotMetadata.getSnapshotId() == null) {
            throw new IllegalArgumentException("snapshotMetadata y snapshotId no pueden ser null");
        }
        
        String metadataPath = String.format("%s/%s", 
            PathUtils.getSnapshotPath(baseDir, snapshotMetadata), METADATA_FILENAME);
        File file = new File(metadataPath);
        
        if (!file.exists()) {
            logger.warn("Metadata file not found: {}", metadataPath);
            return null;
        }
        
        SnapshotMetadata metadata = mapper.readValue(file, SnapshotMetadata.class);
        logger.debug("METADATA READ: snapshot={}, network={}, records={}", 
            snapshotMetadata.getSnapshotId(), snapshotMetadata.getNetworkAcronym(), metadata.getSize());
        return metadata;
    }    /**
     * Verifica si existe metadata para un snapshot
     * 
     * @param baseDir directorio base
     * @param snapshotMetadata metadata del snapshot (necesita snapshotId y networkAcronym)
     * @return true si existe el archivo metadata.json
     */
    public static boolean metadataExists(String baseDir, SnapshotMetadata snapshotMetadata) {
        if (snapshotMetadata == null || snapshotMetadata.getSnapshotId() == null) {
            return false;
        }
        
        String metadataPath = String.format("%s/%s", 
            PathUtils.getSnapshotPath(baseDir, snapshotMetadata), METADATA_FILENAME);
        return new File(metadataPath).exists();
    }
    
   
    
    /**
     * Elimina metadata de un snapshot
     * 
     * @param baseDir directorio base
     * @param snapshotMetadata metadata del snapshot (necesita snapshotId y networkAcronym)
     * @return true si se eliminó correctamente
     */
    public static boolean deleteMetadata(String baseDir, SnapshotMetadata snapshotMetadata) {
        if (snapshotMetadata == null || snapshotMetadata.getSnapshotId() == null) {
            return false;
        }
        
        String metadataPath = String.format("%s/%s", 
            PathUtils.getSnapshotPath(baseDir, snapshotMetadata), METADATA_FILENAME);
        File file = new File(metadataPath);
        
        if (file.exists() && file.delete()) {
            logger.info("METADATA DELETED: snapshot={}, network={}", 
                snapshotMetadata.getSnapshotId(), snapshotMetadata.getNetworkAcronym());
            return true;
        }
        
        return false;
    }
    
    /**
     * Escribe estadísticas de validación de snapshot en JSON
     * 
     * @param baseDir directorio base de almacenamiento
     * @param validationStats estadísticas de validación del snapshot
     * @throws IOException si falla la escritura
     */
    public static void writeValidationStats(String baseDir, SnapshotValidationStats validationStats) throws IOException {
        if (validationStats == null || validationStats.getSnapshotMetadata() == null || validationStats.getSnapshotMetadata().getSnapshotId() == null) {
            throw new IllegalArgumentException("validationStats, snapshotMetadata y snapshotId no pueden ser null");
        }
        
        SnapshotMetadata metadata = validationStats.getSnapshotMetadata();
        
        String validationDir = String.format("%s/%s", 
            PathUtils.getSnapshotPath(baseDir, metadata), VALIDATION_SUBDIR);
        Path dirPath = Paths.get(validationDir);
        
        // Crear directorio si no existe
        if (!Files.exists(dirPath)) {
            Files.createDirectories(dirPath);
            logger.debug("Created validation directory: {}", validationDir);
        }
        
        // Escribir JSON
        String validationStatsPath = String.format("%s/%s", validationDir, VALIDATION_STATS_FILENAME);
        mapper.writeValue(new File(validationStatsPath), validationStats);
        
        logger.info("VALIDATION STATS WRITTEN: snapshot={}, network={}, path={}", 
            metadata.getSnapshotId(), metadata.getNetworkAcronym(), validationStatsPath);
    }
    
    /**
     * Lee estadísticas de validación de snapshot desde JSON
     * 
     * @param baseDir directorio base de almacenamiento
     * @param snapshotMetadata metadata del snapshot (necesita snapshotId y networkAcronym)
     * @return estadísticas de validación del snapshot o null si no existe
     * @throws IOException si falla la lectura
     */
    public static SnapshotValidationStats readValidationStats(String baseDir, SnapshotMetadata snapshotMetadata) throws IOException {
        if (snapshotMetadata == null || snapshotMetadata.getSnapshotId() == null) {
            throw new IllegalArgumentException("snapshotMetadata y snapshotId no pueden ser null");
        }
        
        String validationStatsPath = String.format("%s/%s/%s", 
            PathUtils.getSnapshotPath(baseDir, snapshotMetadata), 
            VALIDATION_SUBDIR, VALIDATION_STATS_FILENAME);
        File file = new File(validationStatsPath);
        
        if (!file.exists()) {
            logger.warn("VALIDATION STATS NOT FOUND: snapshot={}, network={}, path={}", 
                snapshotMetadata.getSnapshotId(), snapshotMetadata.getNetworkAcronym(), validationStatsPath);
            return null;
        }
        
        SnapshotValidationStats validationStats = mapper.readValue(file, SnapshotValidationStats.class);
        logger.debug("VALIDATION STATS READ: snapshot={}, network={}, totalRecords={}", 
            snapshotMetadata.getSnapshotId(), snapshotMetadata.getNetworkAcronym(), validationStats.getTotalRecords());
        return validationStats;
    }
    
    /**
     * Verifica si existen estadísticas de validación para un snapshot
     * 
     * @param baseDir directorio base
     * @param snapshotMetadata metadata del snapshot (necesita snapshotId y networkAcronym)
     * @return true si existe el archivo validation-stats.json
     */
    public static boolean validationStatsExists(String baseDir, SnapshotMetadata snapshotMetadata) {
        if (snapshotMetadata == null || snapshotMetadata.getSnapshotId() == null) {
            return false;
        }
        
        String validationStatsPath = String.format("%s/%s/%s", 
            PathUtils.getSnapshotPath(baseDir, snapshotMetadata), 
            VALIDATION_SUBDIR, VALIDATION_STATS_FILENAME);
        return new File(validationStatsPath).exists();
    }
    
    /**
     * Elimina estadísticas de validación de un snapshot
     * 
     * @param baseDir directorio base
     * @param snapshotMetadata metadata del snapshot (necesita snapshotId y networkAcronym)
     * @return true si se eliminó correctamente
     */
    public static boolean deleteValidationStats(String baseDir, SnapshotMetadata snapshotMetadata) {
        if (snapshotMetadata == null || snapshotMetadata.getSnapshotId() == null) {
            return false;
        }
        
        String validationStatsPath = String.format("%s/%s/%s", 
            PathUtils.getSnapshotPath(baseDir, snapshotMetadata), 
            VALIDATION_SUBDIR, VALIDATION_STATS_FILENAME);
        File file = new File(validationStatsPath);
        
        if (file.exists() && file.delete()) {
            logger.info("VALIDATION STATS DELETED: snapshot={}, network={}", 
                snapshotMetadata.getSnapshotId(), snapshotMetadata.getNetworkAcronym());
            return true;
        }
        
        return false;
    }
}
