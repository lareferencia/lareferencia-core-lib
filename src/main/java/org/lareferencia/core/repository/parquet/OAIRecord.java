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

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.LocalDateTime;
import java.util.Objects;

import org.lareferencia.core.domain.IOAIRecord;

/**
 * OAI RECORD CATALOG: Catálogo INMUTABLE de registros OAI cosechados.
 * 
 * ARQUITECTURA:
 * - Almacenado en Parquet, organizado por snapshot
 * - Se escribe UNA SOLA VEZ durante harvesting, nunca se actualiza
 * - NO contiene estado de validación (eso está en RecordValidation)
 * - Solo contiene hash de metadata ORIGINAL (publishedMetadataHash está en RecordValidation)
 * 
 * DIFERENCIAS CON OAIRecord (JPA):
 * - Sin anotaciones JPA (no es entidad SQL)
 * - Sin relación a NetworkSnapshot (se organiza por directorio)
 * - Sin estado de validación (RecordStatus, transformed)
 * - Sin publishedMetadataHash (movido a RecordValidation)
 * - ID es hash MD5 del identifier (no secuencial)
 * 
 * ESTRUCTURA DE ARCHIVOS:
 * /data/parquet/snapshot_{id}/catalog/oai_records_batch_*.parquet
 * 
 * ESQUEMA PARQUET:
 * - id: STRING (required) - Hash MD5 del identifier (garantiza unicidad)
 * - identifier: STRING (required) - Identificador OAI único del registro
 * - datestamp: TIMESTAMP (required) - Fecha de última modificación según OAI-PMH
 * - original_metadata_hash: STRING (required) - Hash MD5 del XML original cosechado
 * - deleted: BOOLEAN (required) - Flag que indica si el registro fue eliminado en el repositorio origen
 * 
 * USO:
 * - Harvesting: Escribir nuevos registros al cosechar (ID se calcula automáticamente)
 * - Validation: Leer registros para procesar (solo lectura)
 * - Indexing: Leer catálogo combinado con RecordValidation ligero (índice)
 */
public class OAIRecord implements IOAIRecord {

    private String id;  // Hash MD5 del identifier
    private String identifier;
    private LocalDateTime datestamp;
    private String originalMetadataHash;
    private Boolean deleted;

    /**
     * Constructor vacío (requerido para deserialización Parquet).
     */
    public OAIRecord() {
        this.deleted = false;
    }

    /**
     * Constructor completo.
     * 
     * @param id Hash MD5 del identifier (si es null, se calcula automáticamente)
     * @param identifier Identificador OAI
     * @param datestamp Fecha de última modificación
     * @param originalMetadataHash Hash MD5 del XML original
     * @param deleted Flag de eliminación
     */
    public OAIRecord(String id, String identifier, LocalDateTime datestamp, 
                     String originalMetadataHash, Boolean deleted) {
        this.id = id;
        this.identifier = identifier;
        this.datestamp = datestamp;
        this.originalMetadataHash = originalMetadataHash;
        this.deleted = deleted != null ? deleted : false;
        
        // Si no se proporciona ID, calcularlo automáticamente
        if (this.id == null && this.identifier != null) {
            this.id = generateIdFromIdentifier(this.identifier);
        }
    }

    // Getters y Setters

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getIdentifier() {
        return identifier;
    }

    public void setIdentifier(String identifier) {
        this.identifier = identifier;
    }

    public LocalDateTime getDatestamp() {
        return datestamp;
    }

    public void setDatestamp(LocalDateTime datestamp) {
        this.datestamp = datestamp;
    }

    public String getOriginalMetadataHash() {
        return originalMetadataHash;
    }

    public void setOriginalMetadataHash(String originalMetadataHash) {
        this.originalMetadataHash = originalMetadataHash;
    }

    public Boolean getDeleted() {
        return deleted;
    }

    public void setDeleted(Boolean deleted) {
        this.deleted = deleted;
    }

    // Utility methods
    
    /**
     * Genera un ID basado en el hash MD5 del identifier.
     * Este método se llama automáticamente si no se proporciona un ID en el constructor.
     * 
     * @param identifier Identificador OAI
     * @return Hash MD5 del identifier en formato hexadecimal
     */
    public static String generateIdFromIdentifier(String identifier) {
        if (identifier == null || identifier.isEmpty()) {
            throw new IllegalArgumentException("Identifier cannot be null or empty");
        }
        
        try {
            MessageDigest md = MessageDigest.getInstance("MD5");
            byte[] hashBytes = md.digest(identifier.getBytes());
            
            // Convertir bytes a hexadecimal
            StringBuilder hexString = new StringBuilder();
            for (byte b : hashBytes) {
                String hex = Integer.toHexString(0xff & b);
                if (hex.length() == 1) {
                    hexString.append('0');
                }
                hexString.append(hex);
            }
            return hexString.toString();
            
        } catch (NoSuchAlgorithmException e) {
            // MD5 siempre está disponible en Java
            throw new RuntimeException("MD5 algorithm not found", e);
        }
    }
    
    /**
     * Método de conveniencia para crear un OAIRecord con ID auto-generado.
     * 
     * @param identifier Identificador OAI
     * @param datestamp Fecha de última modificación
     * @param originalMetadataHash Hash MD5 del XML original
     * @param deleted Flag de eliminación
     * @return OAIRecord con ID calculado automáticamente
     */
    public static OAIRecord create(String identifier, LocalDateTime datestamp, 
                                   String originalMetadataHash, Boolean deleted) {
        String id = generateIdFromIdentifier(identifier);
        return new OAIRecord(id, identifier, datestamp, originalMetadataHash, deleted);
    }

    // equals, hashCode, toString

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        OAIRecord oaiRecord = (OAIRecord) o;
        return Objects.equals(id, oaiRecord.id) &&
               Objects.equals(identifier, oaiRecord.identifier);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, identifier);
    }

    @Override
    public String toString() {
        return "OAIRecord{" +
                "id=" + id +
                ", identifier='" + identifier + '\'' +
                ", datestamp=" + datestamp +
                ", originalMetadataHash='" + originalMetadataHash + '\'' +
                ", deleted=" + deleted +
                '}';
    }
}
