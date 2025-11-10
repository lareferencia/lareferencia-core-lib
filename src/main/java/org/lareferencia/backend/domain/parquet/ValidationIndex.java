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

package org.lareferencia.backend.domain.parquet;

import java.io.Serializable;
import java.util.Objects;

/**
 * VALIDATION INDEX: Versión LIGERA de RecordValidation para carga en memoria.
 * 
 * PROPÓSITO:
 * - Índice lightweight que se puede cargar completo en memoria (~350 MB para 10M records)
 * - Solo campos esenciales para queries rápidas en workers
 * - Se persiste en paralelo a RecordValidation (archivo index único)
 * - Carga on-demand: Solo cuando se necesita, sin caché interno en manager
 * 
 * ARQUITECTURA:
 * - ValidationRecordManager escribe DOS versiones:
 *   1. Archivos batch completos: records_batch_*.parquet (con RuleFacts)
 *   2. Archivo index único: validation_index.parquet (solo estos campos)
 * 
 * - El index se puede cargar completamente en memoria para queries rápidas
 * - Método loadLightweightIndex() retorna List<ValidationIndex> on-demand
 * 
 * CAMPOS (solo 5 esenciales):
 * - recordId: Hash MD5 que referencia a OAIRecord
 * - identifier: Identificador OAI (denormalizado para búsquedas)
 * - recordIsValid: Si pasó validación
 * - isTransformed: Si fue transformado
 * - publishedMetadataHash: Hash del XML a indexar (opcional)
 * 
 * TAMAÑO ESTIMADO:
 * - ~35 bytes/record comprimido con SNAPPY
 * - 10M records = ~350 MB en memoria
 * - Viable para carga completa en heap de workers
 * 
 * USO EN WORKERS:
 * <pre>
 * // Cargar index completo cuando se necesita
 * List<ValidationIndex> index = validationRecordManager.loadLightweightIndex();
 * 
 * // Queries rápidas en memoria
 * long validCount = index.stream().filter(ValidationIndex::getRecordIsValid).count();
 * 
 * // Búsqueda por identifier
 * ValidationIndex record = index.stream()
 *     .filter(r -> r.getIdentifier().equals(targetId))
 *     .findFirst()
 *     .orElse(null);
 * </pre>
 */
public class ValidationIndex implements Serializable {
    
    private static final long serialVersionUID = 1L;
    
    /**
     * Hash MD5 del identifier - referencia a OAIRecord en catálogo.
     */
    private String recordId;
    
    /**
     * Identificador OAI del record (denormalizado para búsquedas).
     */
    private String identifier;
    
    /**
     * Si el record pasó validación.
     */
    private Boolean recordIsValid;
    
    /**
     * Si el record fue transformado.
     */
    private Boolean isTransformed;
    
    /**
     * Hash MD5 del XML a indexar (resultado de validación/transformación).
     * - Si isTransformed = true: hash del XML transformado
     * - Si isTransformed = false && recordIsValid = true: hash original
     * - Si recordIsValid = false: null
     */
    private String publishedMetadataHash;
    
    // Constructores
    
    public ValidationIndex() {
    }
    
    public ValidationIndex(String recordId, String identifier, 
                          Boolean recordIsValid, Boolean isTransformed,
                          String publishedMetadataHash) {
        this.recordId = recordId;
        this.identifier = identifier;
        this.recordIsValid = recordIsValid;
        this.isTransformed = isTransformed;
        this.publishedMetadataHash = publishedMetadataHash;
    }
    
    /**
     * Factory method: Crea ValidationIndex desde RecordValidation completo.
     * Extrae solo los campos necesarios para el índice ligero.
     * 
     * @param record RecordValidation completo
     * @return ValidationIndex ligero
     */
    public static ValidationIndex fromRecordValidation(RecordValidation record) {
        if (record == null) {
            return null;
        }
        
        return new ValidationIndex(
            record.getRecordId(),
            record.getIdentifier(),
            record.getRecordIsValid(),
            record.getIsTransformed(),
            record.getPublishedMetadataHash()
        );
    }
    
    // Getters y Setters
    
    public String getRecordId() {
        return recordId;
    }
    
    public void setRecordId(String recordId) {
        this.recordId = recordId;
    }
    
    public String getIdentifier() {
        return identifier;
    }
    
    public void setIdentifier(String identifier) {
        this.identifier = identifier;
    }
    
    public Boolean getRecordIsValid() {
        return recordIsValid;
    }
    
    public void setRecordIsValid(Boolean recordIsValid) {
        this.recordIsValid = recordIsValid;
    }
    
    public Boolean getIsTransformed() {
        return isTransformed;
    }
    
    public void setIsTransformed(Boolean isTransformed) {
        this.isTransformed = isTransformed;
    }
    
    public String getPublishedMetadataHash() {
        return publishedMetadataHash;
    }
    
    public void setPublishedMetadataHash(String publishedMetadataHash) {
        this.publishedMetadataHash = publishedMetadataHash;
    }
    
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ValidationIndex that = (ValidationIndex) o;
        return Objects.equals(recordId, that.recordId) &&
               Objects.equals(identifier, that.identifier) &&
               Objects.equals(recordIsValid, that.recordIsValid) &&
               Objects.equals(isTransformed, that.isTransformed) &&
               Objects.equals(publishedMetadataHash, that.publishedMetadataHash);
    }
    
    @Override
    public int hashCode() {
        return Objects.hash(recordId, identifier, recordIsValid, isTransformed, publishedMetadataHash);
    }
    
    @Override
    public String toString() {
        return "ValidationIndex{" +
                "recordId='" + recordId + '\'' +
                ", identifier='" + identifier + '\'' +
                ", recordIsValid=" + recordIsValid +
                ", isTransformed=" + isTransformed +
                ", publishedMetadataHash='" + publishedMetadataHash + '\'' +
                '}';
    }
}
