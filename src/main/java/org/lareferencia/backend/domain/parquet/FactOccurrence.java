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

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * FACT TABLE REPRESENTATION: Una fila por ocurrencia de regla.
 * 
 * ARQUITECTURA:
 * - Cada valor de validación genera una fila separada
 * - Permite análisis dimensional eficiente
 * - Optimizado para agregaciones y filtros
 * - Soporta particionamiento por snapshot_id, network, is_valid
 * 
 * COLUMNAS:
 * - Dimensiones: id, identifier, snapshot_id, origin, network, repository, institution
 * - Métricas: rule_id, value, is_valid, is_transformed
 * - Metadatos: metadata_prefix, set_spec
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class FactOccurrence {
    
    // ==================== IDENTIFICADORES ====================
    
    /**
     * ID del documento fuente (requerido)
     */
    private String id;
    
    /**
     * Identificador OAI u otro externo (requerido)
     */
    private String identifier;
    
    /**
     * ID del snapshot - usado para particionamiento (requerido)
     */
    private Long snapshotId;
    
    // ==================== DIMENSIONES ====================
    
    /**
     * URL de origen (requerido)
     */
    private String origin;
    
    /**
     * Acrónimo de red - usado para particionamiento (nullable)
     */
    private String network;
    
    /**
     * Nombre de repositorio (nullable)
     */
    private String repository;
    
    /**
     * Nombre de institución (nullable)
     */
    private String institution;
    
    // ==================== HECHO CENTRAL ====================
    
    /**
     * ID de la regla de validación (requerido)
     * Parseado desde string a integer
     */
    private Integer ruleId;
    
    /**
     * Valor específico de la ocurrencia (nullable)
     * Una fila por cada valor único
     */
    private String value;
    
    /**
     * Indica si es válida o inválida - usado para particionamiento (requerido)
     * true = proviene de validOccurrencesByRuleID
     * false = proviene de invalidOccurrencesByRuleID
     */
    private Boolean isValid;
    
    /**
     * Indica si el registro completo es válido (requerido)
     * true = el record original pasó la validación completa (obs.getIsValid())
     * false = el record original tiene errores de validación
     * Este campo representa el estado de validación a nivel de record, no de ocurrencia individual
     */
    private Boolean recordIsValid;
    
    /**
     * Indica si el registro fue transformado (requerido)
     */
    private Boolean isTransformed;
    
    // ==================== METADATOS ====================
    
    /**
     * Prefijo de metadatos (nullable)
     * Ejemplo: "xoai"
     */
    private String metadataPrefix;
    
    /**
     * Especificación de set OAI (nullable)
     */
    private String setSpec;
    
    // ==================== UTILIDADES ====================
    
    /**
     * Normaliza un string: trim, convierte "" a null, opcional truncamiento
     */
    public static String normalize(String s) {
        if (s == null) {
            return null;
        }
        String trimmed = s.trim();
        if (trimmed.isEmpty()) {
            return null;
        }
        // Truncar valores patológicamente largos (16KB)
        return trimmed.length() > 16_384 ? trimmed.substring(0, 16_384) : trimmed;
    }
    
    /**
     * Valida campos requeridos
     */
    public static String mustNotBeNull(String s, String fieldName) {
        if (s == null || s.trim().isEmpty()) {
            throw new IllegalArgumentException("Campo requerido vacío: " + fieldName);
        }
        return s.trim();
    }
    
    /**
     * Parsea rule ID desde string, retorna null si falla
     */
    public static Integer tryParseRuleId(String key, String idForLog) {
        try {
            return Integer.parseInt(key.trim());
        } catch (NumberFormatException ex) {
            System.err.println("WARN: rule_id inválido '" + key + "' en id=" + idForLog);
            return null;
        }
    }
    
    /**
     * Genera clave única para deduplicación intra-registro
     */
    public String getDeduplicationKey() {
        return ruleId + "\u0001" + String.valueOf(value) + "\u0001" + isValid;
    }
    
    /**
     * Obtiene el path de partición para este registro
     * Formato: snapshot_id={id}/network={net}/is_valid={valid}
     */
    public String getPartitionPath() {
        String networkPart = network != null ? network : "UNKNOWN";
        return String.format("snapshot_id=%d/network=%s/is_valid=%s", 
                           snapshotId, networkPart, isValid);
    }
}
