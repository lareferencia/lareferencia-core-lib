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

import java.util.Arrays;
import java.util.List;
import java.util.Objects;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;

/**
 * RULE FACT: Detalle de una regla aplicada a un record.
 * 
 * ARQUITECTURA SIMPLIFICADA:
 * - Almacenado dentro de RecordValidation como lista
 * - Cada RuleFact representa una regla validada en el record
 * - Mantiene array de occurrences para análisis detallado (xpath, campo, etc.)
 * 
 * USO:
 * - Análisis detallado de qué reglas se aplicaron a cada record
 * - Ubicaciones específicas donde se aplicó la regla (xpath, campo)
 * - Filtrado por reglas específicas
 * 
 * ESQUEMA:
 * - rule_id: INT32 (required) - ID de la regla validada
 * - valid_occurrences: LIST<STRING> (optional) - Ubicaciones donde la regla pasó
 * - invalid_occurrences: LIST<STRING> (optional) - Ubicaciones donde la regla falló
 * - is_valid: BOOLEAN (required) - Si la regla pasó o falló en general
 */
@Getter
@Setter
@EqualsAndHashCode
public class RuleFact {
    
    private Integer ruleId;
    private List<String> validOccurrences;
    private List<String> invalidOccurrences;    
    private Boolean isValid;
    
    public RuleFact() {
    }
    
    public RuleFact(Integer ruleId, List<String> validOccurrences, 
                   List<String> invalidOccurrences, Boolean isValid) {
        this.ruleId = ruleId;
        this.validOccurrences = validOccurrences;
        this.invalidOccurrences = invalidOccurrences;
        this.isValid = isValid;
    }
    

}
