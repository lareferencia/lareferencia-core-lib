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

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * RECORD VALIDATION: 1 fila por record en Parquet.
 *
 * ARQUITECTURA SIMPLIFICADA CON RULE FACTS INTEGRADOS:
 * - Sin explosión: 1 registro = 1 fila (vs antiguo 1 registro = N filas)
 * - Rule facts incluidos: Lista de reglas aplicadas dentro del mismo record
 * - Paginación correcta: 20 filas = 20 records
 * - Estructura por snapshot: Cada snapshot tiene su propio directorio (sin particiones)
 *
 * ESQUEMA PARQUET:
 * - id: STRING (required) - ID único del record
 * - identifier: STRING (required) - Identificador OAI del record
 * - record_is_valid: BOOLEAN (required) - Si el record es válido
 * - is_transformed: BOOLEAN (required) - Si el record fue transformado
 * - rule_facts: LIST<RuleFact> (optional) - Detalles de todas las reglas aplicadas
 */
public class RecordValidation {

    private String id;
    private String identifier;
    
    private Boolean recordIsValid;
    private Boolean isTransformed;
    
    private List<RuleFact> ruleFacts;

    // Constructor vacío
    public RecordValidation() {
        this.ruleFacts = new ArrayList<>();
    }

    public RecordValidation(String id, String identifier, 
                           Boolean recordIsValid, Boolean isTransformed) {
        this.id = id;
        this.identifier = identifier;
        this.recordIsValid = recordIsValid;
        this.isTransformed = isTransformed;
        this.ruleFacts = new ArrayList<>();
    }
    
    public RecordValidation(String id, String identifier, 
                           Boolean recordIsValid, Boolean isTransformed, List<RuleFact> ruleFacts) {
        this.id = id;
        this.identifier = identifier;
        this.recordIsValid = recordIsValid;
        this.isTransformed = isTransformed;
        this.ruleFacts = ruleFacts != null ? ruleFacts : new ArrayList<>();
    }    // Getters y Setters
    
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
    
    public Boolean getRecordIsValid() {
        return recordIsValid;
    }    public void setRecordIsValid(Boolean recordIsValid) {
        this.recordIsValid = recordIsValid;
    }
    
    public Boolean getIsTransformed() {
        return isTransformed;
    }
    
    public void setIsTransformed(Boolean isTransformed) {
        this.isTransformed = isTransformed;
    }
    
    public List<RuleFact> getRuleFacts() {
        return ruleFacts;
    }
    
    public void setRuleFacts(List<RuleFact> ruleFacts) {
        this.ruleFacts = ruleFacts;
    }
    
    public void addRuleFact(RuleFact ruleFact) {
        if (this.ruleFacts == null) {
            this.ruleFacts = new ArrayList<>();
        }
        this.ruleFacts.add(ruleFact);
    }
    
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        RecordValidation that = (RecordValidation) o;
        return Objects.equals(id, that.id) &&
               Objects.equals(identifier, that.identifier) &&
               Objects.equals(recordIsValid, that.recordIsValid) &&
               Objects.equals(isTransformed, that.isTransformed) &&
               Objects.equals(ruleFacts, that.ruleFacts);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, identifier, recordIsValid, isTransformed, ruleFacts);
    }

    @Override
    public String toString() {
        return "RecordValidation{" +
                "id='" + id + '\'' +
                ", identifier='" + identifier + '\'' +
                ", recordIsValid=" + recordIsValid +
                ", isTransformed=" + isTransformed +
                ", ruleFacts=" + (ruleFacts != null ? ruleFacts.size() : 0) + " facts" +
                '}';
    }
}