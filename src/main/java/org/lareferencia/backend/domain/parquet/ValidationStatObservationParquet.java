/*
 *   Copyright (c) 2013-2022. LA Referencia / Red CLARA and others
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

import lombok.Getter;
import lombok.Setter;
import lombok.NoArgsConstructor;
import lombok.AllArgsConstructor;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Representación de ValidationStatObservation para almacenamiento en archivos Parquet.
 * Esta clase es compatible con la estructura original pero optimizada para Parquet.
 */
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
public class ValidationStatObservationParquet {
    
    private String id;
    private String identifier;
    private Long snapshotID;
    private String origin;
    private String setSpec;
    private String metadataPrefix;
    private String networkAcronym;
    private String repositoryName;
    private String institutionName;
    private Boolean isValid;
    private Boolean isTransformed;
    
    // Para facilitar el almacenamiento en Parquet, se almacenan como strings separadas por comas
    private String validOccurrencesByRuleIDJson;
    private String invalidOccurrencesByRuleIDJson;
    private String validRulesID;
    private String invalidRulesID;
    
    // Campos transientes para la conversión
    private transient Map<String, List<String>> validOccurrencesByRuleID;
    private transient Map<String, List<String>> invalidOccurrencesByRuleID;
    private transient List<String> validRulesIDList;
    private transient List<String> invalidRulesIDList;
    
    /**
     * Constructor a partir de los mapas y listas originales
     */
    public ValidationStatObservationParquet(String id, String identifier, Long snapshotID, String origin,
                                          String setSpec, String metadataPrefix, String networkAcronym,
                                          String repositoryName, String institutionName, Boolean isValid,
                                          Boolean isTransformed, Map<String, List<String>> validOccurrencesByRuleID,
                                          Map<String, List<String>> invalidOccurrencesByRuleID,
                                          List<String> validRulesIDList, List<String> invalidRulesIDList) {
        this.id = id;
        this.identifier = identifier;
        this.snapshotID = snapshotID;
        this.origin = origin;
        this.setSpec = setSpec;
        this.metadataPrefix = metadataPrefix;
        this.networkAcronym = networkAcronym;
        this.repositoryName = repositoryName;
        this.institutionName = institutionName;
        this.isValid = isValid;
        this.isTransformed = isTransformed;
        
        // Serializar los mapas y listas a JSON strings
        this.validOccurrencesByRuleIDJson = serializeMapToJson(validOccurrencesByRuleID);
        this.invalidOccurrencesByRuleIDJson = serializeMapToJson(invalidOccurrencesByRuleID);
        this.validRulesID = String.join(",", validRulesIDList != null ? validRulesIDList : new ArrayList<>());
        this.invalidRulesID = String.join(",", invalidRulesIDList != null ? invalidRulesIDList : new ArrayList<>());
        
        // Mantener referencias transientes
        this.validOccurrencesByRuleID = validOccurrencesByRuleID;
        this.invalidOccurrencesByRuleID = invalidOccurrencesByRuleID;
        this.validRulesIDList = validRulesIDList;
        this.invalidRulesIDList = invalidRulesIDList;
    }
    
    /**
     * Obtiene el mapa de ocurrencias válidas deserializando del JSON
     */
    public Map<String, List<String>> getValidOccurrencesByRuleID() {
        if (validOccurrencesByRuleID == null && validOccurrencesByRuleIDJson != null) {
            validOccurrencesByRuleID = deserializeMapFromJson(validOccurrencesByRuleIDJson);
        }
        return validOccurrencesByRuleID != null ? validOccurrencesByRuleID : new HashMap<>();
    }
    
    /**
     * Obtiene el mapa de ocurrencias inválidas deserializando del JSON
     */
    public Map<String, List<String>> getInvalidOccurrencesByRuleID() {
        if (invalidOccurrencesByRuleID == null && invalidOccurrencesByRuleIDJson != null) {
            invalidOccurrencesByRuleID = deserializeMapFromJson(invalidOccurrencesByRuleIDJson);
        }
        return invalidOccurrencesByRuleID != null ? invalidOccurrencesByRuleID : new HashMap<>();
    }
    
    /**
     * Obtiene la lista de reglas válidas deserializando del string
     */
    public List<String> getValidRulesIDList() {
        if (validRulesIDList == null && validRulesID != null && !validRulesID.trim().isEmpty()) {
            validRulesIDList = List.of(validRulesID.split(","));
        }
        return validRulesIDList != null ? validRulesIDList : new ArrayList<>();
    }
    
    /**
     * Obtiene la lista de reglas inválidas deserializando del string
     */
    public List<String> getInvalidRulesIDList() {
        if (invalidRulesIDList == null && invalidRulesID != null && !invalidRulesID.trim().isEmpty()) {
            invalidRulesIDList = List.of(invalidRulesID.split(","));
        }
        return invalidRulesIDList != null ? invalidRulesIDList : new ArrayList<>();
    }
    
    /**
     * Serializa un mapa a JSON simple (formato clave:valor1,valor2;clave2:valor3,valor4)
     */
    private String serializeMapToJson(Map<String, List<String>> map) {
        if (map == null || map.isEmpty()) {
            return "";
        }
        
        StringBuilder sb = new StringBuilder();
        for (Map.Entry<String, List<String>> entry : map.entrySet()) {
            if (sb.length() > 0) {
                sb.append(";");
            }
            sb.append(entry.getKey()).append(":");
            if (entry.getValue() != null && !entry.getValue().isEmpty()) {
                sb.append(String.join(",", entry.getValue()));
            }
        }
        return sb.toString();
    }
    
    /**
     * Deserializa un string JSON a mapa
     */
    private Map<String, List<String>> deserializeMapFromJson(String json) {
        Map<String, List<String>> map = new HashMap<>();
        if (json == null || json.trim().isEmpty()) {
            return map;
        }
        
        String[] entries = json.split(";");
        for (String entry : entries) {
            String[] keyValue = entry.split(":", 2);
            if (keyValue.length == 2) {
                String key = keyValue[0];
                String[] values = keyValue[1].split(",");
                List<String> valueList = new ArrayList<>();
                for (String value : values) {
                    if (!value.trim().isEmpty()) {
                        valueList.add(value.trim());
                    }
                }
                map.put(key, valueList);
            }
        }
        return map;
    }
}
