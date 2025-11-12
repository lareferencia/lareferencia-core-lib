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

package org.lareferencia.core.metadata;

import lombok.Data;
import lombok.NoArgsConstructor;
import org.lareferencia.core.domain.Network;

import java.util.*;

/**
 * Simple class to accumulate snapshot validation metadata.
 * Contains general snapshot information and references to validation statistics.
 */
@Data
@NoArgsConstructor
public class SnapshotMetadata {
    
    private Long snapshotId;
    
    private Integer size;
    private Integer transformedSize;
    private Integer validSize;
    
    private Long createdAt;
    
    // Snapshot origin information
    private String origin;
    private String setSpec;
    private String metadataPrefix;
    private String networkAcronym;
    
    // Network reference
    private Network network;
    
    // Rule definitions: ruleID -> rule information
    private Map<Long, RuleDefinition> ruleDefinitions = new LinkedHashMap<>();
    
    public SnapshotMetadata(Long snapshotId) {
        this.snapshotId = snapshotId;
        this.size = 0;
        this.transformedSize = 0;
        this.validSize = 0;
        this.createdAt = System.currentTimeMillis();
    }
    
    /**
     * Inner class for rule definition/metadata (immutable once created)
     */
    @Data
    @NoArgsConstructor
    public static class RuleDefinition {
        private Long ruleID;
        private String name;
        private String description;
        private String quantifier;
        private Boolean mandatory;
        
        public RuleDefinition(Long ruleID, String name, String description, String quantifier, Boolean mandatory) {
            this.ruleID = ruleID;
            this.name = name;
            this.description = description;
            this.quantifier = quantifier;
            this.mandatory = mandatory;
        }
    }
    
    // Counter increment methods
    public void incrementSize() { this.size++; }
    public void incrementValidSize() { this.validSize++; }
    public void incrementTransformedSize() { this.transformedSize++; }
    
    // Rule management methods
    public void registerRule(Long ruleID, String name, String description, String quantifier, Boolean mandatory) {
        if (!ruleDefinitions.containsKey(ruleID)) {
            ruleDefinitions.put(ruleID, new RuleDefinition(ruleID, name, description, quantifier, mandatory));
        }
    }
    
    public RuleDefinition getRuleDefinition(Long ruleID) {
        return ruleDefinitions.get(ruleID);
    }
    
    // Calculated field for invalid records
    public int getInvalidSize() { 
        return (size != null && validSize != null) ? size - validSize : 0; 
    }
}
