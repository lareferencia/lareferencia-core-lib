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

package org.lareferencia.core.service.validation;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.lareferencia.core.repository.parquet.SnapshotValidationStats;
import org.lareferencia.core.metadata.SnapshotMetadata;
import org.lareferencia.core.worker.validation.QuantifierValues;

import lombok.Getter;
import lombok.Setter;

/**
 * Class representing validation statistics for a snapshot.
 * Contains counts of total, valid, and transformed records,
 * plus detailed rule statistics and facet information.
 */
@Getter
@Setter
public class ValidationStatsResult {

	/**
	 * Constructs a new ValidationStatsResult with empty facets and rules maps.
	 */
    public ValidationStatsResult() {
        facets = new HashMap<>();
        rulesByID = new HashMap<>();
    }

	/** Total number of records in the snapshot */
    Integer size;
    /** Number of records that were transformed */
    Integer transformedSize;
    /** Number of records that passed validation */
    Integer validSize;
    /** Map of validation rule statistics by rule ID */
    Map<String, ValidationRuleStat> rulesByID;
    /** Map of faceted field entries for search and filtering */
    Map<String, List<FacetFieldEntry>> facets;
    
    /**
     * Creates a ValidationStatsResult from a SnapshotValidationStats instance.
     * Converts the internal domain structure to the API result format.
     * 
     * @param snapshotStats the snapshot validation statistics to convert
     * @return a new ValidationStatsResult with converted data
     */
    public static ValidationStatsResult fromSnapshotValidationStats(SnapshotValidationStats snapshotStats) {
        ValidationStatsResult result = new ValidationStatsResult();
        
        // Convert basic counts
        result.setSize(snapshotStats.getTotalRecords() != null ? snapshotStats.getTotalRecords() : 0);
        result.setTransformedSize(snapshotStats.getTransformedRecords() != null ? snapshotStats.getTransformedRecords() : 0);
        result.setValidSize(snapshotStats.getValidRecords() != null ? snapshotStats.getValidRecords() : 0);
        
        // Convert rules statistics
        Map<String, ValidationRuleStat> rulesByID = new HashMap<>();
        SnapshotMetadata metadata = snapshotStats.getSnapshotMetadata();
        
        if (metadata != null && metadata.getRuleDefinitions() != null) {
            for (Map.Entry<Long, SnapshotMetadata.RuleDefinition> entry : metadata.getRuleDefinitions().entrySet()) {
                Long ruleID = entry.getKey();
                SnapshotMetadata.RuleDefinition ruleDef = entry.getValue();
                SnapshotValidationStats.RuleStats ruleStats = snapshotStats.getRuleStats(ruleID);
                
                ValidationRuleStat ruleStat = new ValidationRuleStat();
                ruleStat.setRuleID(ruleID.longValue());
                ruleStat.setName(ruleDef.getName());
                ruleStat.setDescription(ruleDef.getDescription());
                ruleStat.setQuantifier(QuantifierValues.valueOf(ruleDef.getQuantifier()));
                ruleStat.setMandatory(ruleDef.getMandatory());
                ruleStat.setValidCount(ruleStats != null ? ruleStats.getValidCount().intValue() : 0);
                ruleStat.setInvalidCount(ruleStats != null ? ruleStats.getInvalidCount().intValue() : null);
                
                rulesByID.put(ruleID.toString(), ruleStat);
            }
        }
        
        result.setRulesByID(rulesByID);
        
        // Convert facets
        Map<String, List<FacetFieldEntry>> facets = new HashMap<>();
        
        for (Map.Entry<String, Map<String, Long>> facetEntry : snapshotStats.getFacets().entrySet()) {
            String facetName = facetEntry.getKey();
            Map<String, Long> facetValues = facetEntry.getValue();
            
            List<FacetFieldEntry> facetEntries = new ArrayList<>();
            for (Map.Entry<String, Long> valueEntry : facetValues.entrySet()) {
                facetEntries.add(new FacetFieldEntry(valueEntry.getKey(), valueEntry.getValue(), facetName));
            }
            
            facets.put(facetName, facetEntries);
        }
        
        result.setFacets(facets);
        
        return result;
    }
}
