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

package org.lareferencia.core.repository.validation;

import lombok.Data;
import lombok.NoArgsConstructor;
import java.util.*;

import org.lareferencia.core.metadata.SnapshotMetadata;

import com.fasterxml.jackson.annotation.JsonIgnore;

/**
 * Class to accumulate snapshot validation statistics and facets.
 * Contains rule statistics and facet data (rule definitions are in
 * SnapshotMetadata).
 */
@Data
@NoArgsConstructor
public class SnapshotValidationStats {

    // Reference to parent metadata for projection
    private SnapshotMetadata snapshotMetadata;

    // Record counts
    private Integer totalRecords = 0;
    private Integer transformedRecords = 0;
    private Integer validRecords = 0;

    // Rule statistics: ruleID -> counters
    private Map<Long, RuleStats> ruleStats = new LinkedHashMap<>();

    // Facets map: facetName -> Map<value, counter>
    private Map<String, Map<String, Long>> facets = new LinkedHashMap<>();

    /**
     * Constructor with metadata reference
     */
    public SnapshotValidationStats(SnapshotMetadata snapshotMetadata) {
        this.snapshotMetadata = snapshotMetadata;

        // Initialize ruleStats map based on metadata rule definitions
        for (Map.Entry<Long, SnapshotMetadata.RuleDefinition> entry : snapshotMetadata.getRuleDefinitions()
                .entrySet()) {
            Long ruleID = entry.getKey();
            ruleStats.put(ruleID, new RuleStats());
        }
    }

    /**
     * Inner class for rule statistics/counters (mutable)
     */
    @Data
    public static class RuleStats {
        private Long validCount = 0L;
        private Long invalidCount = 0L;

        public void incrementValid() {
            this.validCount++;
        }

        public void incrementInvalid() {
            this.invalidCount++;
        }
    }

    // Record count increment methods
    public void incrementTotalRecords() {
        this.totalRecords++;
    }

    public void incrementTransformedRecords() {
        this.transformedRecords++;
    }

    public void incrementValidRecords() {
        this.validRecords++;
    }

    // Calculated invalid records - not serialized to avoid deserialization issues
    @JsonIgnore
    public Integer getInvalidRecords() {
        return (totalRecords != null && validRecords != null) ? totalRecords - validRecords : 0;
    }

    public void incrementRuleValid(Long ruleID) {
        // Create stats if missing and increment (safer for callers that may not
        // register beforehand)
        ruleStats.computeIfAbsent(ruleID, k -> new RuleStats()).incrementValid();
    }

    public void incrementRuleInvalid(Long ruleID) {
        // Create stats if missing and increment
        ruleStats.computeIfAbsent(ruleID, k -> new RuleStats()).incrementInvalid();
    }

    public RuleStats getRuleStats(Long ruleID) {
        return ruleStats.get(ruleID);
    }

    /**
     * Registers a rule ID (creates empty stats if not exists).
     */
    public void registerRule(Long ruleID) {
        ruleStats.computeIfAbsent(ruleID, k -> new RuleStats());
    }

    // Facet management methods
    public void updateFacet(String facetName, String value) {
        Map<String, Long> facetValues = facets.computeIfAbsent(facetName, k -> new LinkedHashMap<>());
        facetValues.put(value, facetValues.getOrDefault(value, 0L) + 1);
    }

    /**
     * Updates a facet with a specific count (for SQL-based aggregation).
     */
    public void updateFacet(String facetName, String value, long count) {
        Map<String, Long> facetValues = facets.computeIfAbsent(facetName, k -> new LinkedHashMap<>());
        facetValues.put(value, facetValues.getOrDefault(value, 0L) + count);
    }

    public Map<String, Long> getFacet(String facetName) {
        return facets.get(facetName);
    }

    /**
     * Updates statistics using data from a validation record.
     * Centralized method to avoid logic duplication.
     * 
     * @param record the validation record with its results
     */
    public void updateFromRecord(ValidationRecord record) {
        incrementTotalRecords();

        if (record.isValid()) {
            incrementValidRecords();
        }

        if (record.isTransformed()) {
            incrementTransformedRecords();
        }

        updateFacet("record_is_valid", String.valueOf(record.isValid()));
        updateFacet("record_is_transformed", String.valueOf(record.isTransformed()));

        if (record.getRuleResults() != null) {
            for (Map.Entry<Long, Boolean> entry : record.getRuleResults().entrySet()) {
                Long ruleID = entry.getKey();
                String ruleIdStr = ruleID.toString();
                boolean isValid = entry.getValue();

                if (isValid) {
                    incrementRuleValid(ruleID);
                    updateFacet("valid_rules", ruleIdStr);
                } else {
                    incrementRuleInvalid(ruleID);
                    updateFacet("invalid_rules", ruleIdStr);
                }
            }
        }
    }
}
