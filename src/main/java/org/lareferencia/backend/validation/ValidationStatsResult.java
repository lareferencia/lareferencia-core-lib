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

package org.lareferencia.backend.validation;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
}
