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
package org.lareferencia.backend.domain;

import lombok.Getter;
import lombok.Setter;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * COMPATIBILITY CLASS for dashboard-rest module
 * This class maintains the original Solr-based interface for ValidationStatObservation
 * to ensure dashboard-rest continues working without major changes.
 * 
 * This is a temporary compatibility layer during the transition to the new
 * org.lareferencia.backend.domain.validation.ValidationStatObservation structure.
 * 
 * NOTE: Solr annotations removed to avoid dependency issues during compilation.
 * This class is used only for internal API compatibility.
 */
@Getter
@Setter
public class ValidationStatObservation {
	
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
	private Map<String, List<String>> validOccurrencesByRuleID;
	private Map<String, List<String>> invalidOccurrencesByRuleID;
	private List<String> validRulesID;
	private List<String> invalidRulesID;

	/**
	 * Default constructor that initializes collections for rule tracking.
	 */
	public ValidationStatObservation() {
		super();
		
		validOccurrencesByRuleID = new HashMap<String, List<String>>();
		invalidOccurrencesByRuleID = new HashMap<String, List<String>>();
		validRulesID = new ArrayList<String>();
		invalidRulesID = new ArrayList<String>();
	}

}
