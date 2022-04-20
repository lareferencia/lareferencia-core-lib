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

package org.lareferencia.backend.domain;

import lombok.Getter;
import lombok.Setter;
import org.apache.solr.client.solrj.beans.Field;
import org.lareferencia.backend.services.ValidationStatisticsService;
import org.springframework.data.annotation.Id;
import org.springframework.data.solr.core.mapping.Dynamic;
import org.springframework.data.solr.core.mapping.SolrDocument;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


//TODO solrCoreName is set in two different places and need to be changed in both to work correctly

@Getter
@Setter
@SolrDocument(collection="vstats")
public class ValidationStatObservation {
	
	
	@Id
	@Field
	private String id;

	@Field("oai_identifier")
	private String identifier;

	@Field(ValidationStatisticsService.SNAPSHOT_ID_FIELD)
	private Long snapshotID;

	@Field("origin")
	private String origin;

	@Field("set_spec")
	private String setSpec;
	
	@Field("metadata_prefix")
	private String metadataPrefix;

	@Field("network_acronym")
	private String networkAcronym;

	@Field("repository_name")
	private String repositoryName;

	@Field("institution_name")
	private String institutionName;

	@Field("record_is_valid")
	private Boolean isValid;

	@Field("record_is_transformed")
	private Boolean isTransformed;

	@Dynamic
	@Field("*" + ValidationStatisticsService.VALID_RULE_SUFFIX)
	private Map<String, List<String>> validOccurrencesByRuleID;

	@Dynamic
	@Field("*" + ValidationStatisticsService.INVALID_RULE_SUFFIX)
	private Map<String, List<String>> invalidOccurrencesByRuleID;

	@Field("valid_rules")
	private List<String> validRulesID;

	@Field("invalid_rules")
	private List<String> invalidRulesID;

	
	public ValidationStatObservation() {
		super();
		
		validOccurrencesByRuleID = new HashMap<String, List<String>>();
		invalidOccurrencesByRuleID = new HashMap<String, List<String>>();
		validRulesID = new ArrayList<String>();
		invalidRulesID = new ArrayList<String>();
	}

}