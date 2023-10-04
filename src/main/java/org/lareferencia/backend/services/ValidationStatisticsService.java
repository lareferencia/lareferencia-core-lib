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

package org.lareferencia.backend.services;

import java.util.*;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.solr.client.solrj.response.SolrPingResponse;
import org.lareferencia.backend.domain.NetworkSnapshot;
import org.lareferencia.backend.domain.OAIRecord;
import org.lareferencia.backend.domain.ValidationStatObservation;
import org.lareferencia.backend.domain.ValidatorRule;
import org.lareferencia.backend.repositories.solr.ValidationStatRepository;
import org.lareferencia.backend.validation.validator.ContentValidatorResult;
import org.lareferencia.core.metadata.IMetadataRecordStoreService;
import org.lareferencia.core.metadata.OAIMetadataBitstream;
import org.lareferencia.core.metadata.OAIRecordMetadata;
import org.lareferencia.core.validation.QuantifierValues;
import org.lareferencia.core.validation.ValidatorResult;
import org.lareferencia.core.validation.ValidatorRuleResult;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Scope;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.solr.core.SolrTemplate;
import org.springframework.data.solr.core.query.FacetOptions;
import org.springframework.data.solr.core.query.FacetOptions.FacetSort;
import org.springframework.data.solr.core.query.FacetQuery;
import org.springframework.data.solr.core.query.Query;
import org.springframework.data.solr.core.query.SimpleFacetQuery;
import org.springframework.data.solr.core.query.SimpleFilterQuery;
import org.springframework.data.solr.core.query.SimpleQuery;
import org.springframework.data.solr.core.query.SimpleStringCriteria;
import org.springframework.data.solr.core.query.result.FacetFieldEntry;
import org.springframework.data.solr.core.query.result.FacetPage;
import org.springframework.stereotype.Component;

import lombok.Getter;
import lombok.Setter;

@Component
@Scope("prototype")
public class ValidationStatisticsService {
	
	private static Logger logger = LogManager.getLogger(ValidationStatisticsService.class);

	@Autowired
	@Qualifier("validationSolrTemplate")
	private SolrTemplate validationSolrTemplate;
	
	@Autowired
	@Qualifier("validationSolrCoreName")
	private String validationCoreName;

	
	public static final String[] FACET_FIELDS = { "record_is_valid", "record_is_transformed", "valid_rules", "invalid_rules", "institution_name", "repository_name" };
	public static final String SNAPSHOT_ID_FIELD = "snapshot_id";
	public static final String INVALID_RULE_SUFFIX = "_rule_invalid_occrs";
	public static final String VALID_RULE_SUFFIX = "_rule_valid_occrs";

		
	@Value("${reponame.fieldname}")
	private String repositoryFieldName;
	
	@Value("${reponame.prefix}")
	private String repositoryPrefix;
	
	@Value("${instname.fieldname}")
	private String institutionFieldName;
	
	@Value("${instname.prefix}")
	private String institutionPrefix;
	
	@Autowired
	ValidationStatRepository validationStatRepository;
	
	@Autowired
	IMetadataRecordStoreService metadataStoreService;
	
	@Setter
	@Getter
	boolean detailedDiagnose = false;
	
	
	
	
	/**
	 * Se construye un resultado de validación persistible en solr a partir del
	 * objeto resultado devuelto por el validador para un registro
	 * 
	 * @param result
	 */
	public ValidationStatObservation buildObservation(OAIRecord record, ValidatorResult validationResult) {

		ValidationStatObservation obs = new ValidationStatObservation();

		logger.debug("Building validation result record ID: " +  record.getId().toString() );

		obs.setId( record.getSnapshot().getId() + "-"+ record.getId().toString() );

		obs.setIdentifier( record.getIdentifier() );
		obs.setOrigin( record.getSnapshot().getNetwork().getOriginURL() );
		obs.setMetadataPrefix( record.getSnapshot().getNetwork().getMetadataPrefix() );
		//obs.setSetSpec( metadata.getSetSpec() );
		obs.setIsTransformed( record.getTransformed() );
		
		//obs.setRepositoryName( metadata.getFieldPrefixedContent(repositoryFieldName, repositoryPrefix) );
		//obs.setInstitutionName( metadata.getFieldPrefixedContent(institutionFieldName, institutionPrefix) );

		obs.setSnapshotID( record.getSnapshot().getId() );
		obs.setNetworkAcronym( record.getSnapshot().getNetwork().getAcronym() );

		obs.setIsValid( validationResult.isValid() );

		for (ValidatorRuleResult ruleResult : validationResult.getRulesResults()) {

			String ruleID = ruleResult.getRule().getRuleId().toString();

			List<String> invalidOccr = new ArrayList<String>();
			List<String> validOccr = new ArrayList<String>();
			
			
			if ( detailedDiagnose ) { // solo se recolectan casos de ocurrencias cuando el diagnostico es detallado
				logger.debug("Detailed validation report - Rule ID: " + ruleID );


				for (ContentValidatorResult contentResult : ruleResult.getResults()) {
	
					if (contentResult.isValid())
						validOccr.add(contentResult.getReceivedValue());
					else
						invalidOccr.add(contentResult.getReceivedValue());
				}
				
			 	obs.getValidOccurrencesByRuleID().put(ruleID, validOccr);
				obs.getInvalidOccurrencesByRuleID().put(ruleID, invalidOccr);
			
			}

			// agregado de las reglas validas e invalidas
			if (ruleResult.getValid())
				obs.getValidRulesID().add(ruleID);
			else
				obs.getInvalidRulesID().add(ruleID);


		}
		
		return obs;
	}
	
	
	public void registerObservations(List<ValidationStatObservation> validationStatsObservations) {
		
		try {
			validationStatRepository.saveAll(validationStatsObservations);
		} catch (Exception e) {
			logger.error( "Error en regitro de stats (solr): " + e.getMessage() );
		}
	}
	
	
	public ValidationStats queryValidatorRulesStatsBySnapshot(NetworkSnapshot snapshot, List<String> fq) throws Exception {

		ValidationStats result = new ValidationStats();

		// Prepara una query basada en el snapshot_id y las facet queries fq parámetro
		FacetQuery facetQuery = new SimpleFacetQuery(new SimpleStringCriteria(SNAPSHOT_ID_FIELD + ":" + snapshot.getId()));

		// se agregan los criterios basados en las fq
		for (String fqTerm : fq) {
			fqTerm = fqTerm.replace("@@", ":"); // las fq estan separadas por @@ por problemas con url encoding, se restablece el separador de query solr :
			facetQuery.addFilterQuery(new SimpleFilterQuery(new SimpleStringCriteria(fqTerm)));
		}

		// no se necesitan los resultados sino las reglas y sus valores de frecuencias
		facetQuery.setRows(0);

		// se limitan las opciones y se devuelven solo las facetas con al menos 1 caso
		FacetOptions facetOptions = new FacetOptions();
		facetOptions.setFacetMinCount(1);
		facetOptions.setFacetLimit(1000);

		// se agrega las opciones para obtener facetas sobre cada uno de los campos de facetas
		for (String facetName : FACET_FIELDS)
			facetOptions.addFacetOnField(facetName);

		facetQuery.setFacetOptions(facetOptions);

		// Consulta SOLR
		FacetPage<ValidationStatObservation> facetResult = validationSolrTemplate.queryForFacetPage(validationCoreName,facetQuery, ValidationStatObservation.class);

		// se transforma el formato devuelto por solr a un formato de map<string, integer> para mostrar los conteos
		Map<String, Integer> validRuleMap = obtainFacetMap(facetResult.getFacetResultPage("valid_rules").getContent());
		Map<String, Integer> invalidRuleMap = obtainFacetMap(facetResult.getFacetResultPage("invalid_rules").getContent());
		Map<String, Integer> validRecordMap = obtainFacetMap(facetResult.getFacetResultPage("record_is_valid").getContent());
		Map<String, Integer> transformedRecordMap = obtainFacetMap(facetResult.getFacetResultPage("record_is_transformed").getContent());

		result.size = (int) facetResult.getTotalElements();
		result.validSize = 0;
		result.transformedSize = 0;

		if (validRecordMap.get("true") != null)
			result.validSize = validRecordMap.get("true");

		if (transformedRecordMap.get("true") != null)
			result.transformedSize = transformedRecordMap.get("true");

		for (String facetName : FACET_FIELDS)
			result.facets.put(facetName, facetResult.getFacetResultPage(facetName).getContent());

		
		if (snapshot.getNetwork().getValidator() != null ) {
			for (ValidatorRule rule : snapshot.getNetwork().getValidator().getRules() ) {
	
				String ruleID = rule.getId().toString();
	
				ValidationRuleStat ruleResult = new ValidationRuleStat();
	
				ruleResult.ruleID = rule.getId();
				ruleResult.validCount = validRuleMap.get(ruleID);
				ruleResult.invalidCount = invalidRuleMap.get(ruleID);
				ruleResult.name = rule.getName();
				ruleResult.description = rule.getDescription();
				ruleResult.mandatory = rule.getMandatory();
				ruleResult.quantifier = rule.getQuantifier();
	
				result.rulesByID.put(ruleID, ruleResult);
	
			}
		}

		return result;
	}
	
	@Getter
	@Setter
	public class ValidationStats {

		public ValidationStats() {
			facets = new HashMap<String, List<FacetFieldEntry>>();
			rulesByID = new HashMap<String, ValidationRuleStat>();
		}

		Integer size;
		Integer transformedSize;
		Integer validSize;
		Map<String, ValidationRuleStat> rulesByID;
		Map<String, List<FacetFieldEntry>> facets;
	}
	
	@Getter
	@Setter
	public class ValidationRuleStat {
		Long ruleID;
		String name;
		String description;
		QuantifierValues quantifier;
		Boolean mandatory;
		Integer validCount;
		Integer invalidCount;
	}
	
	
	public boolean isServiceAvaliable() {
		
		
		try {
			SolrPingResponse ping = validationSolrTemplate.ping(validationCoreName);
			logger.debug( "Diagnose core - vstats - SOLR - Status: " + ((ping.getStatus() == 0) ? "OK" : "FAIL")  );

		} 
		catch ( Exception e) {
			logger.error( "Diagnose core:vstats - SOLR ERROR: " + e.getMessage());
			return false;
		}
		
		return true;
	}
	
	
	public ValidationRuleOccurrencesCount queryValidRuleOcurrencesCountBySnapshotID(Long snapshotID, Long ruleID, List<String> fq)  {

		
		ValidationRuleOccurrencesCount result = new ValidationRuleOccurrencesCount();

		FacetQuery facetQuery = new SimpleFacetQuery(new SimpleStringCriteria(SNAPSHOT_ID_FIELD + ":" + snapshotID));
		facetQuery.setRows(0);

		for (String fqTerm : fq) {
			fqTerm = fqTerm.replace("@@", ":");
			facetQuery.addFilterQuery(new SimpleFilterQuery(new SimpleStringCriteria(fqTerm)));
		}

		FacetOptions facetOptions = new FacetOptions();
		facetOptions.setFacetMinCount(1);
		facetOptions.setFacetLimit(1000);

		facetOptions.addFacetOnField(ruleID.toString() + INVALID_RULE_SUFFIX);
		facetOptions.addFacetOnField(ruleID.toString() + VALID_RULE_SUFFIX);

		facetOptions.setFacetSort(FacetSort.COUNT);

		facetQuery.setFacetOptions(facetOptions);

		FacetPage<ValidationStatObservation> facetResult = validationSolrTemplate.queryForFacetPage(validationCoreName,facetQuery, ValidationStatObservation.class);

		List<OccurrenceCount> validRuleOccurrence = new ArrayList<OccurrenceCount>();
		List<OccurrenceCount> invalidRuleOccurrence = new ArrayList<OccurrenceCount>();

		for (FacetFieldEntry occr : facetResult.getFacetResultPage(ruleID.toString() + VALID_RULE_SUFFIX).getContent())
			validRuleOccurrence.add(new OccurrenceCount(occr.getValue(), (int) occr.getValueCount()));

		for (FacetFieldEntry occr : facetResult.getFacetResultPage(ruleID.toString() + INVALID_RULE_SUFFIX).getContent())
			invalidRuleOccurrence.add(new OccurrenceCount(occr.getValue(), (int) occr.getValueCount()));

		result.setValidRuleOccrs(validRuleOccurrence);
		result.setInvalidRuleOccrs(invalidRuleOccurrence);

		return result;
	}
	
	
	@Getter
	@Setter
	public class ValidationRuleOccurrencesCount {
		List<OccurrenceCount> invalidRuleOccrs;
		List<OccurrenceCount> validRuleOccrs;
	}
	
	@Getter
	@Setter
	public class OccurrenceCount {
		public OccurrenceCount(String value, int count) {
			super();
			this.value = value;
			this.count = count;
		}

		String value;
		Integer count;
	}
	
	
	public Page<ValidationStatObservation> queryValidationStatsObservationsBySnapshotID(Long snapshotID, List<String> fq, Pageable pageable) {

		Query query = new SimpleQuery(SNAPSHOT_ID_FIELD + ":" + snapshotID.toString());

		// Esta correccion permite pagiona
		pageable = pageable.previousOrFirst();

		query.setPageRequest(pageable);

		for (String fqTerm : fq) {
			fqTerm = fqTerm.replace("@@", ":");
			query.addFilterQuery(new SimpleFilterQuery(new SimpleStringCriteria(fqTerm)));
		}

		Page<ValidationStatObservation> results = validationSolrTemplate.queryForPage(validationCoreName,query, ValidationStatObservation.class);

		return results;
	}



	/**
	 * deleteValidationStatsObservationsBySnapshotID
	 * @param snapshotID
	 * @param recordIDs
	 */
	public void deleteValidationStatsObservationsBySnapshotID(Long snapshotID,  Collection<Long> recordIDs) throws ValidationStatisticsException {

		for (Long recordID : recordIDs) {
			try {
				validationStatRepository.deleteById(snapshotID.toString() + "-" + recordID.toString());
			} catch (Exception e) {
				throw new ValidationStatisticsException("Error deleting validation info | snapahot:" + snapshotID + " recordID:" + recordID + " :: " + e.getMessage());
			}
		}
	}

	public void deleteValidationStatsObservationBySnapshotID(Long snapshotID,  Long recordID) throws ValidationStatisticsException {

			try {
				validationStatRepository.deleteById(snapshotID.toString() + "-" + recordID.toString());
			} catch (Exception e) {
				throw new ValidationStatisticsException("Error deleting validation info | snapahot:" + snapshotID + " recordID:" + recordID + " :: " + e.getMessage());
			}

	}

	/** Copy ValidationStatObservation from an existing snapshotID to another **/
	public boolean copyValidationStatsObservationsFromTo(Long originalSnapshotId, Long newSnapshotId) {

		int pageSize = 1000;
		Long pageNumber = 0L;

		Query baseQuery = new SimpleQuery(SNAPSHOT_ID_FIELD + ":" + originalSnapshotId.toString());
		Query query = baseQuery.setRows(pageSize).setOffset(pageNumber * pageSize);

		// query the first page
		Page<ValidationStatObservation> page = validationSolrTemplate.queryForPage(validationCoreName,query,ValidationStatObservation.class);

		// while there are results
		while ( page.hasContent() ) {

			// for each result update the snapshotID
			for ( ValidationStatObservation obs : page.getContent() ) {

				// parse first part of the id separated by -
				String[] parts = obs.getId().split("-");

				// if there is a second part, use it, otherwise use the first part (backward compatibility)
				if ( parts.length >= 2 ) {
					obs.setId( newSnapshotId + "-" + parts[1] );
				} else {
					obs.setId( newSnapshotId + "-" + obs.getId());
				}

				// update the snapshotID
				obs.setSnapshotID(newSnapshotId);
			}

			// save the page
			validationStatRepository.saveAll(page.getContent());

			// query the next page

			pageNumber++;
			query = baseQuery.setRows(pageSize).setOffset(pageNumber * pageSize);
			page = validationSolrTemplate.queryForPage(validationCoreName,query, ValidationStatObservation.class);
		}

		return false;
	}
	
	
	
	/**
	 * Retorna un Map entre los ids y los nombres de las reglas
	 */
	private Map<String, Integer> obtainFacetMap(List<FacetFieldEntry> facetList) {

		Map<String, Integer> facetMap = new HashMap<String, Integer>();

		for (FacetFieldEntry entry : facetList)
			facetMap.put(entry.getValue(), (int) entry.getValueCount());

		return facetMap;
	}
	
	public void deleteValidationStatsBySnapshotID(Long snapshotID) throws ValidationStatisticsException {
		
		try {
			Query query = new SimpleQuery(SNAPSHOT_ID_FIELD + ":" + snapshotID.toString());
			validationSolrTemplate.delete(validationCoreName,query);
		} catch (Exception e) {
			throw new ValidationStatisticsException("Error deleting validation info | snapahot:" + snapshotID  + " :: " + e.getMessage());
		}
		
	}



}
