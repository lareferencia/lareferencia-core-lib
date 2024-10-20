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

// import java.util.*;

// import org.apache.logging.log4j.LogManager;
// import org.apache.logging.log4j.Logger;
// import org.apache.solr.client.solrj.response.SolrPingResponse;
// import org.lareferencia.backend.domain.NetworkSnapshot;
// import org.lareferencia.backend.domain.OAIRecord;
// import org.lareferencia.backend.domain.ValidationStatObservation;
// import org.lareferencia.backend.domain.ValidatorRule;
// import org.lareferencia.backend.repositories.solr.ValidationStatRepository;
// import org.lareferencia.backend.validation.validator.ContentValidatorResult;
// import org.lareferencia.core.metadata.IMetadataRecordStoreService;
// import org.lareferencia.core.util.IRecordFingerprintHelper;
// import org.lareferencia.core.validation.QuantifierValues;
// import org.lareferencia.core.validation.ValidatorResult;
// import org.lareferencia.core.validation.ValidatorRuleResult;
// import org.springframework.beans.factory.annotation.Autowired;
// import org.springframework.beans.factory.annotation.Qualifier;
// import org.springframework.beans.factory.annotation.Value;
// import org.springframework.context.annotation.Scope;
// import org.springframework.data.domain.Page;
// import org.springframework.data.domain.Pageable;
// import org.springframework.data.solr.core.SolrTemplate;
// import org.springframework.data.solr.core.query.FacetOptions;
// import org.springframework.data.solr.core.query.FacetOptions.FacetSort;
// import org.springframework.data.solr.core.query.FacetQuery;
// import org.springframework.data.solr.core.query.Query;
// import org.springframework.data.solr.core.query.SimpleFacetQuery;
// import org.springframework.data.solr.core.query.SimpleFilterQuery;
// import org.springframework.data.solr.core.query.SimpleQuery;
// import org.springframework.data.solr.core.query.SimpleStringCriteria;
// import org.springframework.data.solr.core.query.result.FacetFieldEntry;
// import org.springframework.data.solr.core.query.result.FacetPage;
// import org.springframework.stereotype.Component;

// import lombok.Getter;
// import lombok.Setter;

// @Component
// @Scope("prototype")
// public class ValidationStatisticsService {
	
// 	private static Logger logger = LogManager.getLogger(ValidationStatisticsService.class);

// 	@Autowired
// 	@Qualifier("validationSolrTemplate")
// 	private SolrTemplate validationSolrTemplate;
	
// 	@Autowired
// 	@Qualifier("validationSolrCoreName")
// 	private String validationCoreName;

	
// 	public static final String[] FACET_FIELDS = { "record_is_valid", "record_is_transformed", "valid_rules", "invalid_rules", "institution_name", "repository_name" };
// 	public static final String SNAPSHOT_ID_FIELD = "snapshot_id";
// 	public static final String INVALID_RULE_SUFFIX = "_rule_invalid_occrs";
// 	public static final String VALID_RULE_SUFFIX = "_rule_valid_occrs";

		
// 	@Value("${reponame.fieldname}")
// 	private String repositoryFieldName;
	
// 	@Value("${reponame.prefix}")
// 	private String repositoryPrefix;
	
// 	@Value("${instname.fieldname}")
// 	private String institutionFieldName;
	
// 	@Value("${instname.prefix}")
// 	private String institutionPrefix;
	
// 	@Autowired
// 	ValidationStatRepository validationStatRepository;
	
// 	@Autowired
// 	IMetadataRecordStoreService metadataStoreService;
	
// 	@Setter
// 	@Getter
// 	boolean detailedDiagnose = false;

// 	@Autowired
// 	private IRecordFingerprintHelper fingerprintHelper;


	// /**
	//  * Se construye un resultado de validación persistible en solr a partir del
	//  * objeto resultado devuelto por el validador para un registro
	//  * 
	//  * @param validationResult
	//  */
	// public ValidationStatObservation buildObservation(OAIRecord record, ValidatorResult validationResult) {

	// 	ValidationStatObservation obs = new ValidationStatObservation();

	// 	logger.debug("Building validation result record ID: " +  record.getId().toString() );

	// 	obs.setId( fingerprintHelper.getStatsIDfromRecord(record) );

	// 	obs.setIdentifier( record.getIdentifier() );
	// 	obs.setOrigin( record.getSnapshot().getNetwork().getOriginURL() );
	// 	obs.setMetadataPrefix( record.getSnapshot().getNetwork().getMetadataPrefix() );
	// 	//obs.setSetSpec( metadata.getSetSpec() );
	// 	obs.setIsTransformed( record.getTransformed() );
		
	// 	//obs.setRepositoryName( metadata.getFieldPrefixedContent(repositoryFieldName, repositoryPrefix) );
	// 	//obs.setInstitutionName( metadata.getFieldPrefixedContent(institutionFieldName, institutionPrefix) );

	// 	obs.setSnapshotID( record.getSnapshot().getId() );
	// 	obs.setNetworkAcronym( record.getSnapshot().getNetwork().getAcronym() );

	// 	obs.setIsValid( validationResult.isValid() );

	// 	for (ValidatorRuleResult ruleResult : validationResult.getRulesResults()) {

	// 		String ruleID = ruleResult.getRule().getRuleId().toString();

	// 		List<String> invalidOccr = new ArrayList<String>();
	// 		List<String> validOccr = new ArrayList<String>();
			
			
	// 		if ( detailedDiagnose ) { // solo se recolectan casos de ocurrencias cuando el diagnostico es detallado
	// 			logger.debug("Detailed validation report - Rule ID: " + ruleID );


	// 			for (ContentValidatorResult contentResult : ruleResult.getResults()) {
	
	// 				if (contentResult.isValid())
	// 					validOccr.add(contentResult.getReceivedValue());
	// 				else
	// 					invalidOccr.add(contentResult.getReceivedValue());
	// 			}
				
	// 		 	obs.getValidOccurrencesByRuleID().put(ruleID, validOccr);
	// 			obs.getInvalidOccurrencesByRuleID().put(ruleID, invalidOccr);
			
	// 		}

	// 		// agregado de las reglas validas e invalidas
	// 		if (ruleResult.getValid())
	// 			obs.getValidRulesID().add(ruleID);
	// 		else
	// 			obs.getInvalidRulesID().add(ruleID);


	// 	}
		
	// 	return obs;
	// }
	
	
// 	public void registerObservations(List<ValidationStatObservation> validationStatsObservations) {
		
// 		try {
// 			validationStatRepository.saveAll(validationStatsObservations);
// 		} catch (Exception e) {
// 			logger.error( "Error en regitro de stats (solr): " + e.getMessage() );
// 		}
// 	}
	
	
// 	public ValidationStats queryValidatorRulesStatsBySnapshot(NetworkSnapshot snapshot, List<String> fq) throws Exception {

// 		ValidationStats result = new ValidationStats();

// 		// Prepara una query basada en el snapshot_id y las facet queries fq parámetro
// 		FacetQuery facetQuery = new SimpleFacetQuery(new SimpleStringCriteria(SNAPSHOT_ID_FIELD + ":" + snapshot.getId()));

// 		// se agregan los criterios basados en las fq
// 		for (String fqTerm : fq) {
// 			fqTerm = fqTerm.replace("@@", ":"); // las fq estan separadas por @@ por problemas con url encoding, se restablece el separador de query solr :
// 			facetQuery.addFilterQuery(new SimpleFilterQuery(new SimpleStringCriteria(fqTerm)));
// 		}

// 		// no se necesitan los resultados sino las reglas y sus valores de frecuencias
// 		facetQuery.setRows(0);

// 		// se limitan las opciones y se devuelven solo las facetas con al menos 1 caso
// 		FacetOptions facetOptions = new FacetOptions();
// 		facetOptions.setFacetMinCount(1);
// 		facetOptions.setFacetLimit(1000);

// 		// se agrega las opciones para obtener facetas sobre cada uno de los campos de facetas
// 		for (String facetName : FACET_FIELDS)
// 			facetOptions.addFacetOnField(facetName);

// 		facetQuery.setFacetOptions(facetOptions);

// 		// Consulta SOLR
// 		FacetPage<ValidationStatObservation> facetResult = validationSolrTemplate.queryForFacetPage(validationCoreName,facetQuery, ValidationStatObservation.class);

// 		// se transforma el formato devuelto por solr a un formato de map<string, integer> para mostrar los conteos
// 		Map<String, Integer> validRuleMap = obtainFacetMap(facetResult.getFacetResultPage("valid_rules").getContent());
// 		Map<String, Integer> invalidRuleMap = obtainFacetMap(facetResult.getFacetResultPage("invalid_rules").getContent());
// 		Map<String, Integer> validRecordMap = obtainFacetMap(facetResult.getFacetResultPage("record_is_valid").getContent());
// 		Map<String, Integer> transformedRecordMap = obtainFacetMap(facetResult.getFacetResultPage("record_is_transformed").getContent());

// 		result.size = (int) facetResult.getTotalElements();
// 		result.validSize = 0;
// 		result.transformedSize = 0;

// 		if (validRecordMap.get("true") != null)
// 			result.validSize = validRecordMap.get("true");

// 		if (transformedRecordMap.get("true") != null)
// 			result.transformedSize = transformedRecordMap.get("true");

// 		for (String facetName : FACET_FIELDS)
// 			result.facets.put(facetName, facetResult.getFacetResultPage(facetName).getContent());

		
// 		if (snapshot.getNetwork().getValidator() != null ) {
// 			for (ValidatorRule rule : snapshot.getNetwork().getValidator().getRules() ) {
	
// 				String ruleID = rule.getId().toString();
	
// 				ValidationRuleStat ruleResult = new ValidationRuleStat();
	
// 				ruleResult.ruleID = rule.getId();
// 				ruleResult.validCount = validRuleMap.get(ruleID);
// 				ruleResult.invalidCount = invalidRuleMap.get(ruleID);
// 				ruleResult.name = rule.getName();
// 				ruleResult.description = rule.getDescription();
// 				ruleResult.mandatory = rule.getMandatory();
// 				ruleResult.quantifier = rule.getQuantifier();
	
// 				result.rulesByID.put(ruleID, ruleResult);
	
// 			}
// 		}

// 		return result;
// 	}
	
// 	@Getter
// 	@Setter
// 	public class ValidationStats {

// 		public ValidationStats() {
// 			facets = new HashMap<String, List<FacetFieldEntry>>();
// 			rulesByID = new HashMap<String, ValidationRuleStat>();
// 		}

// 		Integer size;
// 		Integer transformedSize;
// 		Integer validSize;
// 		Map<String, ValidationRuleStat> rulesByID;
// 		Map<String, List<FacetFieldEntry>> facets;
// 	}
	
// 	@Getter
// 	@Setter
// 	public class ValidationRuleStat {
// 		Long ruleID;
// 		String name;
// 		String description;
// 		QuantifierValues quantifier;
// 		Boolean mandatory;
// 		Integer validCount;
// 		Integer invalidCount;
// 	}
	
	
// 	public boolean isServiceAvaliable() {
		
		
// 		try {
// 			SolrPingResponse ping = validationSolrTemplate.ping(validationCoreName);
// 			logger.debug( "Diagnose core - vstats - SOLR - Status: " + ((ping.getStatus() == 0) ? "OK" : "FAIL")  );

// 		} 
// 		catch ( Exception e) {
// 			logger.error( "Diagnose core:vstats - SOLR ERROR: " + e.getMessage());
// 			return false;
// 		}
		
// 		return true;
// 	}
	
	
	// public ValidationRuleOccurrencesCount queryValidRuleOcurrencesCountBySnapshotID(Long snapshotID, Long ruleID, List<String> fq)  {

		
	// 	ValidationRuleOccurrencesCount result = new ValidationRuleOccurrencesCount();

	// 	FacetQuery facetQuery = new SimpleFacetQuery(new SimpleStringCriteria(SNAPSHOT_ID_FIELD + ":" + snapshotID));
	// 	facetQuery.setRows(0);

	// 	for (String fqTerm : fq) {
	// 		fqTerm = fqTerm.replace("@@", ":");
	// 		facetQuery.addFilterQuery(new SimpleFilterQuery(new SimpleStringCriteria(fqTerm)));
	// 	}

	// 	FacetOptions facetOptions = new FacetOptions();
	// 	facetOptions.setFacetMinCount(1);
	// 	facetOptions.setFacetLimit(1000);

	// 	facetOptions.addFacetOnField(ruleID.toString() + INVALID_RULE_SUFFIX);
	// 	facetOptions.addFacetOnField(ruleID.toString() + VALID_RULE_SUFFIX);

	// 	facetOptions.setFacetSort(FacetSort.COUNT);

	// 	facetQuery.setFacetOptions(facetOptions);

	// 	FacetPage<ValidationStatObservation> facetResult = validationSolrTemplate.queryForFacetPage(validationCoreName,facetQuery, ValidationStatObservation.class);

	// 	List<OccurrenceCount> validRuleOccurrence = new ArrayList<OccurrenceCount>();
	// 	List<OccurrenceCount> invalidRuleOccurrence = new ArrayList<OccurrenceCount>();

	// 	for (FacetFieldEntry occr : facetResult.getFacetResultPage(ruleID.toString() + VALID_RULE_SUFFIX).getContent())
	// 		validRuleOccurrence.add(new OccurrenceCount(occr.getValue(), (int) occr.getValueCount()));

	// 	for (FacetFieldEntry occr : facetResult.getFacetResultPage(ruleID.toString() + INVALID_RULE_SUFFIX).getContent())
	// 		invalidRuleOccurrence.add(new OccurrenceCount(occr.getValue(), (int) occr.getValueCount()));

	// 	result.setValidRuleOccrs(validRuleOccurrence);
	// 	result.setInvalidRuleOccrs(invalidRuleOccurrence);

	// 	return result;
	// }
	
	
// 	@Getter
// 	@Setter
// 	public class ValidationRuleOccurrencesCount {
// 		List<OccurrenceCount> invalidRuleOccrs;
// 		List<OccurrenceCount> validRuleOccrs;
// 	}
	
// 	@Getter
// 	@Setter
// 	public class OccurrenceCount {
// 		public OccurrenceCount(String value, int count) {
// 			super();
// 			this.value = value;
// 			this.count = count;
// 		}

// 		String value;
// 		Integer count;
// 	}
	
	
	// public Page<ValidationStatObservation> queryValidationStatsObservationsBySnapshotID(Long snapshotID, List<String> fq, Pageable pageable) {

	// 	Query query = new SimpleQuery(SNAPSHOT_ID_FIELD + ":" + snapshotID.toString());

	// 	// Esta correccion permite pagiona
	// 	pageable = pageable.previousOrFirst();

	// 	query.setPageRequest(pageable);

	// 	for (String fqTerm : fq) {
	// 		fqTerm = fqTerm.replace("@@", ":");
	// 		query.addFilterQuery(new SimpleFilterQuery(new SimpleStringCriteria(fqTerm)));
	// 	}

	// 	Page<ValidationStatObservation> results = validationSolrTemplate.queryForPage(validationCoreName,query, ValidationStatObservation.class);

	// 	return results;
	// }

// 	/**
// 	 * deleteValidationStatsObservationsBySnapshotID
// 	 */
// 	public void deleteValidationStatsObservationsByRecordIDsAndSnapshotID(Long snapshotID) throws ValidationStatisticsException {

// 		try {
// 			Query query = new SimpleQuery(SNAPSHOT_ID_FIELD + ":" + snapshotID.toString());
// 			validationSolrTemplate.delete(validationCoreName,query);
// 		} catch (Exception e) {
// 			throw new ValidationStatisticsException("Error deleting validation info | snapahot:" + snapshotID  + " :: " + e.getMessage());
// 		}
// 	}


// 	/**
// 	 * deleteValidationStatsObservationsBySnapshotID
// 	 * @param snapshotID
// 	 * @param records
// 	 */
// 	public void deleteValidationStatsObservationsByRecordsAndSnapshotID(Long snapshotID, Collection<OAIRecord> records) throws ValidationStatisticsException {

// 		for (OAIRecord record : records) {
// 			try {
// 				validationStatRepository.deleteById( fingerprintHelper.getStatsIDfromRecord(record) );
// 			} catch (Exception e) {
// 				throw new ValidationStatisticsException("Error deleting validation info | snapahot:" + snapshotID + " recordID:" + record.getIdentifier() + " :: " + e.getMessage());
// 			}
// 		}
// 	}

// 	/**
// 	 * deleteValidationStatsObservationBySnapshotID
// 	 * @param snapshotID
// 	 * @param record
// 	 */
// 	public void deleteValidationStatsObservationByRecordAndSnapshotID(Long snapshotID, OAIRecord record) throws ValidationStatisticsException {

// 			try {
// 				validationStatRepository.deleteById( fingerprintHelper.getStatsIDfromRecord(record) );
// 			} catch (Exception e) {
// 				throw new ValidationStatisticsException("Error deleting validation info | snapahot:" + snapshotID + " recordID:" + record.getIdentifier() + " :: " + e.getMessage());
// 			}

// 	}

// 	/** Copy ValidationStatObservation from an existing snapshotID to another **/
// 	public boolean copyValidationStatsObservationsFromTo(Long originalSnapshotId, Long newSnapshotId) throws ValidationStatisticsException {

// 		int pageSize = 1000;
// 		Long pageNumber = 0L;

// 		Query baseQuery = new SimpleQuery(SNAPSHOT_ID_FIELD + ":" + originalSnapshotId.toString());
// 		Query query = baseQuery.setRows(pageSize).setOffset(pageNumber * pageSize);

// 		try {
// 			// query the first page
// 			Page<ValidationStatObservation> page = validationSolrTemplate.queryForPage(validationCoreName, query, ValidationStatObservation.class);

// 			// while there are results
// 			while (page.hasContent()) {

// 				// for each result update the snapshotID
// 				for (ValidationStatObservation obs : page.getContent()) {
// 					// update the snapshotID
// 					obs.setSnapshotID(newSnapshotId);
// 					obs.setId( fingerprintHelper.getStatsIDfromValidationStatObservation(obs) );
// 				}

// 				// save the page
// 				validationStatRepository.saveAll(page.getContent());

// 				// query the next page

// 				pageNumber++;
// 				query = baseQuery.setRows(pageSize).setOffset(pageNumber * pageSize);
// 				page = validationSolrTemplate.queryForPage(validationCoreName, query, ValidationStatObservation.class);
// 			}
// 		} catch (Exception e) {
// 			throw new ValidationStatisticsException("Error copying validation info | snapahot:" + originalSnapshotId + " to snapahot:" + newSnapshotId + " :: " + e.getMessage());
// 		}

// 		return true;
// 	}
	
	
	
// 	/**
// 	 * Retorna un Map entre los ids y los nombres de las reglas
// 	 */
// 	private Map<String, Integer> obtainFacetMap(List<FacetFieldEntry> facetList) {

// 		Map<String, Integer> facetMap = new HashMap<String, Integer>();

// 		for (FacetFieldEntry entry : facetList)
// 			facetMap.put(entry.getValue(), (int) entry.getValueCount());

// 		return facetMap;
// 	}
	
// 	public void deleteValidationStatsBySnapshotID(Long snapshotID) throws ValidationStatisticsException {
		
// 		try {
// 			Query query = new SimpleQuery(SNAPSHOT_ID_FIELD + ":" + snapshotID.toString());
// 			validationSolrTemplate.delete(validationCoreName,query);
// 		} catch (Exception e) {
// 			throw new ValidationStatisticsException("Error deleting validation info | snapahot:" + snapshotID  + " :: " + e.getMessage());
// 		}
		
// 	}



// }


import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.client.solrj.response.UpdateResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrInputDocument;
import org.lareferencia.backend.domain.OAIRecord;
import org.lareferencia.backend.validation.validator.ContentValidatorResult;
import org.lareferencia.core.util.IRecordFingerprintHelper;
import org.lareferencia.core.validation.QuantifierValues;
import org.lareferencia.core.validation.ValidatorResult;
import org.lareferencia.core.validation.ValidatorRuleResult;
import org.apache.solr.client.solrj.response.FacetField;
import org.apache.solr.client.solrj.response.FacetField.Count;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Scope;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageImpl;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.ArrayList;

import lombok.Getter;
import lombok.Setter;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Component
@Scope("prototype")
public class ValidationStatisticsService {

    private static Logger logger = LogManager.getLogger(ValidationStatisticsService.class);

    public static final String[] FACET_FIELDS = { "record_is_valid", "record_is_transformed", "valid_rules", "invalid_rules", "institution_name", "repository_name" };
    public static final String INVALID_RULE_SUFFIX = "_rule_invalid_occrs";
    public static final String VALID_RULE_SUFFIX = "_rule_valid_occrs";

    public static final String SNAPSHOT_ID_FIELD = "snapshot_id";
    public static final String NETWORK_ACRONYM_FIELD = "network_acronym";
    public static final String IDENTIFIER_FIELD = "oai_identifier";
    public static final String ORIGIN_FIELD = "origin";
    public static final String SET_SPEC_FIELD = "set_spec";
    public static final String METADATA_PREFIX_FIELD = "metadata_prefix";
    public static final String REPOSITORY_NAME_FIELD = "repository_name";
    public static final String INSTITUTION_NAME_FIELD = "institution_name";
    public static final String RECORD_IS_VALID_FIELD = "record_is_valid";
    public static final String RECORD_IS_TRANSFORMED_FIELD = "record_is_transformed";
    public static final String ID_FIELD = "id";

    @Value("${vstats.solr.core}")
    private String validationCoreName;

	@Autowired
	private IRecordFingerprintHelper fingerprintHelper;

	@Setter
 	@Getter
 	boolean detailedDiagnose = false;

    // named solr client
    @Autowired
    @Qualifier("validationSolrClient")
    private SolrClient solrClient;

    public ValidationStatisticsService() {
    }


 /**
     * Construye un objeto ValidationStatObservation persistible en Solr a partir del resultado de validación
     * devuelto por el validador para un registro.
     *
     * @param record El registro OAI que contiene los datos del snapshot y otros metadatos
     * @param validationResult El resultado de la validación aplicado al registro
     * @return Un objeto ValidationStatObservation listo para ser persistido
     */
    public ValidationStatObservation buildObservation(OAIRecord record, ValidatorResult validationResult) {
        ValidationStatObservation obs = new ValidationStatObservation();

        logger.debug("Construyendo registro de resultado de validación para el ID: " + record.getId().toString());

        // Configura los campos básicos del objeto de observación
        obs.setId(fingerprintHelper.getStatsIDfromRecord(record));
        obs.setIdentifier(record.getIdentifier());
        obs.setOrigin(record.getSnapshot().getNetwork().getOriginURL());
        obs.setMetadataPrefix(record.getSnapshot().getNetwork().getMetadataPrefix());
        obs.setIsTransformed(record.getTransformed());
        obs.setSnapshotID(record.getSnapshot().getId());
        obs.setNetworkAcronym(record.getSnapshot().getNetwork().getAcronym());
        obs.setIsValid(validationResult.isValid());

        // Procesa cada regla de validación del resultado
        for (ValidatorRuleResult ruleResult : validationResult.getRulesResults()) {
            String ruleID = ruleResult.getRule().getRuleId().toString();

            List<String> invalidOccurrences = new ArrayList<>();
            List<String> validOccurrences = new ArrayList<>();

            if (detailedDiagnose) { // Recolecta las ocurrencias solo si el diagnóstico es detallado
                logger.debug("Reporte de validación detallado - ID de la Regla: " + ruleID);

                for (ContentValidatorResult contentResult : ruleResult.getResults()) {
                    if (contentResult.isValid()) {
                        validOccurrences.add(contentResult.getReceivedValue());
                    } else {
                        invalidOccurrences.add(contentResult.getReceivedValue());
                    }
                }

                // Agrega las ocurrencias válidas e inválidas a las respectivas colecciones
                obs.getValidOccurrencesByRuleID().put(ruleID, validOccurrences);
                obs.getInvalidOccurrencesByRuleID().put(ruleID, invalidOccurrences);
            }

            // Añade la regla a las listas de reglas válidas o inválidas según corresponda
            if (ruleResult.getValid()) {
                obs.getValidRulesID().add(ruleID);
            } else {
                obs.getInvalidRulesID().add(ruleID);
            }
        }

        return obs;
    }

    /**
     * Consulta estadísticas de validación por snapshot utilizando facetas en Solr.
     *
     * @param snapshotId El ID del snapshot para filtrar
     * @param fq Lista de filtros adicionales para la consulta
     * @return Un objeto ValidationStats con las estadísticas obtenidas
     * @throws Exception Si ocurre un error durante la consulta
     */
    public ValidationStats queryValidatorRulesStatsBySnapshot(Long snapshotId, List<String> fq) throws Exception {
        ValidationStats result = new ValidationStats();

        // Construcción de la consulta Solr
        SolrQuery query = new SolrQuery();
        query.setQuery("snapshot_id:" + snapshotId);
        query.setRows(0); // Solo queremos facetas, no resultados de documentos

        // Agrega filtros de consulta (filter queries)
        for (String fqTerm : fq) {
            query.addFilterQuery(fqTerm.replace("@@", ":"));
        }

        // Configuración de las opciones de facetas
        query.setFacet(true);
        query.addFacetField("valid_rules", "invalid_rules", "record_is_valid", "record_is_transformed");
        query.setFacetLimit(1000);
        query.setFacetMinCount(1);

        // Ejecución de la consulta y manejo de la respuesta
        QueryResponse response = solrClient.query(validationCoreName, query);

        // Procesar los resultados de facetas
        Map<String, Map<String, Integer>> facetResults = new HashMap<>();
        for (FacetField facetField : response.getFacetFields()) {
            Map<String, Integer> facetMap = new HashMap<>();
            for (Count count : facetField.getValues()) {
                facetMap.put(count.getName(), (int) count.getCount());
            }
            facetResults.put(facetField.getName(), facetMap);
        }
        result.setFacets(facetResults);
	
        return result;
    }

    /**
     * Verifica si el servicio Solr está disponible.
     *
     * @return true si el servicio está disponible, false de lo contrario
     */
    public boolean isServiceAvailable() {
        try {
            solrClient.ping(validationCoreName);
            logger.debug("Diagnose core - SOLR - Status: OK");
            return true;
        } catch (Exception e) {
            logger.error("Diagnose core - SOLR ERROR: " + e.getMessage());
            return false;
        }
    }

    /**
     * Elimina documentos de Solr según un filtro de consulta.
     *
     * @param query El filtro para eliminar documentos
     * @throws Exception Si ocurre un error durante la eliminación
     */
    public void deleteDocumentsByQuery(String query) throws Exception {
        try {
            UpdateResponse response = solrClient.deleteByQuery(validationCoreName, query);
            solrClient.commit(validationCoreName);
            logger.debug("Documentos eliminados con éxito. Respuesta: " + response);
        } catch (Exception e) {
            logger.error("Error eliminando documentos en Solr: " + e.getMessage());
            throw e;
        }
    }

    /**
     * Cierra el cliente Solr para liberar recursos.
     *
     * @throws Exception Si ocurre un error durante el cierre
     */
    public void close() throws Exception {
        solrClient.close();
    }

	@Getter
	@Setter
	public class ValidationStats {

		public ValidationStats() {
			facets = new HashMap<String, Map<String, Integer>>();
			rulesByID = new HashMap<String, ValidationRuleStat>();
		}

		Integer size;
		Integer transformedSize;
		Integer validSize;
		Map<String, ValidationRuleStat> rulesByID;
		Map<String, Map<String, Integer>> facets;
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

    public void deleteValidationStatsBySnapshotID(Long snapshotID) throws ValidationStatisticsException {
        try {
            String queryStr = SNAPSHOT_ID_FIELD + snapshotID.toString();
            UpdateResponse response = solrClient.deleteByQuery(queryStr);
            
            // check if the record was deleted
            if (response.getStatus() == 0) {
                logger.debug("Record deleted successfully | snapshot: " + snapshotID  );
            } else {
                logger.error("Error deleting validation info | snapshot: " + snapshotID );
                throw new ValidationStatisticsException("Error deleting validation info | snapshot: " + snapshotID );
            }

            solrClient.commit();
        } catch (Exception e) {
            logger.error("Error deleting validation info | snapshot: " + snapshotID + " :: " + e.getMessage());
            throw new ValidationStatisticsException("Error deleting validation info | snapshot: " + snapshotID + " :: " + e.getMessage());
        }
    }


 /** Copy ValidationStatObservation from an existing snapshotID to another **/
    public boolean copyValidationStatsObservationsFromTo(Long originalSnapshotId, Long newSnapshotId) throws ValidationStatisticsException {
        int pageSize = 1000;
        int start = 0;

        SolrQuery query = new SolrQuery();
        query.setQuery( SNAPSHOT_ID_FIELD + originalSnapshotId.toString());
        query.setRows(pageSize);
        query.setStart(start);

        try {
            QueryResponse response = solrClient.query(query);
            SolrDocumentList documents = response.getResults();

            while (!documents.isEmpty()) {
                List<SolrInputDocument> updatedDocuments = new ArrayList<>();

                for (SolrDocument doc : documents) {

                    doc.setField(SNAPSHOT_ID_FIELD, newSnapshotId);
                    doc.setField(ID_FIELD, fingerprintHelper.getStatsIDfromValidationStatObservation(doc));

                    SolrInputDocument idoc = new SolrInputDocument();
                    
                    for (String fieldName : doc.getFieldNames()) {
                        for (Object value : doc.getFieldValues(fieldName)) {
                            idoc.addField(fieldName, value);
                        }   
                    }

                    updatedDocuments.add(idoc);
                }

                solrClient.add(updatedDocuments);
                solrClient.commit();

                start += pageSize;
                query.setStart(start);
                response = solrClient.query(query);
                documents = response.getResults();
            }
        } catch (Exception e) {
            logger.error("Error copying validation info | snapshot: " + originalSnapshotId + " to snapshot: " + newSnapshotId + " :: " + e.getMessage());
            throw new ValidationStatisticsException("Error copying validation info | snapshot: " + originalSnapshotId + " to snapshot: " + newSnapshotId + " :: " + e.getMessage());
        }

        return true;
    }


    public void deleteValidationStatsObservationByRecordAndSnapshotID(Long snapshotID, OAIRecord record) throws ValidationStatisticsException {
        try {
            String queryStr = SNAPSHOT_ID_FIELD + snapshotID.toString();
            UpdateResponse response = solrClient.deleteByQuery(queryStr);
            
            // check if the record was deleted
            if (response.getStatus() == 0) {
                logger.debug("Record deleted successfully | snapshot: " + snapshotID + " | record: " + record.getIdentifier());
            } else {
                logger.error("Error deleting validation info | snapshot: " + snapshotID + " | record: " + record.getIdentifier());
                throw new ValidationStatisticsException("Error deleting validation info | snapshot: " + snapshotID + " | record: " + record.getIdentifier());
            }

            solrClient.commit();
        } catch (Exception e) {
            logger.error("Error deleting validation info | snapshot: " + snapshotID + " :: " + e.getMessage());
            throw new ValidationStatisticsException("Error deleting validation info | snapshot: " + snapshotID + " :: " + e.getMessage());
        }
    }


    public void registerObservations(ArrayList<ValidationStatObservation> validationStatsObservations) {
        try {
            for (ValidationStatObservation observation : validationStatsObservations) {
                SolrInputDocument doc = observation.toSolrInputDocument();
                solrClient.add(doc);
            }
            solrClient.commit();
        } catch (Exception e) {
            logger.error("Error registering stats (solr): " + e.getMessage());
        }
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

    public ValidationRuleOccurrencesCount queryValidRuleOcurrencesCountBySnapshotID(Long snapshotID, Long ruleID, List<String> fq) throws ValidationStatisticsException {

        ValidationRuleOccurrencesCount result = new ValidationRuleOccurrencesCount();

        // Crear la consulta Solr con el criterio basado en snapshotID
        SolrQuery solrQuery = new SolrQuery();
        solrQuery.setQuery(SNAPSHOT_ID_FIELD + ":" + snapshotID);
        solrQuery.setRows(0);

        // Añadir filtros a la consulta basados en la lista fq
        for (String fqTerm : fq) {
            fqTerm = fqTerm.replace("@@", ":");
            solrQuery.addFilterQuery(fqTerm);
        }

        // Configurar las opciones de facetas
        solrQuery.setFacet(true);
        solrQuery.addFacetField(ruleID.toString() + INVALID_RULE_SUFFIX);
        solrQuery.addFacetField(ruleID.toString() + VALID_RULE_SUFFIX);
        solrQuery.setFacetMinCount(1);
        solrQuery.setFacetLimit(1000);
        solrQuery.setFacetSort("count");

        // Ejecutar la consulta de facetas y obtener los resultados
        QueryResponse response = null;
        try {
            response = solrClient.query(validationCoreName, solrQuery);
        } catch (SolrServerException | IOException e) {
            logger.error("Error executing facet query: " + e.getMessage());
            throw new ValidationStatisticsException("Error executing facet query: " + e.getMessage());
        }
        List<FacetField> facetFields = response.getFacetFields();

        List<OccurrenceCount> validRuleOccurrence = new ArrayList<>();
        List<OccurrenceCount> invalidRuleOccurrence = new ArrayList<>();

        // Procesar los resultados para obtener las ocurrencias de reglas válidas e inválidas
        for (FacetField facetField : facetFields) {
            if (facetField.getName().equals(ruleID.toString() + VALID_RULE_SUFFIX)) {
                for (Count count : facetField.getValues()) {
                    validRuleOccurrence.add(new OccurrenceCount(count.getName(), (int) count.getCount()));
                }
            } else if (facetField.getName().equals(ruleID.toString() + INVALID_RULE_SUFFIX)) {
                for (Count count : facetField.getValues()) {
                    invalidRuleOccurrence.add(new OccurrenceCount(count.getName(), (int) count.getCount()));
                }
            }
        }

        // Establecer las ocurrencias en el resultado y devolverlo
        result.setValidRuleOccrs(validRuleOccurrence);
        result.setInvalidRuleOccrs(invalidRuleOccurrence);

        return result;
    }



   public Page<ValidationStatObservation> queryValidationStatsObservationsBySnapshotID(Long snapshotID, List<String> fq, Pageable pageable) throws Exception {

        // Crear la consulta Solr con el criterio basado en snapshotID
        SolrQuery solrQuery = new SolrQuery();
        solrQuery.setQuery(SNAPSHOT_ID_FIELD + ":" + snapshotID.toString());

        // Configurar la consulta para la paginación
        pageable = pageable.previousOrFirst();
        solrQuery.setStart((int) pageable.getOffset());
        solrQuery.setRows(pageable.getPageSize());

        // Añadir filtros a la consulta basados en la lista fq
        for (String fqTerm : fq) {
            fqTerm = fqTerm.replace("@@", ":");
            solrQuery.addFilterQuery(fqTerm);
        }

        // Ejecutar la consulta y obtener los resultados
        QueryResponse response = solrClient.query(validationCoreName, solrQuery);
        SolrDocumentList documents = response.getResults();

        // Convertir los resultados en una lista de ValidationStatObservation
        List<ValidationStatObservation> observations = new ArrayList<>();
        for (SolrDocument document : documents) {
            ValidationStatObservation observation = new ValidationStatObservation();
            // Aquí debes mapear los campos del documento a los campos de ValidationStatObservation
            // observation.setField(document.getFieldValue("field"));
            observations.add(observation);
        }

        // Crear una instancia de Page<ValidationStatObservation> y devolverla
        return new PageImpl<>(observations, pageable, documents.getNumFound());
    }

}
