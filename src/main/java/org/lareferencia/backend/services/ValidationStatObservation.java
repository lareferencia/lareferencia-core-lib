package org.lareferencia.backend.services;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrInputDocument;

import lombok.Getter;
import lombok.Setter;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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

    public ValidationStatObservation() {
        validOccurrencesByRuleID = new HashMap<>();
        invalidOccurrencesByRuleID = new HashMap<>();
        validRulesID = new ArrayList<>();
        invalidRulesID = new ArrayList<>();
    }

    // Getters y setters para todos los campos...

    // Método para agregar un documento a Solr usando SolrJ
    public void addToSolr(SolrClient solrClient, String collection) throws Exception {
        SolrInputDocument doc = new SolrInputDocument();
        doc.addField("id", this.id);
        doc.addField("oai_identifier", this.identifier);
        doc.addField("snapshot_id", this.snapshotID);
        doc.addField("origin", this.origin);
        doc.addField("set_spec", this.setSpec);
        doc.addField("metadata_prefix", this.metadataPrefix);
        doc.addField("network_acronym", this.networkAcronym);
        doc.addField("repository_name", this.repositoryName);
        doc.addField("institution_name", this.institutionName);
        doc.addField("record_is_valid", this.isValid);
        doc.addField("record_is_transformed", this.isTransformed);
        
        // Agregar campos dinámicos para reglas válidas e inválidas
        for (Map.Entry<String, List<String>> entry : validOccurrencesByRuleID.entrySet()) {
            doc.addField(entry.getKey() + "_valid", entry.getValue());
        }
        for (Map.Entry<String, List<String>> entry : invalidOccurrencesByRuleID.entrySet()) {
            doc.addField(entry.getKey() + "_invalid", entry.getValue());
        }
        
        // Agregar listas de reglas válidas e inválidas
        doc.addField("valid_rules", this.validRulesID);
        doc.addField("invalid_rules", this.invalidRulesID);

        solrClient.add(collection, doc);
        solrClient.commit(collection);
    }

    // Método para buscar documentos en Solr usando SolrJ
    public static List<ValidationStatObservation> searchFromSolr(SolrClient solrClient, String collection, String queryStr) throws Exception {
        SolrQuery query = new SolrQuery();
        query.setQuery(queryStr);
        
        QueryResponse response = solrClient.query(collection, query);
        
        List<ValidationStatObservation> results = new ArrayList<>();
        response.getResults().forEach(doc -> {
            ValidationStatObservation observation = new ValidationStatObservation();
            observation.setId((String) doc.getFieldValue("id"));
            observation.setIdentifier((String) doc.getFieldValue("oai_identifier"));
            observation.setSnapshotID((Long) doc.getFieldValue("snapshot_id"));
            observation.setOrigin((String) doc.getFieldValue("origin"));
            observation.setSetSpec((String) doc.getFieldValue("set_spec"));
            observation.setMetadataPrefix((String) doc.getFieldValue("metadata_prefix"));
            observation.setNetworkAcronym((String) doc.getFieldValue("network_acronym"));
            observation.setRepositoryName((String) doc.getFieldValue("repository_name"));
            observation.setInstitutionName((String) doc.getFieldValue("institution_name"));
            observation.setIsValid((Boolean) doc.getFieldValue("record_is_valid"));
            observation.setIsTransformed((Boolean) doc.getFieldValue("record_is_transformed"));

            // Asigna otras propiedades según sea necesario...

            results.add(observation);
        });
        
        return results;
    }

    public static void main(String[] args) {
        String solrUrl = "http://localhost:8983/solr";
        String collection = "vstats";
        try (SolrClient solrClient = new HttpSolrClient.Builder(solrUrl).build()) {
            ValidationStatObservation observation = new ValidationStatObservation();
            observation.setId("12345");
            observation.setIdentifier("example_identifier");
            observation.setSnapshotID(1L);
            observation.setOrigin("example_origin");
            observation.setSetSpec("example_set");
            observation.setMetadataPrefix("example_prefix");
            observation.setNetworkAcronym("example_network");
            observation.setRepositoryName("example_repository");
            observation.setInstitutionName("example_institution");
            observation.setIsValid(true);
            observation.setIsTransformed(true);

            observation.addToSolr(solrClient, collection);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public SolrInputDocument toSolrInputDocument() {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'toSolrInputDocument'");
    }

    public static ValidationStatObservation fromSolrDocument(SolrDocument doc) {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'fromSolrDocument'");
    }
}
