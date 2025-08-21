package org.lareferencia.backend.domain.validation;

import java.util.List;
import java.util.Objects;

/**
 * DTO para intercambio de datos de observaciones de estadísticas de validación
 * Abstrae la implementación específica (Parquet, Solr, etc.)
 */
public class ValidationStatObservationDTO {
    
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
    private String validOccurrencesByRuleIDJson;
    private String invalidOccurrencesByRuleIDJson;
    private List<String> validRulesID;
    private List<String> invalidRulesID;
    
    // Constructor por defecto
    public ValidationStatObservationDTO() {}
    
    // Constructor completo
    public ValidationStatObservationDTO(String snapshotId, String repositoryName, 
                                      int occurrencesByRuleID, int occurrencesByRecordID, 
                                      int transformationCount, String validOccurrencesByRuleIDJson, 
                                      String invalidOccurrencesByRuleIDJson, List<String> validRulesID, 
                                      List<String> invalidRulesID) {
        this.snapshotID = Long.parseLong(snapshotId);
        this.repositoryName = repositoryName;
        this.validOccurrencesByRuleIDJson = validOccurrencesByRuleIDJson;
        this.invalidOccurrencesByRuleIDJson = invalidOccurrencesByRuleIDJson;
        this.validRulesID = validRulesID;
        this.invalidRulesID = invalidRulesID;
    }
    
    // Getters y Setters
    public String getId() { return id; }
    public void setId(String id) { this.id = id; }
    
    public String getIdentifier() { return identifier; }
    public void setIdentifier(String identifier) { this.identifier = identifier; }
    
    public Long getSnapshotID() { return snapshotID; }
    public void setSnapshotID(Long snapshotID) { this.snapshotID = snapshotID; }
    
    public String getOrigin() { return origin; }
    public void setOrigin(String origin) { this.origin = origin; }
    
    public String getSetSpec() { return setSpec; }
    public void setSetSpec(String setSpec) { this.setSpec = setSpec; }
    
    public String getMetadataPrefix() { return metadataPrefix; }
    public void setMetadataPrefix(String metadataPrefix) { this.metadataPrefix = metadataPrefix; }
    
    public String getNetworkAcronym() { return networkAcronym; }
    public void setNetworkAcronym(String networkAcronym) { this.networkAcronym = networkAcronym; }
    
    public String getRepositoryName() { return repositoryName; }
    public void setRepositoryName(String repositoryName) { this.repositoryName = repositoryName; }
    
    public String getInstitutionName() { return institutionName; }
    public void setInstitutionName(String institutionName) { this.institutionName = institutionName; }
    
    public Boolean getIsValid() { return isValid; }
    public void setIsValid(Boolean isValid) { this.isValid = isValid; }
    
    public Boolean getIsTransformed() { return isTransformed; }
    public void setIsTransformed(Boolean isTransformed) { this.isTransformed = isTransformed; }
    
    public String getValidOccurrencesByRuleIDJson() { return validOccurrencesByRuleIDJson; }
    public void setValidOccurrencesByRuleIDJson(String validOccurrencesByRuleIDJson) { 
        this.validOccurrencesByRuleIDJson = validOccurrencesByRuleIDJson; 
    }
    
    public String getInvalidOccurrencesByRuleIDJson() { return invalidOccurrencesByRuleIDJson; }
    public void setInvalidOccurrencesByRuleIDJson(String invalidOccurrencesByRuleIDJson) { 
        this.invalidOccurrencesByRuleIDJson = invalidOccurrencesByRuleIDJson; 
    }
    
    public List<String> getValidRulesID() { return validRulesID; }
    public void setValidRulesID(List<String> validRulesID) { this.validRulesID = validRulesID; }
    
    public List<String> getInvalidRulesID() { return invalidRulesID; }
    public void setInvalidRulesID(List<String> invalidRulesID) { this.invalidRulesID = invalidRulesID; }
    
    
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ValidationStatObservationDTO that = (ValidationStatObservationDTO) o;
        return Objects.equals(id, that.id) && Objects.equals(identifier, that.identifier);
    }
    
    @Override
    public int hashCode() {
        return Objects.hash(id, identifier);
    }
    
    @Override
    public String toString() {
        return "ValidationStatObservationDTO{" +
                "id='" + id + '\'' +
                ", identifier='" + identifier + '\'' +
                ", snapshotID=" + snapshotID +
                ", isValid=" + isValid +
                ", isTransformed=" + isTransformed +
                '}';
    }
}
