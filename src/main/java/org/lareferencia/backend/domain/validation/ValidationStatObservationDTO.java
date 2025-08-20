package org.lareferencia.backend.domain.validation;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Objects;

/**
 * DTO para intercambio de datos de observaciones de estadísticas de validación
 * Abstrae la implementación específica (Parquet, Solr, etc.)
 */
public class ValidationStatObservationDTO {
    
    private String id;
    private String identifier;
    private Long snapshotId;
    private LocalDateTime validationDate;
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
    private String validRulesID;
    private String invalidRulesID;
    
    // Constructor por defecto
    public ValidationStatObservationDTO() {}
    
    // Constructor completo
    public ValidationStatObservationDTO(String id, String identifier, Long snapshotId, 
            String origin, String setSpec, String metadataPrefix, String networkAcronym,
            String repositoryName, String institutionName, Boolean isValid, Boolean isTransformed,
            String validOccurrencesByRuleIDJson, String invalidOccurrencesByRuleIDJson,
            String validRulesID, String invalidRulesID) {
        this.id = id;
        this.identifier = identifier;
        this.snapshotId = snapshotId;
        this.validationDate = LocalDateTime.now();
        this.origin = origin;
        this.setSpec = setSpec;
        this.metadataPrefix = metadataPrefix;
        this.networkAcronym = networkAcronym;
        this.repositoryName = repositoryName;
        this.institutionName = institutionName;
        this.isValid = isValid;
        this.isTransformed = isTransformed;
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
    
    public Long getSnapshotId() { return snapshotId; }
    public void setSnapshotId(Long snapshotId) { this.snapshotId = snapshotId; }
    
    public LocalDateTime getValidationDate() { return validationDate; }
    public void setValidationDate(LocalDateTime validationDate) { this.validationDate = validationDate; }
    
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
    
    public String getValidRulesID() { return validRulesID; }
    public void setValidRulesID(String validRulesID) { this.validRulesID = validRulesID; }
    
    public String getInvalidRulesID() { return invalidRulesID; }
    public void setInvalidRulesID(String invalidRulesID) { this.invalidRulesID = invalidRulesID; }
    
    // Métodos de utilidad
    public List<String> getValidRulesList() {
        if (validRulesID == null || validRulesID.trim().isEmpty()) {
            return List.of();
        }
        return List.of(validRulesID.split(","));
    }
    
    public List<String> getInvalidRulesList() {
        if (invalidRulesID == null || invalidRulesID.trim().isEmpty()) {
            return List.of();
        }
        return List.of(invalidRulesID.split(","));
    }
    
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
                ", snapshotId=" + snapshotId +
                ", isValid=" + isValid +
                ", isTransformed=" + isTransformed +
                '}';
    }
}
