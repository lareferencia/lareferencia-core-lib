package org.lareferencia.core.service.validation;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Validation statistics observation class.
 * This class represents a validation observation that can be stored in different backends
 * (Parquet, Solr, etc.) by serializing complex structures to JSON strings.
 * 
 * NUEVA ARQUITECTURA PARQUET:
 * - Campos JSON marcados con @JsonIgnore (solo para persistencia interna)
 * - Campos Map expuestos para API REST (getValidOccurrencesByRuleID)
 * - Campos legacy marcados @JsonIgnore para compatibilidad sin duplicación
 */
public class ValidationStatObservation {
    
    /**
     * Unique identifier for this validation observation.
     */
    protected String id;
    
    /**
     * OAI identifier of the record being observed.
     */
    protected String identifier;
    
    /**
     * Snapshot ID this observation belongs to.
     */
    protected Long snapshotId;
    
    /**
     * Date when validation was performed.
     */
    protected LocalDateTime validationDate;
    
    /**
     * Origin URL of the record source.
     */
    protected String origin;
    
    /**
     * OAI set specification of the record.
     */
    protected String setSpec;
    
    /**
     * Metadata format prefix of the record.
     */
    protected String metadataPrefix;
    
    /**
     * Network acronym this record belongs to.
     */
    protected String networkAcronym;
    
    /**
     * Name of the repository.
     */
    protected String repositoryName;
    
    /**
     * Name of the institution.
     */
    protected String institutionName;
    
    /**
     * Whether the record passed validation.
     */
    protected Boolean isValid;
    
    /**
     * Whether the record was transformed.
     */
    protected Boolean isTransformed;
    
    /**
     * JSON representation of valid occurrences by rule ID.
     * INTERNAL ONLY - Use getValidOccurrencesByRuleID() for API.
     */
    @JsonIgnore
    protected String validOccurrencesByRuleIDJson;
    
    /**
     * JSON representation of invalid occurrences by rule ID.
     * INTERNAL ONLY - Use getInvalidOccurrencesByRuleID() for API.
     */
    @JsonIgnore
    protected String invalidOccurrencesByRuleIDJson;
    
    /**
     * Identifiers of rules that passed validation.
     * INTERNAL ONLY - Use getValidRulesIDList() for API.
     */
    @JsonIgnore
    protected String validRulesID;
    
    /**
     * Identifiers of rules that failed validation.
     * INTERNAL ONLY - Use getInvalidRulesIDList() for API.
     */
    @JsonIgnore
    protected String invalidRulesID;
    
    /**
     * Default constructor for validation statistics observation.
     */
    public ValidationStatObservation() {}
    
    /**
     * Constructs a complete validation statistics observation with all fields.
     * 
     * @param id unique identifier
     * @param identifier OAI record identifier
     * @param snapshotId snapshot ID
     * @param origin origin URL
     * @param setSpec OAI set specification
     * @param metadataPrefix metadata format prefix
     * @param networkAcronym network acronym
     * @param repositoryName repository name
     * @param institutionName institution name
     * @param isValid whether record is valid
     * @param isTransformed whether record was transformed
     * @param validOccurrencesByRuleIDJson JSON of valid occurrences by rule
     * @param invalidOccurrencesByRuleIDJson JSON of invalid occurrences by rule
     * @param validRulesID identifiers of valid rules
     * @param invalidRulesID identifiers of invalid rules
     */
    public ValidationStatObservation(String id, String identifier, Long snapshotId, 
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
    
    // Getters and setters
    
    /**
     * Gets the unique identifier.
     * 
     * @return the ID
     */
    public String getId() { return id; }
    
    /**
     * Sets the unique identifier.
     * 
     * @param id the ID to set
     */
    public void setId(String id) { this.id = id; }
    
    /**
     * Gets the OAI record identifier.
     * 
     * @return the identifier
     */
    public String getIdentifier() { return identifier; }
    
    /**
     * Sets the OAI record identifier.
     * 
     * @param identifier the identifier to set
     */
    public void setIdentifier(String identifier) { this.identifier = identifier; }
    
    /**
     * Gets the snapshot ID. Serialized as `snapshotID` (legacy external format).
     *
     * @return the snapshot ID
     */
    @JsonProperty("snapshotID")
    public Long getSnapshotId() { return snapshotId; }
    
    /**
     * Sets the snapshot ID.
     * 
     * @param snapshotId the snapshot ID to set
     */
    public void setSnapshotId(Long snapshotId) { this.snapshotId = snapshotId; }
    
    /**
     * Gets the validation date.
     *
     * NOTE: hidden from API responses to match the "correct" format.
     *
     * @return the validation date
     */
    @JsonIgnore
    public LocalDateTime getValidationDate() { return validationDate; }
    
    /**
     * Sets the validation date.
     * 
     * @param validationDate the validation date to set
     */
    public void setValidationDate(LocalDateTime validationDate) { this.validationDate = validationDate; }
    
    /**
     * Gets the origin URL.
     * 
     * @return the origin URL
     */
    public String getOrigin() { return origin; }
    
    /**
     * Sets the origin URL.
     * 
     * @param origin the origin URL to set
     */
    public void setOrigin(String origin) { this.origin = origin; }
    
    /**
     * Gets the OAI set specification.
     * 
     * @return the set specification
     */
    public String getSetSpec() { return setSpec; }
    
    /**
     * Sets the OAI set specification.
     * 
     * @param setSpec the set specification to set
     */
    public void setSetSpec(String setSpec) { this.setSpec = setSpec; }
    
    /**
     * Gets the metadata format prefix.
     * 
     * @return the metadata prefix
     */
    public String getMetadataPrefix() { return metadataPrefix; }
    
    /**
     * Sets the metadata format prefix.
     * 
     * @param metadataPrefix the metadata prefix to set
     */
    public void setMetadataPrefix(String metadataPrefix) { this.metadataPrefix = metadataPrefix; }
    
    /**
     * Gets the network acronym.
     * 
     * @return the network acronym
     */
    public String getNetworkAcronym() { return networkAcronym; }
    
    /**
     * Sets the network acronym.
     * 
     * @param networkAcronym the network acronym to set
     */
    public void setNetworkAcronym(String networkAcronym) { this.networkAcronym = networkAcronym; }
    
    /**
     * Gets the repository name.
     * 
     * @return the repository name
     */
    public String getRepositoryName() { return repositoryName; }
    
    /**
     * Sets the repository name.
     * 
     * @param repositoryName the repository name to set
     */
    public void setRepositoryName(String repositoryName) { this.repositoryName = repositoryName; }
    
    /**
     * Gets the institution name.
     * 
     * @return the institution name
     */
    public String getInstitutionName() { return institutionName; }
    
    /**
     * Sets the institution name.
     * 
     * @param institutionName the institution name to set
     */
    public void setInstitutionName(String institutionName) { this.institutionName = institutionName; }
    
    /**
     * Gets whether the record is valid.
     * 
     * @return true if valid, false otherwise
     */
    public Boolean getIsValid() { return isValid; }
    
    /**
     * Sets whether the record is valid.
     * 
     * @param isValid true if valid, false otherwise
     */
    public void setIsValid(Boolean isValid) { this.isValid = isValid; }
    
    /**
     * Gets whether the record was transformed.
     * 
     * @return true if transformed, false otherwise
     */
    public Boolean getIsTransformed() { return isTransformed; }
    
    /**
     * Sets whether the record was transformed.
     * 
     * @param isTransformed true if transformed, false otherwise
     */
    public void setIsTransformed(Boolean isTransformed) { this.isTransformed = isTransformed; }
    
    /**
     * Gets the JSON representation of valid occurrences by rule ID.
     * INTERNAL ONLY - Use getValidOccurrencesByRuleID() for API.
     * 
     * @return the valid occurrences JSON
     */
    @JsonIgnore
    public String getValidOccurrencesByRuleIDJson() { return validOccurrencesByRuleIDJson; }
    
    /**
     * Sets the JSON representation of valid occurrences by rule ID.
     * INTERNAL ONLY - Use setValidOccurrencesByRuleID(Map) for API.
     * 
     * @param validOccurrencesByRuleIDJson the valid occurrences JSON to set
     */
    @JsonIgnore
    public void setValidOccurrencesByRuleIDJson(String validOccurrencesByRuleIDJson) { 
        this.validOccurrencesByRuleIDJson = validOccurrencesByRuleIDJson; 
    }
    
    /**
     * Gets the JSON representation of invalid occurrences by rule ID.
     * INTERNAL ONLY - Use getInvalidOccurrencesByRuleID() for API.
     * 
     * @return the invalid occurrences JSON
     */
    @JsonIgnore
    public String getInvalidOccurrencesByRuleIDJson() { return invalidOccurrencesByRuleIDJson; }
    
    /**
     * Sets the JSON representation of invalid occurrences by rule ID.
     * INTERNAL ONLY - Use setInvalidOccurrencesByRuleID(Map) for API.
     * 
     * @param invalidOccurrencesByRuleIDJson the invalid occurrences JSON to set
     */
    @JsonIgnore
    public void setInvalidOccurrencesByRuleIDJson(String invalidOccurrencesByRuleIDJson) { 
        this.invalidOccurrencesByRuleIDJson = invalidOccurrencesByRuleIDJson; 
    }
    
    /**
     * Gets the identifiers of rules that passed validation.
     * INTERNAL ONLY - Use getValidRulesIDList() for API.
     * 
     * @return the valid rules IDs
     */
    @JsonIgnore
    public String getValidRulesID() { return validRulesID; }
    
    /**
     * Sets the identifiers of rules that passed validation.
     * INTERNAL ONLY - Use setValidRulesIDList() for API.
     * 
     * @param validRulesID the valid rules IDs to set
     */
    @JsonIgnore
    public void setValidRulesID(String validRulesID) { this.validRulesID = validRulesID; }
    
    /**
     * Gets the identifiers of rules that failed validation.
     * INTERNAL ONLY - Use getInvalidRulesIDList() for API.
     * 
     * @return the invalid rules IDs
     */
    @JsonIgnore
    public String getInvalidRulesID() { return invalidRulesID; }
    
    /**
     * Sets the identifiers of rules that failed validation.
     * INTERNAL ONLY - Use setInvalidRulesIDList() for API.
     * 
     * @param invalidRulesID the invalid rules IDs to set
     */
    @JsonIgnore
    public void setInvalidRulesID(String invalidRulesID) { this.invalidRulesID = invalidRulesID; }
    
    // Métodos de utilidad
    
    /**
     * Gets the list of valid rule identifiers as a list.
     * LEGACY COMPATIBILITY - Use getValidRulesIDList() instead.
     * 
     * @return list of valid rule IDs, or empty list if none
     */
    @JsonIgnore
    public List<String> getValidRulesList() {
        if (validRulesID == null || validRulesID.trim().isEmpty()) {
            return List.of();
        }
        return List.of(validRulesID.split(","));
    }
    
    /**
     * Gets the list of invalid rule identifiers as a list.
     * LEGACY COMPATIBILITY - Use getInvalidRulesIDList() instead.
     * 
     * @return list of invalid rule IDs, or empty list if none
     */
    @JsonIgnore
    public List<String> getInvalidRulesList() {
        if (invalidRulesID == null || invalidRulesID.trim().isEmpty()) {
            return List.of();
        }
        return List.of(invalidRulesID.split(","));
    }
    
    // Campos transientes para caché de deserialización
    private transient Map<String, List<String>> validOccurrencesByRuleIDCache;
    private transient Map<String, List<String>> invalidOccurrencesByRuleIDCache;
    
    /**
     * Gets the map of valid occurrences by deserializing from JSON.
     * Results are cached for performance.
     * 
     * @return map of valid occurrences by rule ID
     */
    public Map<String, List<String>> getValidOccurrencesByRuleID() {
        if (validOccurrencesByRuleIDCache == null && validOccurrencesByRuleIDJson != null) {
            validOccurrencesByRuleIDCache = deserializeMapFromJson(validOccurrencesByRuleIDJson);
        }
        return validOccurrencesByRuleIDCache != null ? validOccurrencesByRuleIDCache : new HashMap<>();
    }
    
    /**
     * Gets the map of invalid occurrences by deserializing from JSON.
     * Results are cached for performance.
     * 
     * @return map of invalid occurrences by rule ID
     */
    public Map<String, List<String>> getInvalidOccurrencesByRuleID() {
        if (invalidOccurrencesByRuleIDCache == null && invalidOccurrencesByRuleIDJson != null) {
            invalidOccurrencesByRuleIDCache = deserializeMapFromJson(invalidOccurrencesByRuleIDJson);
        }
        return invalidOccurrencesByRuleIDCache != null ? invalidOccurrencesByRuleIDCache : new HashMap<>();
    }
    
    /**
     * Sets the valid occurrences map and updates the JSON representation.
     * 
     * @param validOccurrencesByRuleID map of valid occurrences
     */
    public void setValidOccurrencesByRuleID(Map<String, List<String>> validOccurrencesByRuleID) {
        this.validOccurrencesByRuleIDCache = validOccurrencesByRuleID;
        this.validOccurrencesByRuleIDJson = serializeMapToJson(validOccurrencesByRuleID);
    }
    
    /**
     * Sets the invalid occurrences map and updates the JSON representation.
     * 
     * @param invalidOccurrencesByRuleID map of invalid occurrences
     */
    public void setInvalidOccurrencesByRuleID(Map<String, List<String>> invalidOccurrencesByRuleID) {
        this.invalidOccurrencesByRuleIDCache = invalidOccurrencesByRuleID;
        this.invalidOccurrencesByRuleIDJson = serializeMapToJson(invalidOccurrencesByRuleID);
    }
    
    /**
     * Alternative constructor that accepts maps directly and converts them to JSON.
     * 
     * @param id unique identifier
     * @param identifier OAI record identifier
     * @param snapshotId snapshot ID
     * @param origin origin URL
     * @param setSpec OAI set specification
     * @param metadataPrefix metadata format prefix
     * @param networkAcronym network acronym
     * @param repositoryName repository name
     * @param institutionName institution name
     * @param isValid whether record is valid
     * @param isTransformed whether record was transformed
     * @param validOccurrencesByRuleID map of valid occurrences by rule ID
     * @param invalidOccurrencesByRuleID map of invalid occurrences by rule ID
     * @param validRulesIDList list of valid rule IDs
     * @param invalidRulesIDList list of invalid rule IDs
     */
    public ValidationStatObservation(String id, String identifier, Long snapshotId, String origin,
                                   String setSpec, String metadataPrefix, String networkAcronym,
                                   String repositoryName, String institutionName, Boolean isValid,
                                   Boolean isTransformed, Map<String, List<String>> validOccurrencesByRuleID,
                                   Map<String, List<String>> invalidOccurrencesByRuleID,
                                   List<String> validRulesIDList, List<String> invalidRulesIDList) {
        
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
        
        // Serializar los mapas y listas a JSON strings
        this.validOccurrencesByRuleIDJson = serializeMapToJson(validOccurrencesByRuleID);
        this.invalidOccurrencesByRuleIDJson = serializeMapToJson(invalidOccurrencesByRuleID);
        this.validRulesID = validRulesIDList != null ? String.join(",", validRulesIDList) : "";
        this.invalidRulesID = invalidRulesIDList != null ? String.join(",", invalidRulesIDList) : "";
        
        // Mantener referencias en caché
        this.validOccurrencesByRuleIDCache = validOccurrencesByRuleID;
        this.invalidOccurrencesByRuleIDCache = invalidOccurrencesByRuleID;
    }
    
    /**
     * Compatibility method for legacy code that uses getSnapshotID.
     * LEGACY - Use getSnapshotId() instead.
     * 
     * @return the snapshot ID
     */
    @JsonIgnore
    public Long getSnapshotID() {
        return getSnapshotId();
    }
    
    /**
     * Compatibility method for legacy code that uses setSnapshotID.
     * LEGACY - Use setSnapshotId() instead.
     * 
     * @param snapshotID the snapshot ID to set
     */
    @JsonIgnore
    public void setSnapshotID(Long snapshotID) {
        setSnapshotId(snapshotID);
    }
    
    /**
     * Gets the list of valid rule IDs as a list (serialized as `validRulesID`).
     *
     * @return list of valid rule IDs
     */
    @JsonProperty("validRulesID")
    public List<String> getValidRulesIDList() {
        return getValidRulesList();
    }
    
    /**
     * Gets the list of invalid rule IDs as a list (serialized as `invalidRulesID`).
     *
     * @return list of invalid rule IDs
     */
    @JsonProperty("invalidRulesID")
    public List<String> getInvalidRulesIDList() {
        return getInvalidRulesList();
    }
    
    /**
     * Serializes a map to JSON simple format (key:value1,value2;key2:value3,value4).
     */
    private String serializeMapToJson(Map<String, List<String>> map) {
        if (map == null || map.isEmpty()) {
            return "";
        }
        
        StringBuilder sb = new StringBuilder();
        for (Map.Entry<String, List<String>> entry : map.entrySet()) {
            if (sb.length() > 0) {
                sb.append(";");
            }
            sb.append(entry.getKey()).append(":");
            if (entry.getValue() != null && !entry.getValue().isEmpty()) {
                sb.append(String.join(",", entry.getValue()));
            }
        }
        return sb.toString();
    }
    
    /**
     * Deserializes a JSON string to map.
     */
    private Map<String, List<String>> deserializeMapFromJson(String json) {
        Map<String, List<String>> map = new HashMap<>();
        if (json == null || json.trim().isEmpty()) {
            return map;
        }
        
        String[] entries = json.split(";");
        for (String entry : entries) {
            String[] keyValue = entry.split(":", 2);
            if (keyValue.length == 2) {
                String key = keyValue[0];
                String[] values = keyValue[1].split(",");
                List<String> valueList = new ArrayList<>();
                for (String value : values) {
                    if (!value.trim().isEmpty()) {
                        valueList.add(value.trim());
                    }
                }
                map.put(key, valueList);
            }
        }
        return map;
    }
    
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ValidationStatObservation that = (ValidationStatObservation) o;
        return Objects.equals(id, that.id) && Objects.equals(identifier, that.identifier);
    }
    
    @Override
    public int hashCode() {
        return Objects.hash(id, identifier);
    }
    
    @Override
    public String toString() {
        return "ValidationStatObservation{" +
                "id='" + id + '\'' +
                ", identifier='" + identifier + '\'' +
                ", snapshotId=" + snapshotId +
                ", isValid=" + isValid +
                ", isTransformed=" + isTransformed +
                '}';
    }
}
