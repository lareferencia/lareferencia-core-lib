package org.lareferencia.backend.domain.validation;

import org.lareferencia.backend.domain.parquet.ValidationStatObservationParquet;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Utilidades para conversión entre DTOs y entidades específicas de implementación
 */
public class ValidationStatObservationMapper {
    
    /**
     * Convierte de Parquet a DTO
     */
    public static ValidationStatObservationDTO fromParquet(ValidationStatObservationParquet parquet) {
        if (parquet == null) return null;
        
        ValidationStatObservationDTO dto = new ValidationStatObservationDTO();
        dto.setId(parquet.getId());
        dto.setIdentifier(parquet.getIdentifier());
        dto.setSnapshotID(parquet.getSnapshotID());
        dto.setOrigin(parquet.getOrigin());
        dto.setSetSpec(parquet.getSetSpec());
        dto.setMetadataPrefix(parquet.getMetadataPrefix());
        dto.setNetworkAcronym(parquet.getNetworkAcronym());
        dto.setRepositoryName(parquet.getRepositoryName());
        dto.setInstitutionName(parquet.getInstitutionName());
        dto.setIsValid(parquet.getIsValid());
        dto.setIsTransformed(parquet.getIsTransformed());
        dto.setValidOccurrencesByRuleIDJson(parquet.getValidOccurrencesByRuleIDJson());
        dto.setInvalidOccurrencesByRuleIDJson(parquet.getInvalidOccurrencesByRuleIDJson());
        dto.setValidRulesID(parquet.getValidRulesIDList());
        dto.setInvalidRulesID(parquet.getInvalidRulesIDList());
        
        return dto;
    }
    
    /**
     * Convierte de DTO a Parquet
     */
    public static ValidationStatObservationParquet toParquet(ValidationStatObservationDTO dto) {
        if (dto == null) return null;
        
        ValidationStatObservationParquet parquet = new ValidationStatObservationParquet();
        parquet.setId(dto.getId());
        parquet.setIdentifier(dto.getIdentifier());
        parquet.setSnapshotID(dto.getSnapshotID());
        // ValidationDate no se almacena en Parquet, se maneja a nivel DTO
        parquet.setOrigin(dto.getOrigin());
        parquet.setSetSpec(dto.getSetSpec());
        parquet.setMetadataPrefix(dto.getMetadataPrefix());
        parquet.setNetworkAcronym(dto.getNetworkAcronym());
        parquet.setRepositoryName(dto.getRepositoryName());
        parquet.setInstitutionName(dto.getInstitutionName());
        parquet.setIsValid(dto.getIsValid());
        parquet.setIsTransformed(dto.getIsTransformed());
        parquet.setValidOccurrencesByRuleIDJson(dto.getValidOccurrencesByRuleIDJson());
        parquet.setInvalidOccurrencesByRuleIDJson(dto.getInvalidOccurrencesByRuleIDJson());
        parquet.setValidRulesID(dto.getValidRulesID() != null ? String.join(",", dto.getValidRulesID()) : "");
        parquet.setInvalidRulesID(dto.getInvalidRulesID() != null ? String.join(",", dto.getInvalidRulesID()) : "");
        
        return parquet;
    }
    
    /**
     * Convierte lista de Parquet a DTOs
     */
    public static List<ValidationStatObservationDTO> fromParquetList(List<ValidationStatObservationParquet> parquetList) {
        if (parquetList == null) return List.of();
        
        return parquetList.stream()
                .map(ValidationStatObservationMapper::fromParquet)
                .collect(Collectors.toList());
    }
    
    /**
     * Convierte lista de DTOs a Parquet
     */
    public static List<ValidationStatObservationParquet> toParquetList(List<ValidationStatObservationDTO> dtoList) {
        if (dtoList == null) return List.of();
        
        return dtoList.stream()
                .map(ValidationStatObservationMapper::toParquet)
                .collect(Collectors.toList());
    }
    
    /**
     * Crea un DTO básico para testing
     */
    public static ValidationStatObservationDTO createTestDTO(String identifier, Long snapshotId, boolean isValid) {
        ValidationStatObservationDTO dto = new ValidationStatObservationDTO();
        dto.setId("test-" + identifier);
        dto.setIdentifier(identifier);
        dto.setSnapshotID(snapshotId);
        dto.setOrigin("http://test.com");
        dto.setSetSpec("test:set");
        dto.setMetadataPrefix("oai_dc");
        dto.setNetworkAcronym("TestNetwork");
        dto.setRepositoryName("Test Repository");
        dto.setInstitutionName("Test Institution");
        dto.setIsValid(isValid);
        dto.setIsTransformed(false);
        dto.setValidOccurrencesByRuleIDJson("{}");
        dto.setInvalidOccurrencesByRuleIDJson("{}");
        dto.setValidRulesID(isValid ? List.of("1","2","3") : List.of());
        dto.setInvalidRulesID(isValid ? List.of() : List.of("4","5","6"));
        
        return dto;
    }
}
