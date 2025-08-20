package org.lareferencia.backend.services.validation;

import org.lareferencia.backend.domain.validation.ValidationStatObservation;
import org.lareferencia.backend.domain.validation.ValidationStatsQueryResult;
import org.springframework.data.domain.Pageable;

import java.util.List;
import java.util.Map;

/**
 * Interface para repositorios de estadísticas de validación
 * Permite abstraer la implementación de almacenamiento (Parquet, DB, etc.)
 */
public interface IValidationStatisticsRepository {
    
    /**
     * Guarda una lista de observaciones
     * @param observations Lista de observaciones a guardar
     * @throws ValidationStatisticsException Si ocurre un error
     */
    void saveAll(List<ValidationStatObservation> observations) throws ValidationStatisticsException;
    
    /**
     * Guarda una observación individual
     * @param observation Observación a guardar
     * @throws ValidationStatisticsException Si ocurre un error
     */
    void save(ValidationStatObservation observation) throws ValidationStatisticsException;
    
    /**
     * Busca observaciones por snapshot ID con filtros y paginación
     * @param snapshotId ID del snapshot
     * @param filters Mapa de filtros
     * @param pageable Configuración de paginación
     * @return Resultado de la consulta
     * @throws ValidationStatisticsException Si ocurre un error
     */
    ValidationStatsQueryResult findBySnapshotIdWithFilters(Long snapshotId, 
            Map<String, Object> filters, Pageable pageable) throws ValidationStatisticsException;
    
    /**
     * Busca todas las observaciones por snapshot ID
     * @param snapshotId ID del snapshot
     * @return Lista de observaciones
     * @throws ValidationStatisticsException Si ocurre un error
     */
    List<ValidationStatObservation> findBySnapshotId(Long snapshotId) throws ValidationStatisticsException;
    
    /**
     * Elimina observaciones por snapshot ID
     * @param snapshotId ID del snapshot
     * @throws ValidationStatisticsException Si ocurre un error
     */
    void deleteBySnapshotId(Long snapshotId) throws ValidationStatisticsException;
    
    /**
     * Cuenta el número de observaciones por snapshot ID
     * @param snapshotId ID del snapshot
     * @return Número de observaciones
     * @throws ValidationStatisticsException Si ocurre un error
     */
    long countBySnapshotId(Long snapshotId) throws ValidationStatisticsException;
    
    /**
     * Verifica si existen datos para un snapshot
     * @param snapshotId ID del snapshot
     * @return true si existen datos
     * @throws ValidationStatisticsException Si ocurre un error
     */
    boolean existsBySnapshotId(Long snapshotId) throws ValidationStatisticsException;
    
    /**
     * Obtiene agregaciones por reglas de validación
     * @param snapshotId ID del snapshot
     * @param filters Filtros aplicados
     * @return Mapa con agregaciones
     * @throws ValidationStatisticsException Si ocurre un error
     */
    Map<String, Object> getValidationRulesAggregations(Long snapshotId, 
            Map<String, Object> filters) throws ValidationStatisticsException;
    
    /**
     * Obtiene estadísticas generales del repositorio
     * @return Mapa con estadísticas
     */
    Map<String, Object> getRepositoryStats();
    
    /**
     * Verifica la salud del repositorio
     * @return true si el repositorio está saludable
     */
    boolean isHealthy();
}
