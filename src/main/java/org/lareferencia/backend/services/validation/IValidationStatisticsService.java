package org.lareferencia.backend.services.validation;

import org.lareferencia.backend.domain.validation.ValidationStatObservation;
import org.lareferencia.backend.domain.validation.ValidationStatsQueryResult;
import org.springframework.data.domain.Pageable;

import java.util.List;
import java.util.Map;

/**
 * Interface principal para servicios de estadísticas de validación
 * Permite abstraer la implementación (Parquet, Solr, DB, etc.)
 */
public interface IValidationStatisticsService {
    
    /**
     * Guarda observaciones de estadísticas de validación
     * @param observations Lista de observaciones a guardar
     * @throws ValidationStatisticsException Si ocurre un error durante el guardado
     */
    void saveValidationStatObservations(List<ValidationStatObservation> observations) throws ValidationStatisticsException;
    
    /**
     * Consulta observaciones por snapshot ID con filtros y paginación
     * @param snapshotID ID del snapshot
     * @param filters Lista de filtros en formato "campo@@valor"
     * @param pageable Configuración de paginación
     * @return Resultado de la consulta con datos paginados
     * @throws ValidationStatisticsException Si ocurre un error durante la consulta
     */
    ValidationStatsQueryResult queryValidationStatsObservationsBySnapshotID(
        Long snapshotID, 
        List<String> filters, 
        Pageable pageable
    ) throws ValidationStatisticsException;
    
    /**
     * Consulta estadísticas de reglas de validación por snapshot
     * @param snapshotID ID del snapshot
     * @param filters Lista de filtros aplicados
     * @return Estadísticas agregadas por reglas
     * @throws ValidationStatisticsException Si ocurre un error durante la consulta
     */
    ValidationStatsQueryResult queryValidatorRulesStatsBySnapshot(
        Long snapshotID, 
        List<String> filters
    ) throws ValidationStatisticsException;
    
    /**
     * Elimina observaciones por snapshot ID
     * @param snapshotID ID del snapshot a eliminar
     * @throws ValidationStatisticsException Si ocurre un error durante la eliminación
     */
    void deleteValidationStatsObservationsBySnapshotID(Long snapshotID) throws ValidationStatisticsException;
    
    /**
     * Inicializa una nueva validación para un snapshot específico
     * Este método debe ser llamado al iniciar un nuevo proceso de validación
     * para garantizar un estado limpio y eliminar datos previos de validación
     * @param snapshotId ID del snapshot para el cual inicializar la validación
     * @throws ValidationStatisticsException Si ocurre un error durante la inicialización
     */
    void initializeValidationForSnapshot(Long snapshotId) throws ValidationStatisticsException;
    
    /**
     * Verifica si el servicio está disponible y funcionando
     * @return true si el servicio está disponible
     */
    boolean isServiceAvailable();
    
    /**
     * Obtiene el tipo de implementación (parquet, solr, database, etc.)
     * @return Nombre del tipo de implementación
     */
    String getImplementationType();
    
    /**
     * Obtiene métricas de performance del servicio
     * @return Mapa con métricas de performance
     */
    Map<String, Object> getPerformanceMetrics();
    
    /**
     * Valida los filtros proporcionados
     * @param filters Lista de filtros a validar
     * @return true si todos los filtros son válidos
     */
    boolean validateFilters(List<String> filters);
}
