package org.lareferencia.backend.services.validation;

import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Component;

import java.util.Map;

/**
 * Factory para crear instancias de servicios de estadísticas de validación
 * según la configuración especificada
 */
@Component
public class ValidationStatisticsServiceFactory {
    
    private final ApplicationContext applicationContext;
    private final ValidationStatisticsConfig defaultConfig;
    
    public ValidationStatisticsServiceFactory(ApplicationContext applicationContext) {
        this.applicationContext = applicationContext;
        this.defaultConfig = new ValidationStatisticsConfig();
    }
    
    /**
     * Crea un servicio usando la configuración por defecto
     */
    public IValidationStatisticsService createService() {
        return createService(defaultConfig);
    }
    
    /**
     * Crea un servicio usando la configuración especificada
     */
    public IValidationStatisticsService createService(ValidationStatisticsConfig config) {
        try {
            switch (config.getImplementationType()) {
                case PARQUET:
                    return createParquetService(config);
                case SOLR:
                    return createSolrService(config);
                case DATABASE:
                    return createDatabaseService(config);
                case ELASTICSEARCH:
                    return createElasticsearchService(config);
                default:
                    throw new IllegalArgumentException("Tipo de implementación no soportado: " + 
                        config.getImplementationType());
            }
        } catch (Exception e) {
            throw new RuntimeException("Error creando servicio de estadísticas de validación", e);
        }
    }
    
    /**
     * Crea un servicio por tipo de implementación
     */
    public IValidationStatisticsService createService(String implementationType) {
        ValidationStatisticsConfig config = new ValidationStatisticsConfig();
        config.setImplementationType(ValidationStatisticsConfig.ImplementationType.fromString(implementationType));
        return createService(config);
    }
    
    /**
     * Obtiene el servicio configurado por defecto del contexto de Spring
     */
    public IValidationStatisticsService getDefaultService() {
        try {
            return applicationContext.getBean(IValidationStatisticsService.class);
        } catch (Exception e) {
            // Si no hay bean configurado, crear uno por defecto
            return createService();
        }
    }
    
    /**
     * Lista todos los servicios disponibles
     */
    public Map<String, IValidationStatisticsService> getAllServices() {
        return applicationContext.getBeansOfType(IValidationStatisticsService.class);
    }
    
    private IValidationStatisticsService createParquetService(ValidationStatisticsConfig config) {
        // Buscar implementación Parquet en el contexto
        try {
            return applicationContext.getBean("validationStatisticsParquetService", IValidationStatisticsService.class);
        } catch (Exception e) {
            throw new RuntimeException("Servicio Parquet no disponible. Asegúrese de que la implementación esté en el classpath.", e);
        }
    }
    
    private IValidationStatisticsService createSolrService(ValidationStatisticsConfig config) {
        try {
            return applicationContext.getBean("validationStatisticsSolrService", IValidationStatisticsService.class);
        } catch (Exception e) {
            throw new RuntimeException("Servicio Solr no disponible. Asegúrese de que la implementación esté en el classpath.", e);
        }
    }
    
    private IValidationStatisticsService createDatabaseService(ValidationStatisticsConfig config) {
        try {
            return applicationContext.getBean("validationStatisticsDatabaseService", IValidationStatisticsService.class);
        } catch (Exception e) {
            throw new RuntimeException("Servicio Database no disponible. Asegúrese de que la implementación esté en el classpath.", e);
        }
    }
    
    private IValidationStatisticsService createElasticsearchService(ValidationStatisticsConfig config) {
        try {
            return applicationContext.getBean("validationStatisticsElasticsearchService", IValidationStatisticsService.class);
        } catch (Exception e) {
            throw new RuntimeException("Servicio Elasticsearch no disponible. Asegúrese de que la implementación esté en el classpath.", e);
        }
    }
}
