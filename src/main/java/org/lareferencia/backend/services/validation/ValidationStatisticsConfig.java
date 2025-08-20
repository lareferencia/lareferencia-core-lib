package org.lareferencia.backend.services.validation;

/**
 * Configuración para servicios de estadísticas de validación
 */
public class ValidationStatisticsConfig {
    
    public enum ImplementationType {
        PARQUET("parquet"),
        SOLR("solr"),
        DATABASE("database"),
        ELASTICSEARCH("elasticsearch");
        
        private final String value;
        
        ImplementationType(String value) {
            this.value = value;
        }
        
        public String getValue() {
            return value;
        }
        
        public static ImplementationType fromString(String value) {
            for (ImplementationType type : values()) {
                if (type.value.equalsIgnoreCase(value)) {
                    return type;
                }
            }
            throw new IllegalArgumentException("Unknown implementation type: " + value);
        }
    }
    
    private ImplementationType implementationType;
    private String dataPath;
    private boolean enablePerformanceMetrics;
    private boolean enableCaching;
    private int defaultPageSize;
    private int maxPageSize;
    private long queryTimeoutMs;
    
    // Constructor por defecto
    public ValidationStatisticsConfig() {
        this.implementationType = ImplementationType.PARQUET;
        this.dataPath = "/tmp/validation-stats";
        this.enablePerformanceMetrics = true;
        this.enableCaching = false;
        this.defaultPageSize = 50;
        this.maxPageSize = 1000;
        this.queryTimeoutMs = 30000; // 30 segundos
    }
    
    // Getters y Setters
    public ImplementationType getImplementationType() { return implementationType; }
    public void setImplementationType(ImplementationType implementationType) { 
        this.implementationType = implementationType; 
    }
    
    public String getDataPath() { return dataPath; }
    public void setDataPath(String dataPath) { this.dataPath = dataPath; }
    
    public boolean isEnablePerformanceMetrics() { return enablePerformanceMetrics; }
    public void setEnablePerformanceMetrics(boolean enablePerformanceMetrics) { 
        this.enablePerformanceMetrics = enablePerformanceMetrics; 
    }
    
    public boolean isEnableCaching() { return enableCaching; }
    public void setEnableCaching(boolean enableCaching) { this.enableCaching = enableCaching; }
    
    public int getDefaultPageSize() { return defaultPageSize; }
    public void setDefaultPageSize(int defaultPageSize) { this.defaultPageSize = defaultPageSize; }
    
    public int getMaxPageSize() { return maxPageSize; }
    public void setMaxPageSize(int maxPageSize) { this.maxPageSize = maxPageSize; }
    
    public long getQueryTimeoutMs() { return queryTimeoutMs; }
    public void setQueryTimeoutMs(long queryTimeoutMs) { this.queryTimeoutMs = queryTimeoutMs; }
    
    @Override
    public String toString() {
        return "ValidationStatisticsConfig{" +
                "implementationType=" + implementationType +
                ", dataPath='" + dataPath + '\'' +
                ", enablePerformanceMetrics=" + enablePerformanceMetrics +
                ", enableCaching=" + enableCaching +
                ", defaultPageSize=" + defaultPageSize +
                ", maxPageSize=" + maxPageSize +
                ", queryTimeoutMs=" + queryTimeoutMs +
                '}';
    }
}
