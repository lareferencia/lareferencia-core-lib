package org.lareferencia.backend.domain.validation;

import org.springframework.data.domain.Pageable;
import org.lareferencia.backend.domain.parquet.ValidationStatObservationParquet;

import java.util.List;
import java.util.Map;

/**
 * DTO para resultados de consultas de estadísticas de validación
 * Encapsula tanto datos paginados como metadatos de la consulta
 */
public class ValidationStatsQueryResult {
    
    private List<ValidationStatObservationParquet> content;
    private long totalElements;
    private int totalPages;
    private int currentPage;
    private int pageSize;
    private boolean hasNext;
    private boolean hasPrevious;
    private Map<String, Object> aggregations;
    private Map<String, Object> metadata;
    private long executionTimeMs;
    
    // Constructor por defecto
    public ValidationStatsQueryResult() {}
    
    // Constructor para consultas paginadas
    public ValidationStatsQueryResult(List<ValidationStatObservationParquet> content, 
            long totalElements, Pageable pageable) {
        this.content = content;
        this.totalElements = totalElements;
        this.pageSize = pageable.getPageSize();
        this.currentPage = pageable.getPageNumber();
        this.totalPages = (int) Math.ceil((double) totalElements / pageSize);
        this.hasNext = currentPage < totalPages - 1;
        this.hasPrevious = currentPage > 0;
    }
    
    // Constructor completo
    public ValidationStatsQueryResult(List<ValidationStatObservationParquet> content,
            long totalElements, int totalPages, int currentPage, int pageSize,
            boolean hasNext, boolean hasPrevious, Map<String, Object> aggregations,
            Map<String, Object> metadata, long executionTimeMs) {
        this.content = content;
        this.totalElements = totalElements;
        this.totalPages = totalPages;
        this.currentPage = currentPage;
        this.pageSize = pageSize;
        this.hasNext = hasNext;
        this.hasPrevious = hasPrevious;
        this.aggregations = aggregations;
        this.metadata = metadata;
        this.executionTimeMs = executionTimeMs;
    }
    
    // Getters y Setters
    public List<ValidationStatObservationParquet> getContent() { return content; }
    public void setContent(List<ValidationStatObservationParquet> content) { 
        this.content = content; 
    }
    
    public long getTotalElements() { return totalElements; }
    public void setTotalElements(long totalElements) { this.totalElements = totalElements; }
    
    public int getTotalPages() { return totalPages; }
    public void setTotalPages(int totalPages) { this.totalPages = totalPages; }
    
    public int getCurrentPage() { return currentPage; }
    public void setCurrentPage(int currentPage) { this.currentPage = currentPage; }
    
    public int getPageSize() { return pageSize; }
    public void setPageSize(int pageSize) { this.pageSize = pageSize; }
    
    public boolean isHasNext() { return hasNext; }
    public void setHasNext(boolean hasNext) { this.hasNext = hasNext; }
    
    public boolean isHasPrevious() { return hasPrevious; }
    public void setHasPrevious(boolean hasPrevious) { this.hasPrevious = hasPrevious; }
    
    public Map<String, Object> getAggregations() { return aggregations; }
    public void setAggregations(Map<String, Object> aggregations) { this.aggregations = aggregations; }
    
    public Map<String, Object> getMetadata() { return metadata; }
    public void setMetadata(Map<String, Object> metadata) { this.metadata = metadata; }
    
    public long getExecutionTimeMs() { return executionTimeMs; }
    public void setExecutionTimeMs(long executionTimeMs) { this.executionTimeMs = executionTimeMs; }
    
    // Métodos de utilidad
    public boolean isEmpty() {
        return content == null || content.isEmpty();
    }
    
    public int getNumberOfElements() {
        return content != null ? content.size() : 0;
    }
    
    public boolean isFirst() {
        return currentPage == 0;
    }
    
    public boolean isLast() {
        return currentPage == totalPages - 1;
    }
    
    @Override
    public String toString() {
        return "ValidationStatsQueryResult{" +
                "contentCount=" + getNumberOfElements() +
                ", totalElements=" + totalElements +
                ", totalPages=" + totalPages +
                ", currentPage=" + currentPage +
                ", pageSize=" + pageSize +
                ", executionTimeMs=" + executionTimeMs +
                '}';
    }
}
