package org.lareferencia.core.service.validation;

import org.springframework.data.domain.Pageable;

import java.util.List;
import java.util.Map;

/**
 * DTO for validation statistics query results.
 * Encapsulates both paginated data and query metadata.
 */
public class ValidationStatsObservationsResult {
    
    /**
     * The list of validation stat observations for the current page.
     */
    private List<ValidationStatObservation> content;
    
    /**
     * Total number of elements across all pages.
     */
    private long totalElements;
    
    /**
     * Total number of pages.
     */
    private int totalPages;
    
    /**
     * Current page number (zero-based).
     */
    private int currentPage;
    
    /**
     * Number of elements per page.
     */
    private int pageSize;
    
    /**
     * Whether there is a next page available.
     */
    private boolean hasNext;
    
    /**
     * Whether there is a previous page available.
     */
    private boolean hasPrevious;
    
    /**
     * Aggregation results (e.g., counts by rule).
     */
    private Map<String, Object> aggregations;
    
    /**
     * Additional metadata about the query.
     */
    private Map<String, Object> metadata;
    
    /**
     * Query execution time in milliseconds.
     */
    private long executionTimeMs;
    
    /**
     * Constructs a new ValidationStatsObservationsResult with default values.
     */
    public ValidationStatsObservationsResult() {}
    
    /**
     * Constructs a paginated validation statistics result.
     *
     * @param content the list of observations for the current page
     * @param totalElements the total number of elements across all pages
     * @param pageable the pagination information
     */
    public ValidationStatsObservationsResult(List<ValidationStatObservation> content, 
            long totalElements, Pageable pageable) {
        this.content = content;
        this.totalElements = totalElements;
        this.pageSize = pageable.getPageSize();
        this.currentPage = pageable.getPageNumber();
        this.totalPages = (int) Math.ceil((double) totalElements / pageSize);
        this.hasNext = currentPage < totalPages - 1;
        this.hasPrevious = currentPage > 0;
    }
    
    /**
     * Constructs a complete validation statistics result with all fields.
     *
     * @param content the list of observations for the current page
     * @param totalElements the total number of elements
     * @param totalPages the total number of pages
     * @param currentPage the current page number
     * @param pageSize the number of elements per page
     * @param hasNext whether there is a next page
     * @param hasPrevious whether there is a previous page
     * @param aggregations aggregation results
     * @param metadata additional query metadata
     * @param executionTimeMs query execution time in milliseconds
     */
    public ValidationStatsObservationsResult(List<ValidationStatObservation> content,
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
    
    /**
     * Gets the list of validation stat observations for the current page.
     *
     * @return the content list
     */
    public List<ValidationStatObservation> getContent() { return content; }
    
    /**
     * Sets the list of validation stat observations for the current page.
     *
     * @param content the content list
     */
    public void setContent(List<ValidationStatObservation> content) { 
        this.content = content; 
    }
    
    /**
     * Gets the total number of elements across all pages.
     *
     * @return the total element count
     */
    public long getTotalElements() { return totalElements; }
    
    /**
     * Sets the total number of elements across all pages.
     *
     * @param totalElements the total element count
     */
    public void setTotalElements(long totalElements) { this.totalElements = totalElements; }
    
    /**
     * Gets the total number of pages.
     *
     * @return the total page count
     */
    public int getTotalPages() { return totalPages; }
    
    /**
     * Sets the total number of pages.
     *
     * @param totalPages the total page count
     */
    public void setTotalPages(int totalPages) { this.totalPages = totalPages; }
    
    /**
     * Gets the current page number (zero-based).
     *
     * @return the current page number
     */
    public int getCurrentPage() { return currentPage; }
    
    /**
     * Sets the current page number (zero-based).
     *
     * @param currentPage the current page number
     */
    public void setCurrentPage(int currentPage) { this.currentPage = currentPage; }
    
    /**
     * Gets the number of elements per page.
     *
     * @return the page size
     */
    public int getPageSize() { return pageSize; }
    
    /**
     * Sets the number of elements per page.
     *
     * @param pageSize the page size
     */
    public void setPageSize(int pageSize) { this.pageSize = pageSize; }
    
    /**
     * Checks if there is a next page available.
     *
     * @return true if there is a next page, false otherwise
     */
    public boolean isHasNext() { return hasNext; }
    
    /**
     * Sets whether there is a next page available.
     *
     * @param hasNext true if there is a next page
     */
    public void setHasNext(boolean hasNext) { this.hasNext = hasNext; }
    
    /**
     * Checks if there is a previous page available.
     *
     * @return true if there is a previous page, false otherwise
     */
    public boolean isHasPrevious() { return hasPrevious; }
    
    /**
     * Sets whether there is a previous page available.
     *
     * @param hasPrevious true if there is a previous page
     */
    public void setHasPrevious(boolean hasPrevious) { this.hasPrevious = hasPrevious; }
    
    /**
     * Gets the aggregation results (e.g., counts by rule).
     *
     * @return the aggregations map
     */
    public Map<String, Object> getAggregations() { return aggregations; }
    
    /**
     * Sets the aggregation results.
     *
     * @param aggregations the aggregations map
     */
    public void setAggregations(Map<String, Object> aggregations) { this.aggregations = aggregations; }
    
    /**
     * Gets additional metadata about the query.
     *
     * @return the metadata map
     */
    public Map<String, Object> getMetadata() { return metadata; }
    
    /**
     * Sets additional metadata about the query.
     *
     * @param metadata the metadata map
     */
    public void setMetadata(Map<String, Object> metadata) { this.metadata = metadata; }
    
    /**
     * Gets the query execution time in milliseconds.
     *
     * @return the execution time in milliseconds
     */
    public long getExecutionTimeMs() { return executionTimeMs; }
    
    /**
     * Sets the query execution time in milliseconds.
     *
     * @param executionTimeMs the execution time in milliseconds
     */
    public void setExecutionTimeMs(long executionTimeMs) { this.executionTimeMs = executionTimeMs; }
    
    /**
     * Checks if the result set is empty.
     *
     * @return true if the content is null or empty, false otherwise
     */
    /**
     * Checks if the result set is empty.
     *
     * @return true if the content is null or empty, false otherwise
     */
    public boolean isEmpty() {
        return content == null || content.isEmpty();
    }
    
    /**
     * Gets the number of elements in the current page.
     *
     * @return the number of elements in the content list, or 0 if null
     */
    public int getNumberOfElements() {
        return content != null ? content.size() : 0;
    }
    
    /**
     * Checks if this is the first page.
     *
     * @return true if the current page is 0, false otherwise
     */
    public boolean isFirst() {
        return currentPage == 0;
    }
    
    /**
     * Checks if this is the last page.
     *
     * @return true if the current page is the last page, false otherwise
     */
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
