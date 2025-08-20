package org.lareferencia.backend.services.validation;

/**
 * Excepción específica para errores en servicios de estadísticas de validación
 */
public class ValidationStatisticsException extends Exception {
    
    private static final long serialVersionUID = 1L;
    
    private final String errorCode;
    private final String operation;
    
    public ValidationStatisticsException(String message) {
        super(message);
        this.errorCode = "GENERAL_ERROR";
        this.operation = "UNKNOWN";
    }
    
    public ValidationStatisticsException(String message, Throwable cause) {
        super(message, cause);
        this.errorCode = "GENERAL_ERROR";
        this.operation = "UNKNOWN";
    }
    
    public ValidationStatisticsException(String errorCode, String operation, String message) {
        super(message);
        this.errorCode = errorCode;
        this.operation = operation;
    }
    
    public ValidationStatisticsException(String errorCode, String operation, String message, Throwable cause) {
        super(message, cause);
        this.errorCode = errorCode;
        this.operation = operation;
    }
    
    public String getErrorCode() {
        return errorCode;
    }
    
    public String getOperation() {
        return operation;
    }
    
    @Override
    public String toString() {
        return "ValidationStatisticsException{" +
                "errorCode='" + errorCode + '\'' +
                ", operation='" + operation + '\'' +
                ", message='" + getMessage() + '\'' +
                '}';
    }
    
    // Métodos factory para errores comunes
    public static ValidationStatisticsException fileNotFound(String operation, String filename) {
        return new ValidationStatisticsException("FILE_NOT_FOUND", operation, 
            "Archivo no encontrado: " + filename);
    }
    
    public static ValidationStatisticsException invalidFilter(String operation, String filter) {
        return new ValidationStatisticsException("INVALID_FILTER", operation, 
            "Filtro inválido: " + filter);
    }
    
    public static ValidationStatisticsException ioError(String operation, Throwable cause) {
        return new ValidationStatisticsException("IO_ERROR", operation, 
            "Error de E/S durante " + operation, cause);
    }
    
    public static ValidationStatisticsException serviceUnavailable(String operation) {
        return new ValidationStatisticsException("SERVICE_UNAVAILABLE", operation, 
            "Servicio no disponible para: " + operation);
    }
}
