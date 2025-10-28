package org.lareferencia.backend.validation;

/**
 * Specific exception for validation statistics service errors.
 */
public class ValidationStatisticsException extends Exception {
    
    private static final long serialVersionUID = 1L;
    
    /**
     * Error code identifying the type of error.
     */
    private final String errorCode;
    
    /**
     * The operation that caused the exception.
     */
    private final String operation;
    
    /**
     * Constructs a new ValidationStatisticsException with a message.
     *
     * @param message the detail message
     */
    public ValidationStatisticsException(String message) {
        super(message);
        this.errorCode = "GENERAL_ERROR";
        this.operation = "UNKNOWN";
    }
    
    /**
     * Constructs a new ValidationStatisticsException with a message and cause.
     *
     * @param message the detail message
     * @param cause the cause of the exception
     */
    public ValidationStatisticsException(String message, Throwable cause) {
        super(message, cause);
        this.errorCode = "GENERAL_ERROR";
        this.operation = "UNKNOWN";
    }
    
    /**
     * Constructs a new ValidationStatisticsException with error code, operation, and message.
     *
     * @param errorCode the error code
     * @param operation the operation being performed
     * @param message the detail message
     */
    public ValidationStatisticsException(String errorCode, String operation, String message) {
        super(message);
        this.errorCode = errorCode;
        this.operation = operation;
    }
    
    /**
     * Constructs a new ValidationStatisticsException with all details.
     *
     * @param errorCode the error code
     * @param operation the operation being performed
     * @param message the detail message
     * @param cause the cause of the exception
     */
    public ValidationStatisticsException(String errorCode, String operation, String message, Throwable cause) {
        super(message, cause);
        this.errorCode = errorCode;
        this.operation = operation;
    }
    
    /**
     * Gets the error code identifying the type of error.
     *
     * @return the error code
     */
    public String getErrorCode() {
        return errorCode;
    }
    
    /**
     * Gets the operation that caused the exception.
     *
     * @return the operation name
     */
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
    
    /**
     * Creates a file not found exception.
     *
     * @param operation the operation being performed
     * @param filename the name of the file that was not found
     * @return a new ValidationStatisticsException instance
     */
    public static ValidationStatisticsException fileNotFound(String operation, String filename) {
        return new ValidationStatisticsException("FILE_NOT_FOUND", operation, 
            "Archivo no encontrado: " + filename);
    }
    
    /**
     * Creates an invalid filter exception.
     *
     * @param operation the operation being performed
     * @param filter the invalid filter description
     * @return a new ValidationStatisticsException instance
     */
    public static ValidationStatisticsException invalidFilter(String operation, String filter) {
        return new ValidationStatisticsException("INVALID_FILTER", operation, 
            "Filtro inválido: " + filter);
    }
    
    /**
     * Creates an I/O error exception.
     *
     * @param operation the operation being performed
     * @param cause the underlying I/O exception
     * @return a new ValidationStatisticsException instance
     */
    public static ValidationStatisticsException ioError(String operation, Throwable cause) {
        return new ValidationStatisticsException("IO_ERROR", operation, 
            "Error de E/S durante " + operation, cause);
    }
    
    /**
     * Creates a service unavailable exception.
     *
     * @param operation the operation being performed
     * @return a new ValidationStatisticsException instance
     */
    public static ValidationStatisticsException serviceUnavailable(String operation) {
        return new ValidationStatisticsException("SERVICE_UNAVAILABLE", operation, 
            "Servicio no disponible para: " + operation);
    }
}
