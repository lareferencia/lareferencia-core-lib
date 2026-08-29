package org.lareferencia.core.task;

/** Stable policy error translated by API v5 while remaining usable by legacy callers. */
public class ApplicationActionPolicyException extends RuntimeException {
    private final String code;

    public ApplicationActionPolicyException(String code, String message) {
        super(message);
        this.code = code;
    }

    public String getCode() {
        return code;
    }
}
