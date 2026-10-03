package org.lareferencia.core.task;

/** Stable admission failure consumed by HTTP and scheduled callers. */
public class TaskSubmissionRejectedException extends RuntimeException {
    private final String reason;

    public TaskSubmissionRejectedException(String reason) {
        super("Task plan rejected: " + reason);
        this.reason = reason;
    }

    public String getReason() { return reason; }
}
