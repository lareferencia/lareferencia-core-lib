package org.lareferencia.core.task;

import java.util.List;
import java.util.UUID;

/** Acknowledges admission of a complete plan, not completion of its workers. */
public record TaskSubmission(String groupId, boolean accepted, List<String> executionIds,
        List<TaskManager.WorkerLaunchResult> results, String reason) {
    public TaskSubmission {
        executionIds = List.copyOf(executionIds);
        results = List.copyOf(results);
    }

    public TaskSubmission requireAccepted() {
        if (!accepted) throw new TaskSubmissionRejectedException(reason);
        return this;
    }

    /** Engines retaining their own submission contract can acknowledge through the common facade. */
    public static TaskSubmission external() {
        return new TaskSubmission(UUID.randomUUID().toString(), true, List.of(), List.of(), null);
    }
}
