package org.lareferencia.core.domain;

import java.time.Instant;

import com.fasterxml.jackson.annotation.JsonFormat;

/** Latest completed attempt for one indexing worker bean. */
public record IndexingResult(SnapshotIndexStatus status, String actionName,
        @JsonFormat(shape = JsonFormat.Shape.STRING) Instant finishedAt, String error) {
    public IndexingResult {
        if (status != SnapshotIndexStatus.INDEXED && status != SnapshotIndexStatus.FAILED) {
            throw new IllegalArgumentException("An indexing result must be INDEXED or FAILED");
        }
        if (finishedAt == null) throw new IllegalArgumentException("Completion time is required");
        error = status == SnapshotIndexStatus.INDEXED ? null
                : error == null || error.isBlank() ? "Indexing did not complete" : error;
        if (error != null && error.length() > 1000) error = error.substring(0, 1000);
    }
}
