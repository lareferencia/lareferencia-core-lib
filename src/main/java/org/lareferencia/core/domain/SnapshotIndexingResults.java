package org.lareferencia.core.domain;

import java.util.LinkedHashMap;
import java.util.Map;

/** A bounded map of completed results, with a token invalidated by revalidation. */
public record SnapshotIndexingResults(long generation, Map<String, IndexingResult> results) {
    public SnapshotIndexingResults {
        results = results == null ? Map.of() : Map.copyOf(results);
    }

    public static SnapshotIndexingResults empty() {
        return new SnapshotIndexingResults(0, Map.of());
    }

    public SnapshotIndexingResults withResult(String workerBeanName, IndexingResult result) {
        Map<String, IndexingResult> next = new LinkedHashMap<>(results);
        next.put(workerBeanName, result);
        return new SnapshotIndexingResults(generation, next);
    }

    public SnapshotIndexingResults reset() {
        return new SnapshotIndexingResults(Math.incrementExact(generation), Map.of());
    }

    public SnapshotIndexStatus summary() {
        if (results.isEmpty()) return SnapshotIndexStatus.UNKNOWN;
        return results.values().stream().anyMatch(result -> result.status() == SnapshotIndexStatus.FAILED)
                ? SnapshotIndexStatus.FAILED : SnapshotIndexStatus.INDEXED;
    }
}
