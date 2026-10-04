package org.lareferencia.core.service.management;

import java.util.List;

import org.lareferencia.core.domain.IndexingResult;
import org.lareferencia.core.domain.SnapshotIndexingResults;
import org.lareferencia.core.domain.SnapshotIndexStatus;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import jakarta.persistence.EntityManager;
import jakarta.persistence.PersistenceContext;

/**
 * The only writer of snapshot indexing columns. Reads bypass SnapshotStore's
 * cache; short row locks preserve results from different workers. Completion has
 * its own transaction so a failed or cancelled page cannot roll its result back.
 */
@Service
public class SnapshotIndexingService {
    @PersistenceContext private EntityManager entityManager;
    private final ObjectMapper mapper;

    public SnapshotIndexingService(ObjectMapper mapper) { this.mapper = mapper; }

    @Transactional(propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public long generation(Long snapshotId) {
        return read(snapshotId, false).generation();
    }

    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public boolean record(Long snapshotId, long generation, String workerBeanName, IndexingResult result) {
        if (workerBeanName == null || workerBeanName.isBlank()) {
            throw new IllegalArgumentException("The indexing worker bean name is required");
        }
        SnapshotIndexingResults state = read(snapshotId, true);
        if (state.generation() != generation) return false; // Revalidation cleared this attempt's results.
        write(snapshotId, state.withResult(workerBeanName, result));
        return true;
    }

    /** Joins the validation transaction: rollback preserves both counts and results. */
    @Transactional
    public SnapshotIndexingResults reset(Long snapshotId) {
        SnapshotIndexingResults state = read(snapshotId, true).reset();
        write(snapshotId, state);
        return state;
    }

    /** Compatibility for callers that do not yet supply a worker identity. */
    @Transactional
    public SnapshotIndexStatus markLegacyAsIndexed(Long snapshotId) {
        SnapshotIndexingResults state = read(snapshotId, true);
        SnapshotIndexStatus status = state.results().isEmpty() ? SnapshotIndexStatus.INDEXED : state.summary();
        entityManager.createNativeQuery("update networksnapshot set indexstatus = :status where id = :id")
                .setParameter("status", status.ordinal()).setParameter("id", snapshotId).executeUpdate();
        return status;
    }

    private SnapshotIndexingResults read(Long snapshotId, boolean lock) {
        List<?> rows = entityManager.createNativeQuery("select cast(indexing_results as text) from networksnapshot"
                + " where id = :id" + (lock ? " for update" : ""))
                .setParameter("id", snapshotId).getResultList();
        if (rows.isEmpty()) throw new IllegalArgumentException("Snapshot not found: " + snapshotId);
        if (rows.get(0) == null) return SnapshotIndexingResults.empty();
        try {
            return mapper.readValue(rows.get(0).toString(), SnapshotIndexingResults.class);
        } catch (JsonProcessingException error) {
            throw new IllegalStateException("Invalid indexing results for snapshot " + snapshotId, error);
        }
    }

    private void write(Long snapshotId, SnapshotIndexingResults state) {
        try {
            entityManager.createNativeQuery("update networksnapshot set indexing_results = cast(:results as jsonb),"
                    + " indexstatus = :status where id = :id")
                    .setParameter("results", mapper.writeValueAsString(state))
                    .setParameter("status", state.summary().ordinal()).setParameter("id", snapshotId).executeUpdate();
        } catch (JsonProcessingException error) {
            throw new IllegalStateException("Cannot serialize indexing results", error);
        }
    }
}
