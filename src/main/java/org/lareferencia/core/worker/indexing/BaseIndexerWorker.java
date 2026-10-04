package org.lareferencia.core.worker.indexing;

import java.time.Instant;

import org.lareferencia.core.domain.IndexingResult;
import org.lareferencia.core.domain.SnapshotIndexStatus;
import org.lareferencia.core.repository.validation.ValidationRecord;
import org.lareferencia.core.service.management.SnapshotIndexingService;
import org.lareferencia.core.worker.BaseBatchWorker;
import org.lareferencia.core.worker.NetworkRunningContext;
import org.springframework.beans.factory.BeanNameAware;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

/** Records one terminal result even when a batch stops before postRun(). */
public abstract class BaseIndexerWorker extends BaseBatchWorker<ValidationRecord, NetworkRunningContext>
        implements BeanNameAware {
    @Autowired private SnapshotIndexingService indexingResults;
    private String workerBeanName;
    private Long indexingSnapshotId;
    private long indexingGeneration;
    private boolean committed;

    @Override
    public final void setBeanName(String name) { workerBeanName = name; }

    protected final void beginIndexing(Long snapshotId) {
        if (workerBeanName == null || workerBeanName.isBlank()) {
            throw new IllegalStateException("Indexing worker has no Spring bean name");
        }
        indexingGeneration = indexingResults.generation(snapshotId);
        indexingSnapshotId = snapshotId;
    }

    protected final void indexingCommitted() { committed = true; }

    @Override
    @Transactional(propagation = Propagation.NOT_SUPPORTED)
    public synchronized void run() {
        if (isCancellationRequested()) return; // A queued cancellation has no result.
        indexingSnapshotId = null;
        committed = false;
        try {
            super.run();
            if (indexingSnapshotId != null && !committed && !isCancellationRequested()
                    && getExecutionFailure() == null) recordExecutionFailure("Indexing did not complete its commit");
            // Flowable also needs an exception for failures handled inside processItem().
            if (getExecutionFailure() != null) throw new IllegalStateException(getExecutionFailure());
        } catch (RuntimeException | Error failure) {
            if (getExecutionFailure() == null) recordExecutionFailure(failure);
            throw failure;
        } finally {
            if (indexingSnapshotId != null) {
                String error = getExecutionFailure();
                if (error == null && isCancellationRequested()) error = "Indexing cancelled";
                if (error == null && !committed) error = "Indexing did not complete its commit";
                IndexingResult result = new IndexingResult(error == null ? SnapshotIndexStatus.INDEXED
                        : SnapshotIndexStatus.FAILED, runningContext.getActionKey(), Instant.now(), error);
                // Cancellation interrupts the runner. Persist its result before restoring that token.
                boolean interrupted = Thread.interrupted();
                try {
                    indexingResults.record(indexingSnapshotId, indexingGeneration, workerBeanName, result);
                } catch (RuntimeException persistenceFailure) {
                    recordExecutionFailure(persistenceFailure);
                    throw persistenceFailure;
                } finally {
                    if (interrupted) Thread.currentThread().interrupt();
                }
            }
        }
    }
}
