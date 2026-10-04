package org.lareferencia.core.worker.indexing;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.lareferencia.core.domain.IndexingResult;
import org.lareferencia.core.domain.Network;
import org.lareferencia.core.domain.SnapshotIndexStatus;
import org.lareferencia.core.repository.validation.ValidationRecord;
import org.lareferencia.core.service.management.SnapshotIndexingService;
import org.lareferencia.core.worker.IPaginator;
import org.lareferencia.core.worker.NetworkRunningContext;
import org.mockito.ArgumentCaptor;
import org.springframework.data.domain.PageImpl;
import org.springframework.test.util.ReflectionTestUtils;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.TransactionStatus;

import java.util.List;

class BaseIndexerWorkerTest {
    private final SnapshotIndexingService results = mock(SnapshotIndexingService.class);
    private final TestIndexer worker = new TestIndexer();

    @BeforeEach void prepare() {
        worker.setBeanName("frontendIndexerWorker");
        worker.setRunningContext(new NetworkRunningContext(new Network(), "FRONTEND_INDEXING_ACTION", null));
        ReflectionTestUtils.setField(worker, "indexingResults", results);
        when(results.generation(42L)).thenReturn(7L);
        PlatformTransactionManager transactions = mock(PlatformTransactionManager.class);
        when(transactions.getTransaction(any(TransactionDefinition.class))).thenReturn(mock(TransactionStatus.class));
        ReflectionTestUtils.setField(worker, "transactionManager", transactions);
        @SuppressWarnings("unchecked") IPaginator<ValidationRecord> paginator = mock(IPaginator.class);
        when(paginator.getTotalPages()).thenReturn(1);
        when(paginator.getStartingPage()).thenReturn(1);
        when(paginator.nextPage()).thenReturn(new PageImpl<>(List.of(mock(ValidationRecord.class))));
        worker.setPaginator(paginator);
    }

    @Test void successIsAttributedToTheBeanAndActionAfterCommit() {
        worker.run();
        IndexingResult result = recorded();
        assertEquals(SnapshotIndexStatus.INDEXED, result.status());
        assertEquals("FRONTEND_INDEXING_ACTION", result.actionName());
        assertNull(result.error());
    }

    @Test void cancellationBeforeEntryHasNoResult() {
        worker.stop();
        worker.run();
        verifyNoInteractions(results);
    }

    @Test void cancellationAfterStartRecordsFailureEvenWithoutPostRun() {
        worker.cancelInPage = true;
        worker.run();
        assertEquals(SnapshotIndexStatus.FAILED, recorded().status());
        assertEquals("Indexing cancelled", recorded().error());
    }

    @Test void preparationFailureStillHasAResult() {
        worker.failPreparation = true;
        assertThrows(IllegalArgumentException.class, worker::run);
        assertEquals(SnapshotIndexStatus.FAILED, recorded().status());
    }

    @Test void partialFailureCannotBecomeSuccessAfterCommit() {
        worker.partialFailure = true;
        assertThrows(IllegalStateException.class, worker::run);
        assertEquals(SnapshotIndexStatus.FAILED, recorded().status());
        assertEquals("Embedding generation failed", recorded().error());
    }

    @Test void absentCommitCannotBeReportedAsSuccess() {
        worker.skipCommit = true;
        assertThrows(IllegalStateException.class, worker::run);
        assertEquals(SnapshotIndexStatus.FAILED, recorded().status());
    }

    @Test void interruptedCancellationRestoresInterruptAfterPersisting() {
        worker.interruptInPage = true;
        doAnswer(invocation -> {
            assertFalse(Thread.currentThread().isInterrupted());
            return true;
        }).when(results).record(eq(42L), eq(7L), anyString(), any());
        try {
            worker.run();
            assertTrue(Thread.currentThread().isInterrupted());
            assertEquals(SnapshotIndexStatus.FAILED, recorded().status());
        } finally { Thread.interrupted(); }
    }

    private IndexingResult recorded() {
        ArgumentCaptor<IndexingResult> result = ArgumentCaptor.forClass(IndexingResult.class);
        verify(results, atLeastOnce()).record(eq(42L), eq(7L), eq("frontendIndexerWorker"), result.capture());
        return result.getValue();
    }

    private static class TestIndexer extends BaseIndexerWorker {
        boolean cancelInPage, failPreparation, partialFailure, skipCommit, interruptInPage;
        @Override public void preRun() {
            beginIndexing(42L);
            if (failPreparation) throw new IllegalArgumentException("Transformer unavailable");
        }
        @Override public void prePage() { }
        @Override public void processItem(ValidationRecord item) {
            if (partialFailure) recordExecutionFailure("Embedding generation failed");
            if (cancelInPage) stop();
            if (interruptInPage) Thread.currentThread().interrupt();
        }
        @Override public void postPage() { }
        @Override public void postRun() { if (!skipCommit) indexingCommitted(); }
    }
}
