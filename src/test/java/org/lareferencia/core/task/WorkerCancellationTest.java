package org.lareferencia.core.task;

import org.junit.jupiter.api.Test;
import org.lareferencia.core.worker.BaseIteratorWorker;
import org.lareferencia.core.worker.IRunningContext;
import java.util.List;
import static org.junit.jupiter.api.Assertions.*;

class WorkerCancellationTest {
    @Test void iteratorStopsAfterTheCurrentItemAndDoesNotMarkPartialWorkComplete() {
        IteratorWorker worker = new IteratorWorker(false);
        worker.run();
        assertEquals(1, worker.processed);
        assertFalse(worker.completed);
        assertTrue(worker.cleaned);
    }
    @Test void cancellationDuringPreparationSkipsItemsAndUsesCancellationCleanup() {
        IteratorWorker worker = new IteratorWorker(true);
        worker.run();
        assertEquals(0, worker.processed);
        assertFalse(worker.completed);
        assertTrue(worker.cleaned);
    }
    private static class IteratorWorker extends BaseIteratorWorker<String, IRunningContext> {
        final boolean stopDuringPreparation;
        int processed;
        boolean completed;
        boolean cleaned;
        IteratorWorker(boolean stopDuringPreparation) { super(() -> "a"); this.stopDuringPreparation = stopDuringPreparation; }
        @Override protected void preRun() {
            setIterator(List.of("a", "b", "c").iterator(), 3);
            if (stopDuringPreparation) stop();
        }
        @Override public void prePage() { }
        @Override public void postPage() { }
        @Override public void processItem(String value) { processed++; stop(); }
        @Override protected void postRun() { completed = true; }
        @Override protected void onCancelled() { cleaned = true; }
    }
}
