package org.lareferencia.core.task;

import org.junit.jupiter.api.Test;
import org.springframework.mock.env.MockEnvironment;
import static org.junit.jupiter.api.Assertions.*;

class TaskManagerConfigTest {
    @Test void defaultsAndLegacyAliasRemainCompatible() {
        assertEquals(32, TaskManagerConfig.queuedLimit(new MockEnvironment()));
        assertEquals(12, TaskManagerConfig.queuedLimit(new MockEnvironment().withProperty("taskmanager.max_queuded.tasks", "12")));
        assertEquals(0, TaskManagerConfig.queuedLimit(new MockEnvironment().withProperty("taskmanager.max-queued-tasks", "0")));
    }
    @Test void conflictingAliasesFailInsteadOfSilentlyChangingTheLimit() {
        MockEnvironment environment = new MockEnvironment().withProperty("taskmanager.max_queuded.tasks", "32")
                .withProperty("taskmanager.max-queued-tasks", "64");
        assertThrows(IllegalArgumentException.class, () -> TaskManagerConfig.queuedLimit(environment));
    }
    @Test void invalidConcurrencyFailsAtStartup() {
        assertThrows(IllegalArgumentException.class, () -> new TaskManagerConfig().taskManager(null,
                new MockEnvironment().withProperty("taskmanager.concurrent.tasks", "0")));
    }
}
