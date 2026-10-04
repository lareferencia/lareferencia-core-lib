package org.lareferencia.core.service.management;

import static org.junit.jupiter.api.Assertions.*;

import java.time.Instant;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import javax.sql.DataSource;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.lareferencia.core.domain.IndexingResult;
import org.lareferencia.core.domain.NetworkSnapshot;
import org.lareferencia.core.domain.SnapshotIndexingResults;
import org.lareferencia.core.domain.SnapshotIndexStatus;
import org.lareferencia.core.domain.Network;
import org.lareferencia.core.repository.validation.ValidationRecord;
import org.lareferencia.core.worker.NetworkRunningContext;
import org.lareferencia.core.worker.indexing.BaseIndexerWorker;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.orm.jpa.JpaTransactionManager;
import org.springframework.orm.jpa.LocalContainerEntityManagerFactoryBean;
import org.springframework.orm.jpa.vendor.HibernateJpaVendorAdapter;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.annotation.EnableTransactionManagement;
import org.springframework.transaction.support.TransactionTemplate;

import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.persistence.EntityManager;
import jakarta.persistence.EntityManagerFactory;
import jakarta.persistence.PersistenceContext;

/** Runs only against an explicitly supplied, disposable PostgreSQL database. */
@SpringJUnitConfig(SnapshotIndexingServiceIntegrationTest.Config.class)
@EnabledIfSystemProperty(named = "indexing.test.jdbc-url", matches = ".+")
class SnapshotIndexingServiceIntegrationTest {
    @Autowired private SnapshotIndexingService results;
    @Autowired private PlatformTransactionManager transactions;
    @Autowired private NamedIndexer namedIndexer;
    @PersistenceContext private EntityManager em;
    private Long snapshotId;

    @BeforeEach void snapshot() {
        snapshotId = new TransactionTemplate(transactions).execute(status -> {
            NetworkSnapshot snapshot = new NetworkSnapshot();
            em.persist(snapshot);
            em.flush();
            return snapshot.getId();
        });
    }

    @Test void storesIndependentResultsAndReplacesOnlyTheRetriedWorker() {
        long generation = results.generation(snapshotId);
        results.record(snapshotId, generation, "frontendIndexerWorker", success());
        results.record(snapshotId, generation, "xoaiIndexerWorker", failure());
        assertEquals(2, state().results().size());
        assertEquals(SnapshotIndexStatus.FAILED, legacy());
        results.record(snapshotId, generation, "xoaiIndexerWorker", success());
        assertEquals(2, state().results().size());
        assertEquals(SnapshotIndexStatus.INDEXED, legacy());
    }

    @Test void concurrentWorkersDoNotOverwriteOneAnother() throws Exception {
        long generation = results.generation(snapshotId);
        var executor = Executors.newFixedThreadPool(2);
        var ready = new CountDownLatch(2);
        var start = new CountDownLatch(1);
        try {
            var first = executor.submit(() -> { ready.countDown(); await(start); results.record(snapshotId, generation, "frontendIndexerWorker", success()); });
            var second = executor.submit(() -> { ready.countDown(); await(start); results.record(snapshotId, generation, "xoaiIndexerWorker", failure()); });
            assertTrue(ready.await(10, TimeUnit.SECONDS));
            start.countDown();
            first.get(10, TimeUnit.SECONDS);
            second.get(10, TimeUnit.SECONDS);
            assertEquals(2, state().results().size());
            assertEquals(SnapshotIndexStatus.FAILED, legacy());
        } finally { start.countDown(); executor.shutdownNow(); }
    }

    @Test void revalidationClearsResultsAndRejectsLateCompletion() {
        long generation = results.generation(snapshotId);
        results.record(snapshotId, generation, "frontendIndexerWorker", success());
        results.reset(snapshotId);
        assertTrue(state().results().isEmpty());
        assertEquals(SnapshotIndexStatus.UNKNOWN, legacy());
        assertFalse(results.record(snapshotId, generation, "xoaiIndexerWorker", failure()));
        assertTrue(state().results().isEmpty());
        assertTrue(results.record(snapshotId, results.generation(snapshotId), "frontendIndexerWorker", success()));
    }

    @Test void staleEntityCannotOverwriteNewResultsOrLegacySummary() {
        new TransactionTemplate(transactions).executeWithoutResult(status -> {
            NetworkSnapshot stale = em.find(NetworkSnapshot.class, snapshotId);
            results.record(snapshotId, results.generation(snapshotId), "xoaiIndexerWorker", failure());
            stale.setSize(100);
            stale.setIndexStatus(SnapshotIndexStatus.INDEXED);
            stale.setIndexingResults(SnapshotIndexingResults.empty());
            em.flush();
        });
        assertEquals(SnapshotIndexStatus.FAILED, legacy());
        assertTrue(state().results().containsKey("xoaiIndexerWorker"));
    }

    @Test void resettingInARolledBackValidationKeepsPreviousResults() {
        results.record(snapshotId, results.generation(snapshotId), "frontendIndexerWorker", success());
        new TransactionTemplate(transactions).executeWithoutResult(status -> { results.reset(snapshotId); status.setRollbackOnly(); });
        assertEquals(1, state().results().size());
        assertEquals(SnapshotIndexStatus.INDEXED, legacy());
    }

    @Test void legacySuccessCannotHideARecordedFailure() {
        results.record(snapshotId, results.generation(snapshotId), "xoaiIndexerWorker", failure());
        assertEquals(SnapshotIndexStatus.FAILED, results.markLegacyAsIndexed(snapshotId));
        assertEquals(SnapshotIndexStatus.FAILED, legacy());
    }

    @Test void springAssignsTheBeanIdentityEvenWhenRunIsTransactionProxied() {
        namedIndexer.setSnapshotId(snapshotId);
        namedIndexer.setRunningContext(new NetworkRunningContext(new Network(), "TEST_ACTION", null));
        assertThrows(IllegalStateException.class, namedIndexer::run);
        assertTrue(state().results().containsKey("namedIndexer"));
        assertEquals("TEST_ACTION", state().results().get("namedIndexer").actionName());
        assertEquals(SnapshotIndexStatus.FAILED, legacy());
    }

    private SnapshotIndexingResults state() {
        return new TransactionTemplate(transactions).execute(status -> em.find(NetworkSnapshot.class, snapshotId).getIndexingResults());
    }
    private SnapshotIndexStatus legacy() {
        return new TransactionTemplate(transactions).execute(status -> em.find(NetworkSnapshot.class, snapshotId).getIndexStatus());
    }
    private static IndexingResult success() { return new IndexingResult(SnapshotIndexStatus.INDEXED, "FRONTEND_INDEXING_ACTION", Instant.now(), null); }
    private static IndexingResult failure() { return new IndexingResult(SnapshotIndexStatus.FAILED, "XOAI_INDEXING_ACTION", Instant.now(), "Solr commit failed"); }
    private static void await(CountDownLatch latch) {
        try { if (!latch.await(10, TimeUnit.SECONDS)) throw new IllegalStateException("Concurrent start timed out"); }
        catch (InterruptedException error) { Thread.currentThread().interrupt(); throw new IllegalStateException(error); }
    }

    @Configuration @EnableTransactionManagement(proxyTargetClass = true)
    static class Config {
        @Bean DataSource dataSource(Environment environment) {
            return new DriverManagerDataSource(environment.getRequiredProperty("indexing.test.jdbc-url"), "postgres", "indexing-test");
        }
        @Bean LocalContainerEntityManagerFactoryBean entityManagerFactory(DataSource dataSource) {
            var factory = new LocalContainerEntityManagerFactoryBean();
            factory.setDataSource(dataSource);
            factory.setPackagesToScan("org.lareferencia.core.domain");
            factory.setJpaVendorAdapter(new HibernateJpaVendorAdapter());
            factory.setJpaPropertyMap(Map.of("hibernate.hbm2ddl.auto", "create-drop",
                    "hibernate.physical_naming_strategy", "org.hibernate.boot.model.naming.PhysicalNamingStrategyStandardImpl"));
            return factory;
        }
        @Bean PlatformTransactionManager transactionManager(EntityManagerFactory factory) { return new JpaTransactionManager(factory); }
        @Bean ObjectMapper objectMapper() { return new ObjectMapper().findAndRegisterModules(); }
        @Bean SnapshotIndexingService indexingService(ObjectMapper mapper) { return new SnapshotIndexingService(mapper); }
        @Bean NamedIndexer namedIndexer() { return new NamedIndexer(); }
    }

    public static class NamedIndexer extends BaseIndexerWorker {
        private Long snapshotId;
        public void setSnapshotId(Long snapshotId) { this.snapshotId = snapshotId; }
        @Override public void preRun() { beginIndexing(snapshotId); throw new IllegalStateException("Preparation failed"); }
        @Override public void prePage() { }
        @Override public void processItem(ValidationRecord item) { }
        @Override public void postPage() { }
        @Override public void postRun() { indexingCommitted(); }
    }
}
