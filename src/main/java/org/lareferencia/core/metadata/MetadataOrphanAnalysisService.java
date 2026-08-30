package org.lareferencia.core.metadata;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.ArrayList;
import java.util.function.Consumer;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;

import org.lareferencia.core.repository.catalog.OAIRecord;
import org.lareferencia.core.repository.catalog.OAIRecordCatalogRepository;
import org.lareferencia.core.repository.validation.RecordValidationRepository;
import org.lareferencia.core.repository.validation.ValidationRecord;
import org.springframework.stereotype.Service;

import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnels;

/** Read-only reachability analysis for one metadata-store namespace (one network). */
@Service
public class MetadataOrphanAnalysisService {
    private static final double FALSE_POSITIVE_PROBABILITY = 0.01d;
    private final IMetadataStore metadataStore;
    private final OAIRecordCatalogRepository oaiRecords;
    private final RecordValidationRepository validationRecords;

    public MetadataOrphanAnalysisService(IMetadataStore metadataStore, OAIRecordCatalogRepository oaiRecords,
            RecordValidationRepository validationRecords) {
        this.metadataStore = metadataStore;
        this.oaiRecords = oaiRecords;
        this.validationRecords = validationRecords;
    }

    public MetadataOrphanAnalysis analyze(List<SnapshotMetadata> protectedSnapshots)
            throws IOException, MetadataRecordStoreException {
        BloomFilter<CharSequence> retained = retainedHashes(protectedSnapshots);
        AtomicLong oaiCount = new AtomicLong();
        AtomicLong validationCount = new AtomicLong();
        for (SnapshotMetadata snapshot : protectedSnapshots) collect(snapshot, retained, oaiCount, validationCount);
        AtomicLong scanned = new AtomicLong();
        AtomicLong candidates = new AtomicLong();
        metadataStore.forEachHash(protectedSnapshots.get(0), hash -> {
            scanned.incrementAndGet();
            if (!retained.mightContain(hash)) candidates.incrementAndGet();
        });
        return new MetadataOrphanAnalysis(protectedSnapshots.stream().map(SnapshotMetadata::getSnapshotId).toList(),
                oaiCount.get(), validationCount.get(), scanned.get(), candidates.get(), FALSE_POSITIVE_PROBABILITY);
    }

    /**
     * Removes only metadata hashes that are not referenced by the protected snapshots.
     * Candidates are collected before deletion so SQL-backed stores never mutate a table
     * while its hash cursor is open. Both limits are mandatory safeguards and are
     * intentionally controlled by the installation-wide worker configuration.
     */
    public MetadataOrphanCleanup cleanup(List<SnapshotMetadata> protectedSnapshots, long maxMetadataEntries,
            long maxDeletes) throws IOException, MetadataRecordStoreException {
        return cleanup(protectedSnapshots, maxMetadataEntries, maxDeletes, null);
    }

    /** Same cleanup operation with optional progress notifications for a worker UI. */
    public MetadataOrphanCleanup cleanup(List<SnapshotMetadata> protectedSnapshots, long maxMetadataEntries,
            long maxDeletes, Consumer<MetadataOrphanCleanupProgress> progress) throws IOException, MetadataRecordStoreException {
        if (maxMetadataEntries < 1 || maxDeletes < 1)
            throw new IllegalArgumentException("Metadata cleanup limits must be greater than zero");
        BloomFilter<CharSequence> retained = retainedHashes(protectedSnapshots);
        AtomicLong oaiCount = new AtomicLong();
        AtomicLong validationCount = new AtomicLong();
        for (SnapshotMetadata snapshot : protectedSnapshots) collect(snapshot, retained, oaiCount, validationCount);

        AtomicLong scanned = new AtomicLong();
        AtomicLong candidates = new AtomicLong();
        AtomicBoolean scanLimitReached = new AtomicBoolean(false);
        List<String> deletions = new ArrayList<>();
        notify(progress, "SCANNING", 0, 0, 0, maxMetadataEntries, maxDeletes);
        try {
            metadataStore.forEachHash(protectedSnapshots.get(0), hash -> {
                if (scanned.incrementAndGet() > maxMetadataEntries) throw new ScanLimitReachedException();
                if (!retained.mightContain(hash)) {
                    long candidate = candidates.incrementAndGet();
                    if (candidate <= maxDeletes) deletions.add(hash);
                }
                notify(progress, "SCANNING", scanned.get(), candidates.get(), 0, maxMetadataEntries, maxDeletes);
            });
        } catch (ScanLimitReachedException ignored) {
            scanLimitReached.set(true);
        }

        AtomicLong deleted = new AtomicLong();
        SnapshotMetadata namespace = protectedSnapshots.get(0);
        notify(progress, "DELETING", scanned.get(), candidates.get(), 0, maxMetadataEntries, maxDeletes);
        for (String hash : deletions) {
            if (metadataStore.deleteMetadata(namespace, hash)) deleted.incrementAndGet();
            notify(progress, "DELETING", scanned.get(), candidates.get(), deleted.get(), maxMetadataEntries, maxDeletes);
        }
        MetadataOrphanCleanup result = new MetadataOrphanCleanup(protectedSnapshots.stream().map(SnapshotMetadata::getSnapshotId).toList(),
                oaiCount.get(), validationCount.get(), scanned.get(), candidates.get(), deleted.get(),
                scanLimitReached.get(), candidates.get() > maxDeletes, maxMetadataEntries, maxDeletes,
                FALSE_POSITIVE_PROBABILITY);
        notify(progress, "COMPLETED", scanned.get(), candidates.get(), deleted.get(), maxMetadataEntries, maxDeletes);
        return result;
    }

    private void notify(Consumer<MetadataOrphanCleanupProgress> progress, String stage, long scanned, long candidates,
            long deleted, long maxMetadataEntries, long maxDeletes) {
        if (progress != null) progress.accept(new MetadataOrphanCleanupProgress(stage, scanned, candidates, deleted,
                maxMetadataEntries, maxDeletes));
    }

    private BloomFilter<CharSequence> retainedHashes(List<SnapshotMetadata> protectedSnapshots) {
        if (protectedSnapshots == null || protectedSnapshots.isEmpty())
            throw new IllegalArgumentException("At least one protected snapshot is required");
        long expected = Math.max(1L, protectedSnapshots.stream().mapToLong(this::countReferences).sum());
        return BloomFilter.create(Funnels.stringFunnel(StandardCharsets.UTF_8), expected, FALSE_POSITIVE_PROBABILITY);
    }

    private long countReferences(SnapshotMetadata snapshot) {
        try (Stream<OAIRecord> records = oaiRecords.streamAll(snapshot)) {
            return records.count() + validationRecords.count(snapshot.getSnapshotId());
        }
    }

    private void collect(SnapshotMetadata snapshot, BloomFilter<CharSequence> retained, AtomicLong oaiCount,
            AtomicLong validationCount) throws IOException {
        try (Stream<OAIRecord> records = oaiRecords.streamAll(snapshot)) {
            records.map(OAIRecord::getOriginalMetadataHash).filter(hash -> hash != null && !hash.isBlank())
                    .forEach(hash -> { retained.put(hash); oaiCount.incrementAndGet(); });
        }
        try (Stream<ValidationRecord> records = validationRecords.streamAll(snapshot.getSnapshotId())) {
            records.map(ValidationRecord::getPublishedMetadataHash).filter(hash -> hash != null && !hash.isBlank())
                    .forEach(hash -> { retained.put(hash); validationCount.incrementAndGet(); });
        }
    }

    public record MetadataOrphanAnalysis(List<Long> protectedSnapshotIds, long oaiReferences,
            long validationReferences, long metadataEntriesScanned, long orphanCandidates,
            double falsePositiveProbability) { }

    public record MetadataOrphanCleanup(List<Long> protectedSnapshotIds, long oaiReferences,
            long validationReferences, long metadataEntriesScanned, long orphanCandidates, long deleted,
            boolean scanLimitReached, boolean deleteLimitReached, long maxMetadataEntries, long maxDeletes,
            double falsePositiveProbability) { }

    public record MetadataOrphanCleanupProgress(String stage, long metadataEntriesScanned, long orphanCandidates,
            long deleted, long maxMetadataEntries, long maxDeletes) { }

    private static final class ScanLimitReachedException extends RuntimeException {
        private static final long serialVersionUID = 1L;
    }
}
