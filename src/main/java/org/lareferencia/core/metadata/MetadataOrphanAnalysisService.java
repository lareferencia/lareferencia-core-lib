package org.lareferencia.core.metadata;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
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
        if (protectedSnapshots == null || protectedSnapshots.isEmpty())
            throw new IllegalArgumentException("At least one protected snapshot is required");
        long expected = Math.max(1L, protectedSnapshots.stream().mapToLong(this::countReferences).sum());
        BloomFilter<CharSequence> retained = BloomFilter.create(Funnels.stringFunnel(StandardCharsets.UTF_8), expected,
                FALSE_POSITIVE_PROBABILITY);
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
}
