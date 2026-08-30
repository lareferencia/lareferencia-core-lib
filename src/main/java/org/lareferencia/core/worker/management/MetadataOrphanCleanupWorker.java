/*
 * Copyright (c) 2013-2026. LA Referencia / Red CLARA and others
 */
package org.lareferencia.core.worker.management;

import java.util.ArrayList;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.lareferencia.core.domain.Network;
import org.lareferencia.core.metadata.ISnapshotStore;
import org.lareferencia.core.metadata.MetadataOrphanAnalysisService;
import org.lareferencia.core.metadata.SnapshotMetadata;
import org.lareferencia.core.worker.BaseWorker;
import org.lareferencia.core.worker.NetworkRunningContext;
import org.springframework.beans.factory.annotation.Autowired;

import lombok.Getter;
import lombok.Setter;

/**
 * Deletes unreferenced entries from a network metadata store.
 * The last good-known and last harvested snapshots are always retained.
 */
public class MetadataOrphanCleanupWorker extends BaseWorker<NetworkRunningContext> {
    private static final Logger logger = LogManager.getLogger(MetadataOrphanCleanupWorker.class);

    @Autowired private ISnapshotStore snapshotStore;
    @Autowired private MetadataOrphanAnalysisService metadataOrphans;

    /** Maximum metadata hashes inspected in one run. */
    @Getter @Setter private long maxMetadataEntries = 5_000_000L;
    /** Maximum unreferenced hashes deleted in one run. */
    @Getter @Setter private long maxDeletes = 1_000_000L;
    /** Number of scanned or deleted hashes between progress messages. */
    @Getter @Setter private long progressEvery = 10_000L;
    private volatile String status = "Waiting";

    @Override
    public String toString() { return "Metadata cleanup"; }

    @Override
    public String getStatus() { return status; }

    @Override
    public void run() {
        Network network = runningContext.getNetwork();
        Long lastGoodKnown = snapshotStore.findLastGoodKnownSnapshot(network);
        if (lastGoodKnown == null) {
            status = "Skipped: no valid snapshot";
            logger.warn("Metadata cleanup skipped for {}: no last good-known snapshot", network.getAcronym());
            return;
        }

        List<SnapshotMetadata> protectedSnapshots = new ArrayList<>();
        protectedSnapshots.add(snapshotStore.getSnapshotMetadata(lastGoodKnown));
        Long lastHarvested = snapshotStore.findLastHarvestingSnapshot(network);
        if (lastHarvested != null && !lastHarvested.equals(lastGoodKnown))
            protectedSnapshots.add(snapshotStore.getSnapshotMetadata(lastHarvested));

        try {
            validateProgressInterval();
            status = "Preparing metadata cleanup";
            logger.info("Starting metadata cleanup for {}: protectedSnapshots={}, maxMetadataEntries={}, maxDeletes={}, progressEvery={}",
                    network.getAcronym(), protectedSnapshots.stream().map(SnapshotMetadata::getSnapshotId).toList(),
                    maxMetadataEntries, maxDeletes, progressEvery);
            long[] lastReported = { -1L };
            var result = metadataOrphans.cleanup(protectedSnapshots, maxMetadataEntries, maxDeletes, progress -> {
                status = status(progress);
                long completed = "DELETING".equals(progress.stage()) ? progress.deleted() : progress.metadataEntriesScanned();
                if ("COMPLETED".equals(progress.stage()) || completed == 0 || completed - lastReported[0] >= progressEvery) {
                    logger.info("Metadata cleanup progress for {}: {}", network.getAcronym(), status);
                    lastReported[0] = completed;
                }
            });
            status = "Completed: " + result.deleted() + " metadata entries deleted";
            logger.info("Metadata cleanup completed for {}: scanned={}, candidates={}, deleted={}, scanLimitReached={}, deleteLimitReached={}",
                    network.getAcronym(), result.metadataEntriesScanned(), result.orphanCandidates(), result.deleted(),
                    result.scanLimitReached(), result.deleteLimitReached());
        } catch (Exception exception) {
            status = "Failed: " + exception.getMessage();
            logger.error("Metadata cleanup failed for {}", network.getAcronym(), exception);
            throw new IllegalStateException("Metadata cleanup failed for " + network.getAcronym(), exception);
        }
    }

    private void validateProgressInterval() {
        if (progressEvery < 1) throw new IllegalArgumentException("progressEvery must be greater than zero");
    }

    private String status(MetadataOrphanAnalysisService.MetadataOrphanCleanupProgress progress) {
        return switch (progress.stage()) {
            case "SCANNING" -> "Scanning " + progress.metadataEntriesScanned() + "/" + progress.maxMetadataEntries()
                    + " hashes; " + progress.orphanCandidates() + " candidates";
            case "DELETING" -> "Deleting " + progress.deleted() + "/" + Math.min(progress.orphanCandidates(), progress.maxDeletes())
                    + " orphan metadata entries";
            default -> "Completed: " + progress.deleted() + " metadata entries deleted";
        };
    }
}
