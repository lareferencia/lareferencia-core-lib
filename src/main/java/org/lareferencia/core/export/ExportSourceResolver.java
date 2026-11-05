package org.lareferencia.core.export;

import com.fasterxml.jackson.databind.node.*;
import java.nio.file.*;
import java.util.*;
import org.lareferencia.core.domain.*;
import org.lareferencia.core.metadata.*;
import org.lareferencia.core.repository.jpa.*;
import org.lareferencia.core.util.PathUtils;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

/** Pins LGK selection and provenance before opening any record streams. */
@Service
public class ExportSourceResolver {
    @FunctionalInterface public interface Verification { void verify() throws Exception; }
    public record Source(String reference, SnapshotMetadata metadata, Path directory,
            ObjectNode provenance, Verification verification) {
        public String acronym() { return metadata.getNetwork().getAcronym(); }
    }
    public record Selection(List<Source> sources, ArrayNode skipped) {}
    private final NetworkRepository networks;
    private final NetworkSnapshotRepository snapshots;
    private final ISnapshotStore store;
    private final String basePath;
    public ExportSourceResolver(NetworkRepository networks, NetworkSnapshotRepository snapshots,
            ISnapshotStore store, @Value("${store.basepath:/tmp/data/}") String basePath) {
        this.networks = networks; this.snapshots = snapshots; this.store = store; this.basePath = basePath;
    }
    public Selection resolve(ExportRequest request) throws Exception {
        List<Network> selected = new ArrayList<>();
        if (request.allNetworks()) selected.addAll(networks.findAll());
        else for (String acronym : request.networks()) {
            Network network = networks.findByAcronym(acronym);
            if (network == null || !acronym.equals(network.getAcronym())) throw new IllegalArgumentException("Unknown acronym: " + acronym);
            selected.add(network);
        }
        selected.sort(Comparator.comparing(Network::getAcronym));
        List<Source> result = new ArrayList<>(); ArrayNode skipped = ExportJson.JSON.createArrayNode();
        Set<String> directories = new HashSet<>();
        for (Network network : selected) {
            if (!directories.add(PathUtils.sanitizeNetworkAcronym(network.getAcronym())))
                throw new IllegalArgumentException("Directory collision for " + network.getAcronym());
            Long id = store.findLastGoodKnownSnapshot(network.getId());
            if (id == null) { skipped.addObject().put("network", network.getAcronym()).put("reason", "NO_LGK"); continue; }
            NetworkSnapshot snapshot = snapshots.findById(id).orElseThrow();
            requireReady(snapshot);
            SnapshotMetadata metadata = store.getSnapshotMetadata(id);
            if (metadata == null) throw new IllegalStateException("Snapshot metadata unavailable: " + id);
            Path directory = Path.of(PathUtils.getSnapshotPath(basePath, metadata));
            ObjectNode baseline = capture(snapshot, directory);
            ObjectNode provenance = baseline.deepCopy();
            provenance.put("network", network.getAcronym());
            provenance.put("originURL", network.getOriginURL());
            provenance.put("metadataPrefix", network.getMetadataPrefix());
            provenance.put("metadataStoreSchema", network.getMetadataStoreSchema());
            String reference = String.format(Locale.ROOT, "source-%06d", result.size() + 1);
            provenance.put("snapshotRef", reference);
            result.add(new Source(reference, metadata, directory, provenance, () -> {
                NetworkSnapshot fresh = snapshots.findById(id).orElseThrow();
                requireReady(fresh);
                if (!baseline.equals(capture(fresh, directory))) throw new IllegalStateException("Snapshot changed during export: " + network.getAcronym());
            }));
        }
        if (!request.allNetworks() && result.isEmpty()) throw new IllegalArgumentException("Selected sources have no LGK");
        return new Selection(List.copyOf(result), skipped);
    }
    private static void requireReady(NetworkSnapshot snapshot) {
        if (snapshot.isDeleted() || snapshot.getStatus() != SnapshotStatus.VALID)
            throw new IllegalStateException("Snapshot is not a completed LGK: " + snapshot.getId());
    }
    private static ObjectNode capture(NetworkSnapshot snapshot, Path directory) throws Exception {
        ObjectNode node = ExportJson.object();
        node.put("internalSnapshotId", snapshot.getId()); node.put("status", snapshot.getStatus().name());
        node.put("startTime", Objects.toString(snapshot.getStartTime(), null));
        node.put("endTime", Objects.toString(snapshot.getEndTime(), null));
        node.put("previousSnapshotId", snapshot.getPreviousSnapshotId());
        ObjectNode evidence = node.putObject("evidence");
        for (String file : List.of("metadata.json", "harvesting-configuration.json", "validation/validation-manifest.json")) {
            Path path = directory.resolve(file);
            if (Files.isRegularFile(path)) {
                ObjectNode item = evidence.putObject(file); item.put("sha256", ExportJson.hash(path));
                item.set("content", ExportJson.JSON.readTree(path.toFile()));
            }
        }
        ObjectNode databases = node.putObject("databaseVersions");
        for (String file : List.of("catalog/catalog.db", "catalog/catalog.db-wal", "validation/validation.db", "validation/validation.db-wal")) {
            Path path = directory.resolve(file);
            if (Files.exists(path)) {
                var attrs = Files.readAttributes(path, java.nio.file.attribute.BasicFileAttributes.class);
                databases.putObject(file).put("size", attrs.size()).put("modified", attrs.lastModifiedTime().toString())
                    .put("fileKey", Objects.toString(attrs.fileKey(), null));
            }
        }
        return node;
    }
}
