package org.lareferencia.core.worker.harvesting;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.lareferencia.core.domain.Network;
import org.lareferencia.core.metadata.SnapshotMetadata;
import org.lareferencia.core.service.validation.ValidatorFingerprintService;
import org.lareferencia.core.util.PathUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

/** Completed harvest provenance; older snapshots require a full UTC baseline. */
@Service
public class HarvestingConfigurationStore {
    @Value("${store.basepath:/tmp/data/}") private String basePath;
    @Autowired private ValidatorFingerprintService fingerprints;
    private final ObjectMapper mapper = new ObjectMapper();

    private String configuration(Network network) throws IOException {
        var value = mapper.createObjectNode();
        value.put("version", 1);
        value.put("clock", "UTC");
        value.put("origin", network.getOriginURL());
        value.put("prefix", network.getMetadataPrefix());
        value.put("schema", network.getMetadataStoreSchema());
        value.set("sets", mapper.valueToTree(network.getSets()));
        value.put("prevalidator", fingerprints.fingerprint(network.getPrevalidator()).getHash());
        return mapper.writeValueAsString(value);
    }

    private Path path(SnapshotMetadata metadata) {
        return Path.of(PathUtils.getSnapshotPath(basePath, metadata), "harvesting-configuration.json");
    }

    public boolean compatible(SnapshotMetadata parent, Network network) {
        try { return Files.readString(path(parent)).equals(configuration(network)); }
        catch (IOException e) { return false; }
    }

    public void complete(SnapshotMetadata metadata, Network network) throws IOException {
        Path target = path(metadata);
        Files.createDirectories(target.getParent());
        Files.writeString(target, configuration(network));
    }
}
