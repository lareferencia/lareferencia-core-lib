package org.lareferencia.core.worker.harvesting;

import static org.junit.jupiter.api.Assertions.*;
import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.lareferencia.core.domain.Network;
import org.lareferencia.core.domain.Validator;
import org.lareferencia.core.metadata.SnapshotMetadata;
import org.lareferencia.core.service.validation.ValidatorFingerprintService;
import org.springframework.test.util.ReflectionTestUtils;

class HarvestingConfigurationStoreTest {
    @TempDir Path directory;
    @Test void legacyAndChangedSourceOrPrevalidatorRequireFullBaseline() throws Exception {
        var store = new HarvestingConfigurationStore();
        ReflectionTestUtils.setField(store,"basePath",directory.toString());
        ReflectionTestUtils.setField(store,"fingerprints",new ValidatorFingerprintService());
        var network = new Network(); network.setAcronym("FIX"); network.setOriginURL("https://fixture.invalid/oai");
        network.setMetadataPrefix("oai_dc"); network.setMetadataStoreSchema("oai_dc");
        var snapshot = new SnapshotMetadata(1L); snapshot.setNetwork(network);
        assertFalse(store.compatible(snapshot,network));
        store.complete(snapshot,network); assertTrue(store.compatible(snapshot,network));
        network.setSets(List.of("restricted")); assertFalse(store.compatible(snapshot,network));
        network.setSets(List.of()); assertTrue(store.compatible(snapshot,network));
        network.setPrevalidator(new Validator()); assertFalse(store.compatible(snapshot,network));
        network.setPrevalidator(null); network.setOriginURL("https://different.invalid/oai"); assertFalse(store.compatible(snapshot,network));
    }
}
