package org.lareferencia.core.service.validation;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

import java.nio.file.Path;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.lareferencia.core.domain.Network;
import org.lareferencia.core.metadata.SnapshotMetadata;
import org.springframework.test.util.ReflectionTestUtils;

class ValidationManifestStoreTest {

    @TempDir
    Path tempDir;

    @Test
    void writesAndReadsManifestAndAcceptsHistoricalSnapshotsWithoutOne() throws Exception {
        ValidationManifestStore store = new ValidationManifestStore();
        ReflectionTestUtils.setField(store, "basePath", tempDir.toString());
        SnapshotMetadata historical = snapshot(1L);
        SnapshotMetadata current = snapshot(2L);

        assertFalse(store.read(historical).isPresent());

        ValidatorFingerprint expected = new ValidatorFingerprint(1, "SHA-256", "validator-v1", "abc123");
        store.write(current, expected);

        ValidatorFingerprint recovered = store.read(current).orElseThrow();
        assertEquals(expected.getHash(), recovered.getHash());
        assertEquals(expected.getAlgorithm(), recovered.getAlgorithm());
        assertEquals(expected.getCanonicalizer(), recovered.getCanonicalizer());
        assertEquals(expected.getFormatVersion(), recovered.getFormatVersion());
    }

    private SnapshotMetadata snapshot(long id) {
        SnapshotMetadata metadata = new SnapshotMetadata(id);
        Network network = new Network();
        network.setAcronym("TEST");
        metadata.setNetwork(network);
        return metadata;
    }
}
