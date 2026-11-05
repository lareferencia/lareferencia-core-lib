package org.lareferencia.core.export;

import java.nio.file.*;
import java.util.*;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;
import org.lareferencia.core.domain.*;
import org.lareferencia.core.metadata.*;
import org.lareferencia.core.repository.jpa.*;
import org.lareferencia.core.util.PathUtils;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class ExportSourceResolverTest {
    @TempDir Path temp;
    NetworkRepository networks;
    NetworkSnapshotRepository snapshots;
    ISnapshotStore store;
    ExportSourceResolver resolver;
    @BeforeEach void setup() {
        networks = mock(NetworkRepository.class); snapshots = mock(NetworkSnapshotRepository.class); store = mock(ISnapshotStore.class);
        when(store.findLastGoodKnownSnapshot(anyLong())).thenReturn(null);
        resolver = new ExportSourceResolver(networks, snapshots, store, temp.toString());
    }
    private ExportRequest request(List<String> acronyms, boolean all) {
        return new ExportRequest(acronyms, all, "original", "xml", null, "collection", "network", null, null, "none", false, temp);
    }
    private Network network(String acronym, long id) {
        Network network = mock(Network.class);
        when(network.getAcronym()).thenReturn(acronym); when(network.getId()).thenReturn(id);
        when(networks.findByAcronym(acronym)).thenReturn(network);
        return network;
    }
    @Test void rejectsUnknownExactAcronymsAndNoLgkExplicitSelection() {
        assertThrows(IllegalArgumentException.class, () -> resolver.resolve(request(List.of("unknown"), false)));
        network("A", 1);
        assertThrows(IllegalArgumentException.class, () -> resolver.resolve(request(List.of("A"), false)));
    }
    @Test void emptyAllSelectionIsValidAndSanitizedCollisionsAreRejected() throws Exception {
        when(networks.findAll()).thenReturn(List.of());
        assertTrue(resolver.resolve(request(List.of(), true)).sources().isEmpty());
        var a = network("a-b", 1); var b = network("A-B", 2);
        when(networks.findAll()).thenReturn(List.of(a,b));
        assertThrows(IllegalArgumentException.class, () -> resolver.resolve(request(List.of(), true)));
    }
    @Test void pinsSnapshotAndDetectsProvenanceAndStatusChanges() throws Exception {
        Network network = network("RCAAP", 1);
        when(store.findLastGoodKnownSnapshot(1L)).thenReturn(123L);
        NetworkSnapshot snapshot = mock(NetworkSnapshot.class);
        when(snapshot.getId()).thenReturn(123L); when(snapshot.getStatus()).thenReturn(SnapshotStatus.VALID);
        when(snapshots.findById(123L)).thenReturn(Optional.of(snapshot));
        SnapshotMetadata metadata = new SnapshotMetadata(123L);
        Network actualNetwork = new Network(); actualNetwork.setAcronym("RCAAP"); metadata.setNetwork(actualNetwork);
        when(store.getSnapshotMetadata(123L)).thenReturn(metadata);
        Path directory = Path.of(PathUtils.getSnapshotPath(temp.toString(), metadata)); Files.createDirectories(directory);
        Files.writeString(directory.resolve("harvesting-configuration.json"), "{\"source\":\"original\"}");
        var selected = resolver.resolve(request(List.of("RCAAP"), false)).sources().get(0);
        selected.verification().verify();
        Files.writeString(directory.resolve("harvesting-configuration.json"), "{\"source\":\"changed\"}");
        assertThrows(IllegalStateException.class, () -> selected.verification().verify());
        when(snapshot.getStatus()).thenReturn(SnapshotStatus.VALIDATING);
        assertThrows(IllegalStateException.class, () -> resolver.resolve(request(List.of("RCAAP"), false)));
        verify(store, times(2)).findLastGoodKnownSnapshot(1L);
    }
}
