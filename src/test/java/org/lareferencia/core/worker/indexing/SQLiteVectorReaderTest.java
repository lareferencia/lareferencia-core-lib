package org.lareferencia.core.worker.indexing;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.DriverManager;
import java.util.List;
import org.apache.commons.codec.digest.DigestUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.junit.jupiter.api.Assertions.*;

class SQLiteVectorReaderTest {
    @TempDir Path directory;
    private final ObjectMapper json = new ObjectMapper();
    private ObjectNode manifest;
    private Path database;

    @BeforeEach
    void publish() throws Exception {
        database = directory.resolve("custom.sqlite");
        manifest = json.createObjectNode().put("type", "embedding-vectors").put("status", "COMPLETE")
                .put("format", "sqlite").put("formatVersion", 1).put("encoding", "float32le");
        manifest.putArray("embeddings").addObject().put("dimensions", 2).put("outputDtype", "float32");
        try (var db = DriverManager.getConnection("jdbc:sqlite:" + database);
                var sql = db.createStatement()) {
            sql.execute("CREATE TABLE package(singleton INTEGER PRIMARY KEY, metadata TEXT)");
            try (var insert = db.prepareStatement("INSERT INTO package VALUES(1,?)")) {
                insert.setString(1, manifest.toString());
                insert.executeUpdate();
            }
            sql.execute("CREATE TABLE items(id INTEGER PRIMARY KEY, network TEXT, catalog_id TEXT)");
            sql.execute("CREATE TABLE vectors(item INTEGER PRIMARY KEY, vector BLOB)");
            sql.execute("INSERT INTO items VALUES(1,'AR','shared'),(2,'UY','shared')");
            sql.execute("INSERT INTO vectors VALUES(1,x'0000803f000000bf'),(2,x'0000003f000080bf')");
        }
        manifest.putArray("files").addObject().put("path", "custom.sqlite")
                .put("sha256", "sha256:" + DigestUtils.sha256Hex(Files.readAllBytes(database)));
        json.writeValue(directory.resolve("manifest.json").toFile(), manifest);
    }

    @Test void readsLittleEndianFloatsByNetworkAndStoredCatalogId() throws Exception {
        try (var reader = new SQLiteVectorReader(directory, "", "AR")) {
            assertEquals(List.of(1f, -0.5f), reader.find("shared"));
            assertEquals(List.of(), reader.find("missing"));
        }
        Files.move(database, directory.resolve("renamed.sqlite"));
        try (var reader = new SQLiteVectorReader(directory, "renamed.sqlite", "UY")) {
            assertEquals(List.of(0.5f, -1f), reader.find("shared"));
        }
    }

    @Test void rejectsAnUnrelatedDatabaseOrInconsistentProfile() throws Exception {
        ((ObjectNode) manifest.at("/embeddings/0")).put("dimensions", 3);
        json.writeValue(directory.resolve("manifest.json").toFile(), manifest);
        assertThrows(java.io.IOException.class, () -> new SQLiteVectorReader(directory, "", "AR"));
        ((ObjectNode) manifest.at("/files/0")).put("sha256", "wrong");
        json.writeValue(directory.resolve("manifest.json").toFile(), manifest);
        assertTrue(assertThrows(java.io.IOException.class,
                () -> new SQLiteVectorReader(directory, "", "AR")).getMessage().contains("checksum"));
    }
}
