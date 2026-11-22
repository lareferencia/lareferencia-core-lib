package org.lareferencia.core.worker.indexing;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.codec.digest.DigestUtils;

/** A read-only lookup for one network in a finalized metadata-tools vector package. */
final class SQLiteVectorReader implements AutoCloseable {
    private static final ObjectMapper JSON = new ObjectMapper();
    private final Connection connection;
    private final PreparedStatement lookup;
    final int dimensions;

    SQLiteVectorReader(Path directory, String filename, String network) throws IOException, SQLException {
        if (!(JSON.readTree(directory.resolve("manifest.json").toFile()) instanceof ObjectNode manifest)
                || !"embedding-vectors".equals(manifest.path("type").asText())
                || !"COMPLETE".equals(manifest.path("status").asText())
                || !"sqlite".equals(manifest.path("format").asText())
                || !"float32le".equals(manifest.path("encoding").asText())
                || manifest.path("formatVersion").asInt() != 1 || manifest.path("files").size() != 1
                || manifest.path("embeddings").size() != 1) {
            throw new IOException("Expected a complete embedding-vectors v1 SQLite manifest in " + directory);
        }
        JsonNode file = manifest.at("/files/0");
        dimensions = manifest.at("/embeddings/0/dimensions").asInt();
        if (dimensions <= 0 || !"float32".equals(manifest.at("/embeddings/0/outputDtype").asText())) {
            throw new IOException("Invalid embedding profile in " + directory);
        }
        String name = filename.isBlank() ? file.path("path").asText() : filename;
        Path path = directory.resolve(name);
        if (name.isBlank() || !Path.of(name).equals(path.getFileName()) || !Files.isRegularFile(path)) {
            throw new IOException("Expected a SQLite file inside " + directory + ": " + name);
        }
        connection = DriverManager.getConnection("jdbc:sqlite:" + path.toUri() + "?mode=ro&immutable=1");
        try (var input = Files.newInputStream(path); var statement = connection.createStatement()) {
            if (!("sha256:" + DigestUtils.sha256Hex(input)).equals(file.path("sha256").asText())) {
                throw new IOException("SQLite checksum differs from manifest: " + path);
            }
            // The internal manifest identifies the database pinned by this connection.
            try (var rows = statement.executeQuery("SELECT metadata FROM package WHERE singleton = 1")) {
                manifest.remove("files");
                if (!rows.next() || !manifest.equals(JSON.readTree(rows.getString(1)))) {
                    throw new IOException("SQLite package metadata differs from manifest: " + path);
                }
            }
            lookup = connection.prepareStatement("""
                    SELECT v.vector FROM items i JOIN vectors v ON v.item = i.id
                    WHERE i.network = ? AND i.catalog_id = ?
                    """);
            lookup.setString(1, network);
        } catch (IOException | SQLException | RuntimeException e) {
            connection.close();
            throw e;
        }
    }

    /** An empty list means the valid bibliographic record has no prepared vector. */
    List<Float> find(String catalogId) throws SQLException, IOException {
        lookup.setString(2, catalogId);
        try (var rows = lookup.executeQuery()) {
            if (!rows.next()) return List.of();
            byte[] bytes = rows.getBytes(1);
            if (bytes.length != (long) Float.BYTES * dimensions) {
                throw new IOException("Invalid vector length for catalog ID " + catalogId);
            }
            var buffer = ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN);
            var vector = new ArrayList<Float>(dimensions);
            while (buffer.hasRemaining()) vector.add(buffer.getFloat());
            return vector;
        }
    }

    @Override
    public void close() throws SQLException {
        connection.close();
    }
}
