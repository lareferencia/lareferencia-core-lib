package org.lareferencia.core.export;

import com.fasterxml.jackson.databind.node.ObjectNode;
import java.nio.file.Path;
import java.sql.*;

/** Standalone mapping index, including filtered records and per-record failures. */
final class ExportRecordIndex implements AutoCloseable {
    private final Connection connection;
    private final PreparedStatement insert;
    private int pending;
    ExportRecordIndex(Path file) throws Exception {
        connection = DriverManager.getConnection("jdbc:sqlite:" + file.toAbsolutePath());
        try {
            try (Statement statement = connection.createStatement()) {
                statement.execute("PRAGMA journal_mode=DELETE");
                statement.execute("CREATE TABLE records (record_key TEXT PRIMARY KEY, export_id TEXT NOT NULL, network TEXT NOT NULL, catalog_id TEXT NOT NULL, identifier TEXT NOT NULL, snapshot_ref TEXT NOT NULL, original_metadata_hash TEXT, variant TEXT NOT NULL, metadata_hash TEXT, datestamp TEXT, is_valid INTEGER, is_transformed INTEGER, status TEXT NOT NULL, file TEXT, position INTEGER, error TEXT)");
                statement.execute("CREATE INDEX record_identity ON records(network,catalog_id)");
            }
            connection.setAutoCommit(false);
            insert = connection.prepareStatement("INSERT INTO records VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)");
        } catch (Exception e) { connection.close(); throw e; }
    }
    void add(ObjectNode envelope, ExportCatalogReader.Row row, String status, ExportBatchWriter.Location location, String error) throws Exception {
        var source = envelope.path("source"); var input = envelope.path("input");
        Object[] values = {envelope.path("recordKey").asText(), envelope.path("exportId").asText(), source.path("network").asText(),
                row.id(), row.identifier(), source.path("snapshotRef").asText(), row.originalHash(), input.path("variant").asText(),
                input.path("metadataHash").isNull() ? null : input.path("metadataHash").asText(null), row.datestamp(),
                row.valid(), row.transformed(), status, location == null ? null : location.file(), location == null ? null : location.position(), error};
        for (int i = 0; i < values.length; i++) insert.setObject(i + 1, values[i]);
        insert.executeUpdate();
        if (++pending == 1000) { connection.commit(); pending = 0; }
    }
    @Override public void close() throws SQLException {
        try { connection.commit(); }
        finally { try { insert.close(); } finally { connection.close(); } }
    }
}
