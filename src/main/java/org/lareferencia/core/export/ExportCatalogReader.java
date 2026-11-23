package org.lareferencia.core.export;

import java.nio.file.*;
import java.sql.*;
import java.util.*;

/** Dedicated read-only SQLite connection; never invokes catalog/validation migrations. */
final class ExportCatalogReader implements AutoCloseable {
    record Row(String id, String identifier, String datestamp, String originalHash,
            Boolean valid, Boolean transformed, String publishedHash, String validationIdentifier, Boolean validationDeleted) {}
    private final Connection connection;
    private Statement statement;
    private ResultSet rows;
    ExportCatalogReader(ExportSourceResolver.Source source, boolean needsValidation) throws Exception {
        Path catalog = source.directory().resolve("catalog/catalog.db");
        if (!Files.isRegularFile(catalog)) throw new java.io.IOException("Catalog missing: " + catalog);
        requireFinalized(catalog);
        // Snapshots are immutable once published. SQLite's plain read-only mode
        // may still create WAL/SHM sidecars; immutable mode avoids touching the
        // source snapshot while reading it.
        connection = DriverManager.getConnection("jdbc:sqlite:" + catalog.toUri() + "?mode=ro&immutable=1");
        try {
            Path validation = source.directory().resolve("validation/validation.db");
            boolean available = Files.isRegularFile(validation);
            if (needsValidation && !available) throw new java.io.IOException("Validation database missing: " + validation);
            if (available) {
                requireFinalized(validation);
                try (PreparedStatement attach = connection.prepareStatement("ATTACH DATABASE ? AS validation")) {
                    attach.setString(1, validation.toUri() + "?mode=ro&immutable=1"); attach.execute();
                }
            }
            try (Statement config = connection.createStatement()) { config.execute("PRAGMA query_only=ON"); }
            connection.setAutoCommit(false);
            String columns = available ? "v.is_valid, v.is_transformed, v.published_metadata_hash, v.identifier AS validation_identifier, "
                    + (hasColumn("validation", "record_validation", "deleted") ? "v.deleted" : "0") + " AS validation_deleted"
                    : "NULL AS is_valid, NULL AS is_transformed, NULL AS published_metadata_hash, NULL AS validation_identifier, NULL AS validation_deleted";
            statement = connection.createStatement();
            rows = statement.executeQuery("SELECT c.id, c.identifier, c.datestamp, c.original_metadata_hash, " + columns
                    + " FROM oai_record c " + (available ? "LEFT JOIN validation.record_validation v ON v.identifier_hash=c.id " : "")
                    + "WHERE c.deleted=0 ORDER BY c.id");
        } catch (Exception e) { close(); throw e; }
    }
    private boolean hasColumn(String schema, String table, String column) throws SQLException {
        try (Statement stmt = connection.createStatement(); ResultSet rs = stmt.executeQuery("PRAGMA " + schema + ".table_info(" + table + ")")) {
            while (rs.next()) if (column.equals(rs.getString("name"))) return true;
            return false;
        }
    }
    /** Immutable mode ignores WAL content; a non-empty WAL means the snapshot was not cleanly finalized and would read stale. */
    private static void requireFinalized(Path database) throws java.io.IOException {
        Path wal = database.resolveSibling(database.getFileName().toString() + "-wal");
        try {
            var attrs = Files.readAttributes(wal, java.nio.file.attribute.BasicFileAttributes.class);
            if (attrs.isRegularFile() && attrs.size() > 0)
                throw new java.io.IOException("Snapshot database has a non-empty WAL; finalize the snapshot before export: " + wal);
        } catch (NoSuchFileException absent) {
            // SQLite may remove an empty sidecar before we can inspect it.
        }
    }
    Row next() throws SQLException {
        if (!rows.next()) return null;
        return new Row(rows.getString("id"), rows.getString("identifier"), rows.getString("datestamp"), rows.getString("original_metadata_hash"),
                bool("is_valid"), bool("is_transformed"), rows.getString("published_metadata_hash"), rows.getString("validation_identifier"), bool("validation_deleted"));
    }
    private Boolean bool(String name) throws SQLException { int value = rows.getInt(name); return rows.wasNull() ? null : value != 0; }
    @Override public void close() throws SQLException {
        try { if (rows != null) rows.close(); }
        finally { try { if (statement != null) statement.close(); } finally { connection.close(); } }
    }
}
