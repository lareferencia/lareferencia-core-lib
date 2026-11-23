package org.lareferencia.core.worker.validation;

import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.security.*;
import java.sql.*;
import java.util.*;
import java.io.*;

public final class SQLiteEnrichmentEdition implements AutoCloseable {
    public record Row(String producer, String target, String content) {}
    private Connection connection;
    private PreparedStatement lookup;
    private String digest = "absent";
    public String digest() { return digest; }

    public SQLiteEnrichmentEdition(Path file, String network) throws Exception {
        if (Files.notExists(file)) return;
        var before = Files.readAttributes(file, BasicFileAttributes.class);
        try {
            connection = DriverManager.getConnection("jdbc:sqlite:" + file.toUri() + "?mode=ro&immutable=1");
            try (var statement = connection.createStatement()) {
                try (var r = statement.executeQuery("PRAGMA user_version")) {
                    if (!r.next() || r.getInt(1) != 1) throw new IOException("Unsealed edition");
                }
                try (var r = statement.executeQuery("SELECT version,network,variant FROM package")) {
                    if (!r.next() || r.getInt(1)!=1 || !network.equals(r.getString(2)) || !"original".equals(r.getString(3)))
                        throw new IOException("Incompatible enrichment package");
                }
                try (var r = statement.executeQuery("PRAGMA quick_check")) {
                    if (!r.next() || !"ok".equals(r.getString(1))) throw new IOException("Invalid SQLite edition");
                }
            }
            lookup = connection.prepareStatement("""
                SELECT r.producer,o.target,o.content FROM items i JOIN results r ON r.item=i.id
                JOIN result_occurrences l ON l.result=r.id JOIN occurrences o ON o.id=l.occurrence
                WHERE i.catalog_id=? AND i.metadata_hash=? ORDER BY r.producer,l.position
                """);
            var sha = MessageDigest.getInstance("SHA-256");
            try (var input = new DigestInputStream(Files.newInputStream(file), sha)) {
                input.transferTo(OutputStream.nullOutputStream());
            }
            var after = Files.readAttributes(file, BasicFileAttributes.class);
            if (before.fileKey()==null || !before.fileKey().equals(after.fileKey()) || !before.lastModifiedTime().equals(after.lastModifiedTime()))
                throw new IOException("Edition changed during initialization");
            digest = HexFormat.of().formatHex(sha.digest());
        } catch (Exception e) { close(); throw new IOException("Cannot open " + file + " for " + network, e); }
    }

    public List<Row> read(String catalogId, String hash) throws SQLException {
        if (lookup == null) return List.of();
        lookup.setString(1, catalogId);
        lookup.setString(2, hash);
        var rows = new ArrayList<Row>();
        try (var r = lookup.executeQuery()) {
            while (r.next()) rows.add(new Row(r.getString(1), r.getString(2), r.getString(3)));
        }
        return rows;
    }

    public void close() throws SQLException {
        if (connection != null) connection.close();
    }
}
