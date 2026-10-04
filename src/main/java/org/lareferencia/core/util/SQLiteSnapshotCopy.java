package org.lareferencia.core.util;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.DriverManager;
import java.sql.SQLException;

/** Copies a consistent SQLite snapshot, including committed WAL contents. */
public final class SQLiteSnapshotCopy {
    private SQLiteSnapshotCopy() { }

    public static void copy(Path source, Path destination) throws IOException {
        if (!Files.isRegularFile(source)) throw new IOException("Missing parent database: " + source);
        Files.createDirectories(destination.getParent());
        Files.deleteIfExists(destination);
        Files.deleteIfExists(Path.of(destination + "-wal"));
        Files.deleteIfExists(Path.of(destination + "-shm"));
        try (var connection = DriverManager.getConnection("jdbc:sqlite:" + source.toAbsolutePath());
                var statement = connection.createStatement()) {
            statement.execute("VACUUM INTO '" + destination.toAbsolutePath().toString().replace("'", "''") + "'");
        } catch (SQLException e) { throw new IOException("Cannot copy SQLite snapshot", e); }
    }
}
