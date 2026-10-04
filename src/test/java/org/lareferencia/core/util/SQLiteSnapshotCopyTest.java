package org.lareferencia.core.util;

import static org.junit.jupiter.api.Assertions.*;
import java.nio.file.Path;
import java.sql.DriverManager;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class SQLiteSnapshotCopyTest {
    @TempDir Path directory;
    @Test void includesCommittedRowsStillInWalWithoutChangingParent() throws Exception {
        Path parent = directory.resolve("parent.db"), child = directory.resolve("child's.db");
        try (var connection = DriverManager.getConnection("jdbc:sqlite:" + parent); var statement = connection.createStatement()) {
            statement.execute("PRAGMA journal_mode=WAL"); statement.execute("PRAGMA wal_autocheckpoint=0");
            statement.execute("CREATE TABLE records (value TEXT)"); statement.execute("INSERT INTO records VALUES ('committed')");
            assertTrue(java.nio.file.Files.exists(Path.of(parent + "-wal")));
            SQLiteSnapshotCopy.copy(parent,child);
            try (var copy = DriverManager.getConnection("jdbc:sqlite:" + child); var query = copy.createStatement();
                    var result = query.executeQuery("SELECT value FROM records")) {
                assertTrue(result.next()); assertEquals("committed",result.getString(1));
            }
            try (var result = statement.executeQuery("SELECT COUNT(*) FROM records")) { assertTrue(result.next()); assertEquals(1,result.getInt(1)); }
        }
    }
    @Test void missingParentCannotCreateAnEmptyIncremental() {
        assertThrows(java.io.IOException.class, () -> SQLiteSnapshotCopy.copy(directory.resolve("missing.db"),directory.resolve("child.db")));
    }
}
