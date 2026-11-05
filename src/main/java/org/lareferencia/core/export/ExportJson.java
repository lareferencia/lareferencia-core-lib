package org.lareferencia.core.export;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.InputStream;
import java.nio.file.*;
import java.security.MessageDigest;
import java.util.HexFormat;

final class ExportJson {
    static final ObjectMapper JSON = new ObjectMapper();
    static ObjectNode object() { return JSON.createObjectNode(); }
    static String hash(Path path) throws Exception {
        MessageDigest digest = MessageDigest.getInstance("SHA-256");
        try (InputStream input = Files.newInputStream(path)) {
            byte[] buffer = new byte[65536]; int n;
            while ((n = input.read(buffer)) != -1) digest.update(buffer, 0, n);
        }
        return "sha256:" + HexFormat.of().formatHex(digest.digest());
    }
    static void write(Path path, Object value) throws Exception {
        Path temporary = path.resolveSibling(path.getFileName() + ".tmp");
        JSON.writerWithDefaultPrettyPrinter().writeValue(temporary.toFile(), value);
        move(temporary, path);
    }
    /** Streams the file inventory so per-record XML exports do not retain all paths in memory. */
    static void writeManifest(Path root, ObjectNode manifest) throws Exception {
        Path temporary = root.resolve("manifest.json.tmp");
        try (var output = Files.newOutputStream(temporary);
                var generator = JSON.getFactory().createGenerator(output)) {
            generator.useDefaultPrettyPrinter();
            generator.writeStartObject();
            var fields = manifest.fields();
            while (fields.hasNext()) {
                var field = fields.next();
                generator.writeFieldName(field.getKey());
                generator.writeTree(field.getValue());
            }
            generator.writeArrayFieldStart("files");
            try (var paths = Files.walk(root)) {
                var iterator = paths.filter(Files::isRegularFile).iterator();
                while (iterator.hasNext()) {
                    Path path = iterator.next();
                    String relative = root.relativize(path).toString().replace(java.io.File.separatorChar, '/');
                    if (relative.equals("manifest.json") || relative.endsWith(".tmp") || relative.endsWith("-journal")) continue;
                    generator.writeTree(object().put("path", relative).put("bytes", Files.size(path)).put("sha256", hash(path)));
                }
            }
            generator.writeEndArray();
            generator.writeEndObject();
        }
        move(temporary, root.resolve("manifest.json"));
    }
    static void move(Path from, Path to) throws java.io.IOException {
        try { Files.move(from, to, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING); }
        catch (AtomicMoveNotSupportedException e) { Files.move(from, to, StandardCopyOption.REPLACE_EXISTING); }
    }
}
