package org.lareferencia.core.export;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.*;
import java.util.zip.GZIPOutputStream;

/** Writes complete records, accounting for framing and UTF-8 bytes before compression. */
final class ExportBatchWriter implements AutoCloseable {
    record Location(String file, Long position) {}
    private final Path root;
    private final ExportRequest request;
    @FunctionalInterface interface OutputFactory { OutputStream open(Path path) throws IOException; }
    private final OutputFactory outputFactory;
    private final String prefix, suffix;
    private OutputStream stream;
    private Path temporary, target;
    private String partition;
    private long count, bytes;
    private int sequence;
    long oversizedRecords;
    private boolean broken;
    ExportBatchWriter(Path root, ExportRequest request) {
        this(root, request, path -> new BufferedOutputStream(Files.newOutputStream(path, StandardOpenOption.CREATE_NEW)));
    }
    ExportBatchWriter(Path root, ExportRequest request, OutputFactory outputFactory) {
        this.root = root; this.request = request; this.outputFactory = outputFactory;
        prefix = switch (request.format()) { case "json" -> "["; case "xml" -> "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<records>\n"; default -> ""; };
        suffix = switch (request.format()) { case "json" -> "]\n"; case "xml" -> "</records>\n"; default -> ""; };
    }
    Location write(String partition, String catalogId, byte[] record) throws Exception {
        try { return append(partition, catalogId, record); }
        catch (Exception e) { broken = true; throw e; }
    }
    private Location append(String partition, String catalogId, byte[] record) throws Exception {
        if (request.layout().equals("per-record")) {
            close();
            Path directory = root.resolve(partition); Files.createDirectories(directory);
            target = directory.resolve(catalogId + ".xml" + extension());
            temporary = target.resolveSibling(target.getFileName() + ".tmp");
            try (OutputStream output = output(temporary)) { output.write(record); }
            ExportJson.move(temporary, target);
            return new Location(relative(target), null);
        }
        long separator = request.format().equals("json") && count > 0 ? 1 : 0;
        long terminator = request.format().equals("jsonl") || request.format().equals("xml") ? 1 : 0;
        long extra = record.length + separator + terminator;
        if (stream != null && (!partition.equals(this.partition) || count >= request.recordLimit()
                || bytes + extra + utf8(suffix).length > request.byteLimit())) close();
        if (stream == null) {
            this.partition = partition;
            Path directory = root.resolve(partition); Files.createDirectories(directory);
            target = directory.resolve(String.format(Locale.ROOT, "part-%06d.%s%s", ++sequence, request.format(), extension()));
            temporary = target.resolveSibling(target.getFileName() + ".tmp");
            stream = output(temporary); count = 0; bytes = 0;
            put(utf8(prefix));
        }
        if (request.format().equals("json") && count > 0) put(utf8(","));
        long position = request.format().equals("jsonl") ? count + 1 : count;
        put(record);
        if (terminator > 0) put(utf8("\n"));
        count++;
        if (bytes + utf8(suffix).length > request.byteLimit()) oversizedRecords++;
        Location location = new Location(relative(target), position);
        if (count >= request.recordLimit() || bytes + utf8(suffix).length >= request.byteLimit()) close();
        return location;
    }
    private String extension() { return request.compression().equals("gzip") ? ".gz" : ""; }
    private String relative(Path path) { return root.relativize(path).toString().replace(File.separatorChar, '/'); }
    private OutputStream output(Path path) throws IOException {
        OutputStream raw = outputFactory.open(path);
        try { return request.compression().equals("gzip") ? new GZIPOutputStream(raw) : raw; }
        catch (IOException e) { raw.close(); throw e; }
    }
    private static byte[] utf8(String value) { return value.getBytes(StandardCharsets.UTF_8); }
    private void put(byte[] value) throws IOException { stream.write(value); bytes += value.length; }
    @Override public void close() throws IOException {
        if (stream == null) return;
        OutputStream current = stream; stream = null;
        try { if (!broken) current.write(utf8(suffix)); } finally { current.close(); }
        if (!broken) ExportJson.move(temporary, target);
        count = 0; bytes = 0;
    }
}
