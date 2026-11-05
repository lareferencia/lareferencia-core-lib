package org.lareferencia.core.export;

import java.nio.file.Path;
import java.util.List;
import java.util.Set;

/** Public export options; null limits select defaults (and distinguish explicit per-record limits). */
public record ExportRequest(List<String> networks, boolean allNetworks, String variant, String format,
        Path mapping, String layout, String partitionBy, Long maxRecords, Long maxBytes,
        String compression, boolean validOnly, Path output) {
    public ExportRequest {
        networks = networks == null ? List.of() : List.copyOf(networks);
        if (allNetworks == !networks.isEmpty()) throw new IllegalArgumentException("Select networks or all-networks");
        if (networks.stream().anyMatch(n -> n == null || n.isBlank()) || Set.copyOf(networks).size() != networks.size())
            throw new IllegalArgumentException("Blank or duplicate acronym");
        if (!Set.of("original", "processed").contains(variant) || !Set.of("xml", "json", "jsonl").contains(format)
                || !Set.of("collection", "per-record").contains(layout) || !Set.of("network", "none").contains(partitionBy)
                || !Set.of("none", "gzip").contains(compression)) throw new IllegalArgumentException("Invalid export option");
        if (format.equals("xml") ? mapping != null : mapping == null) throw new IllegalArgumentException("Mapping required for JSON and forbidden for XML");
        if (!format.equals("xml") && !layout.equals("collection")) throw new IllegalArgumentException("layout is an XML option");
        if (layout.equals("per-record") && (maxRecords != null || maxBytes != null)) throw new IllegalArgumentException("Per-record XML does not accept batch limits");
        if (maxRecords != null && maxRecords < 1 || maxBytes != null && maxBytes < 1) throw new IllegalArgumentException("Limits must be positive");
    }
    public long recordLimit() { return maxRecords == null ? 10000 : maxRecords; }
    public long byteLimit() { return maxBytes == null ? 128L * 1024 * 1024 : maxBytes; }
    public static long parseBytes(String value) {
        var match = java.util.regex.Pattern.compile("([1-9][0-9]*)(B|KiB|MiB|GiB)?").matcher(value);
        if (!match.matches()) throw new IllegalArgumentException("Expected bytes, KiB, MiB or GiB");
        long multiplier = switch (match.group(2) == null ? "B" : match.group(2)) {
            case "KiB" -> 1024; case "MiB" -> 1024 * 1024; case "GiB" -> 1024L * 1024 * 1024; default -> 1;
        };
        return Math.multiplyExact(Long.parseLong(match.group(1)), multiplier);
    }
}
