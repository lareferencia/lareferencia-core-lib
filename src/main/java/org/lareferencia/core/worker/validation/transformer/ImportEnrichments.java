package org.lareferencia.core.worker.validation.transformer;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.Getter;
import lombok.Setter;
import org.lareferencia.core.domain.IOAIRecord;
import org.lareferencia.core.metadata.*;
import org.lareferencia.core.worker.validation.*;
import java.util.*;

@ValidatorRuleMeta
public class ImportEnrichments extends AbstractTransformerRule {
    private static final ObjectMapper JSON = new ObjectMapper();
    @Getter @Setter @SchemaProperty(order = 1) private String filename;
    @Getter @Setter @SchemaProperty(order = 2) private List<EnrichmentMapping> mappings = new ArrayList<>();
    private transient SQLiteEnrichmentEdition edition;

    @Getter @Setter
    public static class EnrichmentMapping {
        @SchemaProperty(order = 1) private String source;
        @SchemaProperty(order = 2) private String producer;
        @SchemaProperty(order = 3, defaultValue = "value") private String property = "value";
        @SchemaProperty(order = 4) private String destination;
        boolean accepts(SQLiteEnrichmentEdition.Row row) {
            return source.equals(row.target()) && (producer == null || producer.isBlank() || producer.equals(row.producer()));
        }

        String selectedProperty() {
            return property == null || property.isBlank() ? "value" : property;
        }

        String target() { return destination == null || destination.isBlank() ? source : destination; }
    }

    public void validateConfiguration() {
        if (filename == null || filename.isBlank() || !filename.matches("[^/\\\\]+") || filename.equals(".") || filename.equals(".."))
            throw new IllegalArgumentException("Expected a simple SQLite filename");
        if (mappings == null || mappings.isEmpty()) throw new IllegalArgumentException("Expected field mappings");
        for (EnrichmentMapping mapping : mappings) {
            if (mapping == null || mapping.source == null || mapping.source.isBlank() || !mapping.target().matches("[\\w-]+(?:\\.[\\w-]+){2}"))
                throw new IllegalArgumentException("Expected source and qualified XOAI destination");
        }
    }

    public void bind(SQLiteEnrichmentEdition edition) { this.edition = edition; }

    @Override
    public boolean transform(SnapshotMetadata snapshot, IOAIRecord record, OAIRecordMetadata metadata) throws ValidationException {
        if (edition == null) throw new ValidationException("Enrichment rule has not been initialized");
        boolean changed = false;
        try {
            var rows = edition.read(record.getId(), record.getOriginalMetadataHash());
            for (EnrichmentMapping mapping : mappings) {
                for (var row : rows) {
                    if (!mapping.accepts(row)) continue;
                    JsonNode value = JSON.readTree(row.content()).path(mapping.selectedProperty());
                    if (!value.isValueNode() || value.isNull() || value.asText().isBlank()) continue;
                    if (!metadata.containsFieldValue(mapping.target(), value.asText())) {
                        metadata.addFieldOcurrence(mapping.target(), value.asText());
                        changed = true;
                    }
                }
            }
            return changed;
        } catch (Exception e) { throw new ValidationException("Reading " + filename + " for " + record.getId(), e); }
    }
}
