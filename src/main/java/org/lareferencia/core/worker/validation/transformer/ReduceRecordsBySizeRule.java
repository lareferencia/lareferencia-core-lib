package org.lareferencia.core.worker.validation.transformer;

import java.nio.charset.StandardCharsets;
import java.util.List;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Getter;
import org.lareferencia.core.domain.IOAIRecord;
import org.lareferencia.core.metadata.OAIRecordMetadata;
import org.lareferencia.core.metadata.SnapshotMetadata;
import org.lareferencia.core.worker.validation.AbstractTransformerRule;
import org.lareferencia.core.worker.validation.SchemaProperty;
import org.lareferencia.core.worker.validation.ValidatorRuleMeta;

@Getter
@ValidatorRuleMeta
public class ReduceRecordsBySizeRule extends AbstractTransformerRule {
    @SchemaProperty(order = 1)
    private final long maxRecordSizeBytes;
    @SchemaProperty(order = 2)
    private final List<String> fieldsToRemove;

    @JsonCreator
    public ReduceRecordsBySizeRule(@JsonProperty(value = "maxRecordSizeBytes", required = true) long maxRecordSizeBytes,
                                   @JsonProperty(value = "fieldsToRemove", required = true) List<String> fieldsToRemove) {
        if (maxRecordSizeBytes < 1) throw new IllegalArgumentException("maxRecordSizeBytes must be positive");
        this.maxRecordSizeBytes = maxRecordSizeBytes;
        this.fieldsToRemove = List.copyOf(fieldsToRemove);
    }

    @Override
    public boolean transform(SnapshotMetadata snapshot, IOAIRecord record, OAIRecordMetadata metadata) {
        return metadata.toString().getBytes(StandardCharsets.UTF_8).length > maxRecordSizeBytes
                && ReduceHeavyRecords.removeFields(metadata, fieldsToRemove);
    }
}
