package org.lareferencia.core.util;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;
import org.lareferencia.core.domain.Network;
import org.lareferencia.core.domain.OAIRecord;
import org.lareferencia.core.metadata.SnapshotMetadata;

class PrefixedRecordFingerprintHelperTest {
    private SnapshotMetadata snapshot(String acronym, long id) {
        Network network = new Network();
        network.setAcronym(acronym);
        SnapshotMetadata metadata = new SnapshotMetadata(id);
        metadata.setNetwork(network);
        return metadata;
    }

    private OAIRecord record(String identifier) {
        OAIRecord record = new OAIRecord();
        record.setIdentifier(identifier);
        return record;
    }

    @Test
    void idIsStableAndIndependentOfSnapshotMetadata() {
        PrefixedRecordFingerprintHelper helper = new PrefixedRecordFingerprintHelper();
        OAIRecord record = record("oai:test:1");
        assertEquals(helper.getRecordIdLong(record, snapshot("NET", 1)),
                helper.getRecordIdLong(record, snapshot("NET", 2)));
        assertEquals(helper.getFingerprint(record, snapshot("NET", 1)), "NET_" + org.apache.commons.codec.digest.DigestUtils.md5Hex("oai:test:1"));
    }

    @Test
    void idChangesWithIdentifierOrNetworkAndIsPositive() {
        PrefixedRecordFingerprintHelper helper = new PrefixedRecordFingerprintHelper();
        long base = helper.getRecordIdLong(record("one"), snapshot("A", 1));
        assertNotEquals(base, helper.getRecordIdLong(record("two"), snapshot("A", 1)));
        assertNotEquals(base, helper.getRecordIdLong(record("one"), snapshot("B", 1)));
        assertTrue(base >= 0 && base <= Long.MAX_VALUE);
    }
}
