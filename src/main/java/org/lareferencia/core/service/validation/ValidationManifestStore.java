/*
 *   Copyright (c) 2013-2026. LA Referencia / Red CLARA and others
 *
 *   This program is free software: you can redistribute it and/or modify
 *   it under the terms of the GNU Affero General Public License as published by
 *   the Free Software Foundation, either version 3 of the License, or
 *   (at your option) any later version.
 */

package org.lareferencia.core.service.validation;

import java.io.IOException;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.Paths;
import java.util.Optional;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.lareferencia.core.metadata.SnapshotMetadata;
import org.lareferencia.core.util.PathUtils;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Stores optional validation provenance separately from validation results.
 * Snapshots created before this component existed simply have no manifest.
 */
@Service
public class ValidationManifestStore {

    private static final Logger logger = LogManager.getLogger(ValidationManifestStore.class);
    static final String VALIDATION_SUBDIR = "validation";
    static final String MANIFEST_FILENAME = "validation-manifest.json";

    @Value("${store.basepath:/tmp/data/}")
    private String basePath;

    private final ObjectMapper objectMapper = new ObjectMapper();

    public void write(SnapshotMetadata metadata, ValidatorFingerprint fingerprint) throws IOException {
        Path manifest = manifestPath(metadata);
        Files.createDirectories(manifest.getParent());
        Path temporary = Files.createTempFile(manifest.getParent(), MANIFEST_FILENAME, ".tmp");
        try {
            objectMapper.writeValue(temporary.toFile(), fingerprint);
            try {
                Files.move(temporary, manifest, StandardCopyOption.ATOMIC_MOVE,
                        StandardCopyOption.REPLACE_EXISTING);
            } catch (AtomicMoveNotSupportedException e) {
                Files.move(temporary, manifest, StandardCopyOption.REPLACE_EXISTING);
            }
        } finally {
            Files.deleteIfExists(temporary);
        }
    }

    /**
     * Returns empty for a historical or unreadable manifest. Either case must
     * cause a future incremental implementation to choose a safe full validation.
     */
    public Optional<ValidatorFingerprint> read(SnapshotMetadata metadata) {
        Path manifest = manifestPath(metadata);
        if (!Files.isRegularFile(manifest)) {
            return Optional.empty();
        }
        try {
            return Optional.of(objectMapper.readValue(manifest.toFile(), ValidatorFingerprint.class));
        } catch (IOException e) {
            logger.warn("Cannot read validation manifest {}. Treating its fingerprint as unknown.", manifest, e);
            return Optional.empty();
        }
    }

    private Path manifestPath(SnapshotMetadata metadata) {
        return Paths.get(PathUtils.getSnapshotPath(basePath, metadata), VALIDATION_SUBDIR, MANIFEST_FILENAME);
    }
}
