/*
 *   Copyright (c) 2013-2026. LA Referencia / Red CLARA and others
 *
 *   This program is free software: you can redistribute it and/or modify
 *   it under the terms of the GNU Affero General Public License as published by
 *   the Free Software Foundation, either version 3 of the License, or
 *   (at your option) any later version.
 */

package org.lareferencia.core.service.validation;

/**
 * Immutable provenance data for the validation configuration used by a snapshot.
 */
public class ValidatorFingerprint {

    private int formatVersion;
    private String algorithm;
    private String canonicalizer;
    private String hash;
    private String scope;

    public ValidatorFingerprint() {
        // Required by Jackson when reading manifests.
    }

    public ValidatorFingerprint(int formatVersion, String algorithm, String canonicalizer, String hash) {
        this.formatVersion = formatVersion;
        this.algorithm = algorithm;
        this.canonicalizer = canonicalizer;
        this.hash = hash;
    }

    public int getFormatVersion() {
        return formatVersion;
    }

    public void setFormatVersion(int formatVersion) {
        this.formatVersion = formatVersion;
    }

    public String getAlgorithm() {
        return algorithm;
    }

    public void setAlgorithm(String algorithm) {
        this.algorithm = algorithm;
    }

    public String getCanonicalizer() {
        return canonicalizer;
    }

    public void setCanonicalizer(String canonicalizer) {
        this.canonicalizer = canonicalizer;
    }

    public String getHash() {
        return hash;
    }

    public void setHash(String hash) {
        this.hash = hash;
    }

    public String getScope() { return scope; }
    public void setScope(String scope) { this.scope = scope; }
}
