/*
 *   Copyright (c) 2013-2025. LA Referencia / Red CLARA and others
 *
 *   This program is free software: you can redistribute it and/or modify
 *   it under the terms of the GNU Affero General Public License as published by
 *   the Free Software Foundation, either version 3 of the License, or
 *   (at your option) any later version.
 *
 *   This program is distributed in the hope that it will be useful,
 *   but WITHOUT ANY WARRANTY; without even the implied warranty of
 *   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *   GNU Affero General Public License for more details.
 *
 *   You should have received a copy of the GNU Affero General Public License
 *   along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 *   This file is part of LA Referencia software platform LRHarvester v4.x
 *   For any further information please contact Lautaro Matas <lmatas@gmail.com>
 */

package org.lareferencia.core.repository.catalog;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.LocalDateTime;
import java.util.Objects;

import org.lareferencia.core.domain.IOAIRecord;

/**
 * OAI Record para catálogo SQLite.
 * 
 * CAMPOS:
 * - id: Hash MD5 del identifier (PK, garantiza unicidad)
 * - identifier: Identificador OAI-PMH original
 * - datestamp: Fecha de última modificación según OAI-PMH
 * - originalMetadataHash: Hash MD5 del XML cosechado
 * - deleted: Flag de registro eliminado en origen
 * 
 * NOTAS:
 * - NO contiene estado de validación, transformación ni publishedHash
 * - Para harvesting incremental: se actualiza datestamp y hash si el registro
 * cambió
 * - ID se genera automáticamente como MD5 del identifier
 * 
 * ESTRUCTURA DE ARCHIVOS:
 * {basePath}/{NETWORK}/snapshots/snapshot_{id}/catalog/catalog.db
 */
public class OAIRecord implements IOAIRecord {

    private String id; // Hash MD5 del identifier
    private String identifier;
    private LocalDateTime datestamp;
    private String originalMetadataHash;
    private boolean deleted;

    /**
     * Constructor vacío (requerido para deserialización).
     */
    public OAIRecord() {
        this.deleted = false;
    }

    /**
     * Constructor completo.
     * 
     * @param id                   Hash MD5 del identifier (si es null, se calcula
     *                             automáticamente)
     * @param identifier           Identificador OAI
     * @param datestamp            Fecha de última modificación
     * @param originalMetadataHash Hash MD5 del XML original
     * @param deleted              Flag de eliminación
     */
    public OAIRecord(String id, String identifier, LocalDateTime datestamp,
            String originalMetadataHash, boolean deleted) {
        this.id = id;
        this.identifier = identifier;
        this.datestamp = datestamp;
        this.originalMetadataHash = originalMetadataHash;
        this.deleted = deleted;

        // Si no se proporciona ID, calcularlo automáticamente
        if (this.id == null && this.identifier != null) {
            this.id = generateIdFromIdentifier(this.identifier);
        }
    }

    // ============================================================================
    // FACTORY METHODS
    // ============================================================================

    /**
     * Crea un OAIRecord con ID auto-generado desde el identifier.
     * 
     * @param identifier           Identificador OAI
     * @param datestamp            Fecha de última modificación
     * @param originalMetadataHash Hash MD5 del XML original
     * @param deleted              Flag de eliminación
     * @return OAIRecord con ID calculado automáticamente
     */
    public static OAIRecord create(String identifier, LocalDateTime datestamp,
            String originalMetadataHash, boolean deleted) {
        String id = generateIdFromIdentifier(identifier);
        return new OAIRecord(id, identifier, datestamp, originalMetadataHash, deleted);
    }

    private static final ThreadLocal<MessageDigest> MD5_DIGEST = ThreadLocal.withInitial(() -> {
        try {
            return MessageDigest.getInstance("MD5");
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException("MD5 algorithm not found", e);
        }
    });

    /**
     * Genera un ID basado en el hash MD5 del identifier.
     * 
     * @param identifier Identificador OAI
     * @return Hash MD5 del identifier en formato hexadecimal
     */
    public static String generateIdFromIdentifier(String identifier) {
        if (identifier == null || identifier.isEmpty()) {
            throw new IllegalArgumentException("Identifier cannot be null or empty");
        }

        MessageDigest md = MD5_DIGEST.get();
        md.reset(); // Reset for reuse
        byte[] hashBytes = md.digest(identifier.getBytes());

        StringBuilder hexString = new StringBuilder();
        for (byte b : hashBytes) {
            String hex = Integer.toHexString(0xff & b);
            if (hex.length() == 1) {
                hexString.append('0');
            }
            hexString.append(hex);
        }
        return hexString.toString();
    }

    // ============================================================================
    // GETTERS & SETTERS
    // ============================================================================

    @Override
    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    @Override
    public String getIdentifier() {
        return identifier;
    }

    @Override
    public void setIdentifier(String identifier) {
        this.identifier = identifier;
    }

    @Override
    public LocalDateTime getDatestamp() {
        return datestamp;
    }

    @Override
    public void setDatestamp(LocalDateTime datestamp) {
        this.datestamp = datestamp;
    }

    @Override
    public String getOriginalMetadataHash() {
        return originalMetadataHash;
    }

    @Override
    public void setOriginalMetadataHash(String originalMetadataHash) {
        this.originalMetadataHash = originalMetadataHash;
    }

    public boolean isDeleted() {
        return deleted;
    }

    public void setDeleted(boolean deleted) {
        this.deleted = deleted;
    }

    // ============================================================================
    // OBJECT METHODS
    // ============================================================================

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        OAIRecord oaiRecord = (OAIRecord) o;
        return Objects.equals(id, oaiRecord.id) &&
                Objects.equals(identifier, oaiRecord.identifier);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, identifier);
    }

    @Override
    public String toString() {
        return "OAIRecord{" +
                "id='" + id + '\'' +
                ", identifier='" + identifier + '\'' +
                ", datestamp=" + datestamp +
                ", originalMetadataHash='" + originalMetadataHash + '\'' +
                ", deleted=" + deleted +
                '}';
    }
}
