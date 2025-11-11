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

package org.lareferencia.core.util;

import java.io.File;

import org.lareferencia.core.metadata.SnapshotMetadata;

/**
 * Utilidades para construcción de rutas en el sistema de archivos.
 * 
 * PROPÓSITO:
 * - Centralizar lógica de sanitización de network acronyms
 * - Construir rutas consistentes para metadata store y parquet repositories
 * - Garantizar nombres de directorios seguros en cualquier OS
 * 
 * ESTRUCTURA UNIFICADA:
 * {basePath}/
 * ├── {NETWORK}/                    ← sanitizeNetworkAcronym()
 * │   ├── metadata/                 ← getMetadataStorePath()
 * │   │   └── {A/B/C/HASH.xml.gz}
 * │   └── snapshots/                ← getSnapshotsBasePath()
 * │       └── snapshot_{id}/        ← getSnapshotPath()
 * │           ├── catalog/
 * │           └── validation/
 */
public final class PathUtils {

    private PathUtils() {
        // Utility class
    }

    /**
     * Sanitiza el network acronym para uso en filesystem.
     * 
     * TRANSFORMACIONES:
     * - Convierte a mayúsculas
     * - Reemplaza caracteres conflictivos por guion bajo
     * - Caracteres permitidos: A-Z, 0-9, guion (-), guion bajo (_)
     * 
     * EJEMPLOS:
     * - "br" → "BR"
     * - "LA Referencia" → "LA_REFERENCIA"
     * - "mx-unam" → "MX-UNAM"
     * - "test@123" → "TEST_123"
     * 
     * @param networkAcronym acronym de la red (puede ser null)
     * @return acronym sanitizado o "UNKNOWN" si es null/vacío
     */
    public static String sanitizeNetworkAcronym(String networkAcronym) {
        if (networkAcronym == null || networkAcronym.trim().isEmpty()) {
            return "UNKNOWN";
        }
        
        // Convertir a mayúsculas y reemplazar caracteres no permitidos
        return networkAcronym.toUpperCase()
            .replaceAll("[^A-Z0-9\\-_]", "_");
    }

    /**
     * Extrae y sanitiza el network acronym desde SnapshotMetadata.
     * 
     * @param snapshotMetadata metadata del snapshot (puede ser null)
     * @return acronym sanitizado
     */
    private static String extractNetworkAcronym(SnapshotMetadata snapshotMetadata) {
        String acronym = snapshotMetadata != null ? snapshotMetadata.getNetworkAcronym() : null;
        return sanitizeNetworkAcronym(acronym);
    }

    /**
     * Construye la ruta base para metadata store de una red.
     * 
     * RUTA: {basePath}/{NETWORK}/metadata
     * 
     * @param basePath directorio base
     * @param snapshotMetadata metadata del snapshot
     * @return ruta completa al directorio de metadata
     */
    public static String getMetadataStorePath(String basePath, SnapshotMetadata snapshotMetadata) {
        String sanitized = extractNetworkAcronym(snapshotMetadata);
        return String.format("%s%s%s%smetadata", 
            basePath, File.separator, sanitized, File.separator);
    }

    /**
     * Construye la ruta base para snapshots de una red.
     * 
     * RUTA: {basePath}/{NETWORK}/snapshots
     * 
     * @param basePath directorio base
     * @param snapshotMetadata metadata del snapshot
     * @return ruta completa al directorio de snapshots
     */
    public static String getSnapshotsBasePath(String basePath, SnapshotMetadata snapshotMetadata) {
        String sanitized = extractNetworkAcronym(snapshotMetadata);
        return String.format("%s%s%s%ssnapshots", 
            basePath, File.separator, sanitized, File.separator);
    }

    /**
     * Construye la ruta completa para un snapshot específico.
     * 
     * RUTA: {basePath}/{NETWORK}/snapshots/snapshot_{id}
     * 
     * @param basePath directorio base
     * @param snapshotMetadata metadata del snapshot
     * @return ruta completa al directorio del snapshot
     */
    public static String getSnapshotPath(String basePath, SnapshotMetadata snapshotMetadata) {
        if (snapshotMetadata == null || snapshotMetadata.getSnapshotId() == null) {
            throw new IllegalArgumentException("SnapshotMetadata and snapshotId cannot be null");
        }
        
        String snapshotsBase = getSnapshotsBasePath(basePath, snapshotMetadata);
        return String.format("%s%ssnapshot_%d", 
            snapshotsBase, File.separator, snapshotMetadata.getSnapshotId());
    }
}
