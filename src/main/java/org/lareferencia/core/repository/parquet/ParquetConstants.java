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

package org.lareferencia.core.repository.parquet;

/**
 * Constantes compartidas para el subsistema de almacenamiento Parquet.
 * 
 * Centraliza nombres de directorios, prefijos de archivos y otras constantes
 * utilizadas por múltiples clases del paquete parquet.
 */
public final class ParquetConstants {

    private ParquetConstants() {
        // Utility class - no instantiation
    }

    // ========================================
    // SUBDIRECTORIOS
    // ========================================
    
    /** Subdirectorio para archivos de validación */
    public static final String VALIDATION_SUBDIR = "validation";
    
    /** Subdirectorio para archivos de catálogo OAI */
    public static final String CATALOG_SUBDIR = "catalog";

    // ========================================
    // ARCHIVOS DE VALIDACIÓN
    // ========================================
    
    /** Prefijo para archivos batch de records de validación */
    public static final String VALIDATION_BATCH_PREFIX = "records_batch_";
    
    /** Sufijo para archivos Parquet */
    public static final String PARQUET_SUFFIX = ".parquet";
    
    /** Nombre del archivo de índice ligero de validación */
    public static final String VALIDATION_INDEX_FILE = "validation_index.parquet";
    
    /** Nombre del archivo de estadísticas de validación (JSON) */
    public static final String VALIDATION_STATS_FILE = "validation_stats.json";

    // ========================================
    // ARCHIVOS DE CATÁLOGO
    // ========================================
    
    /** Prefijo para archivos batch de catálogo OAI */
    public static final String CATALOG_BATCH_PREFIX = "catalog_batch_";
    
    /** Nombre del archivo de índice de catálogo */
    public static final String CATALOG_INDEX_FILE = "catalog_index.parquet";

    // ========================================
    // ARCHIVOS DE METADATA
    // ========================================
    
    /** Nombre del archivo de metadata del snapshot */
    public static final String METADATA_FILE = "metadata.json";
    
    /** Alias para compatibilidad - nombre del archivo de metadata */
    public static final String METADATA_FILENAME = METADATA_FILE;
    
    /** Nombre del archivo de estadísticas de validación (JSON) */
    public static final String VALIDATION_STATS_FILENAME = "validation-stats.json";

    // ========================================
    // CONFIGURACIONES POR DEFECTO
    // ========================================
    
    /** Umbral por defecto de registros por archivo batch */
    public static final int DEFAULT_FLUSH_THRESHOLD = 100000;
    
    /** Tamaño máximo de snapshots en cache por defecto */
    public static final int DEFAULT_CACHE_MAX_SNAPSHOTS = 5;
    
    /** TTL de cache en minutos por defecto */
    public static final int DEFAULT_CACHE_TTL_MINUTES = 30;
}
