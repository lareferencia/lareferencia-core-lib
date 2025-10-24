/*
 *   Copyright (c) 2013-2022. LA Referencia / Red CLARA and others
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

package org.lareferencia.backend.config.parquet;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

import lombok.Getter;
import lombok.Setter;

/**
 * Configuración para el almacenamiento de estadísticas de validación en archivos Parquet.
 */
@Configuration
@ConfigurationProperties(prefix = "validation.parquet")
@Getter
@Setter
public class ValidationParquetConfig {

    /**
     * Ruta base donde se almacenarán los archivos de estadísticas de validación
     * Por defecto: /tmp/validation-stats
     */
    private String basePath = "/tmp/validation-stats";

    /**
     * Tipo de compresión para los archivos Parquet
     * Opciones: UNCOMPRESSED, SNAPPY, GZIP, LZO, BROTLI, LZ4, ZSTD
     * Por defecto: SNAPPY
     */
    private String compression = "SNAPPY";

    /**
     * Tamaño de bloque en bytes para los archivos Parquet
     * Por defecto: 128MB (134217728 bytes)
     */
    private long blockSize = 134217728L;

    /**
     * Tamaño de página en bytes para los archivos Parquet
     * Por defecto: 1MB (1048576 bytes)
     */
    private int pageSize = 1048576;

    /**
     * Habilitar escritura de diccionarios para optimizar el almacenamiento
     * Por defecto: true
     */
    private boolean enableDictionary = true;

    /**
     * Habilitar estadísticas de columnas para optimizar consultas
     * Por defecto: true
     */
    private boolean enableStatistics = true;

    /**
     * Número máximo de archivos a mantener por snapshot
     * Si se supera este número, se consolidarán los archivos más antiguos
     * Por defecto: 10
     */
    private int maxFilesPerSnapshot = 10;

    /**
     * Habilitar compactación automática de archivos
     * Por defecto: true
     */
    private boolean enableAutoCompaction = true;

    /**
     * Intervalo en minutos para la compactación automática
     * Por defecto: 60 minutos
     */
    private int compactionIntervalMinutes = 60;

    /**
     * Tamaño mínimo de archivo en bytes para activar compactación
     * Por defecto: 10MB (10485760 bytes)
     */
    private long minFileSize = 10485760L;

    /**
     * Tamaño máximo de archivo en bytes antes de dividir
     * Por defecto: 1GB (1073741824 bytes)
     */
    private long maxFileSize = 1073741824L;

    /**
     * Habilitar cache en memoria para consultas frecuentes
     * Por defecto: true
     */
    private boolean enableCache = true;

    /**
     * Tamaño máximo del cache en MB
     * Por defecto: 256MB
     */
    private int cacheMaxSizeMB = 256;

    /**
     * Tiempo de vida del cache en minutos
     * Por defecto: 30 minutos
     */
    private int cacheTtlMinutes = 30;

    /**
     * Formato de particionamiento de archivos
     * Opciones: SNAPSHOT_ID, DATE, NETWORK_ACRONYM
     * Por defecto: SNAPSHOT_ID
     */
    private String partitioningStrategy = "SNAPSHOT_ID";

    /**
     * Habilitar logging detallado de operaciones Parquet
     * Por defecto: false
     */
    private boolean enableDetailedLogging = false;

    /**
     * Número de hilos para operaciones paralelas
     * Por defecto: 4
     */
    private int parallelThreads = 4;

    /**
     * Habilitar validación de esquema al escribir archivos
     * Por defecto: true
     */
    private boolean enableSchemaValidation = true;

    /**
     * Versión del formato Parquet a usar
     * Opciones: 1.0, 2.0
     * Por defecto: 2.0
     */
    private String parquetVersion = "2.0";
}
