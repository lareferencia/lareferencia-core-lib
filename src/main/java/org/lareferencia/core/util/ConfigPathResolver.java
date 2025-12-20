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

import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * Utility class for resolving configuration directory paths.
 * <p>
 * Allows the base configuration directory to be customized via the system
 * property
 * {@code app.config.dir}. If not specified, defaults to "config" (relative to
 * working directory).
 * </p>
 * 
 * <h3>Usage Examples:</h3>
 * <ul>
 * <li>Default (relative): {@code java -jar app.jar} → uses ./config</li>
 * <li>Relative path:
 * {@code java -Dapp.config.dir=../shared-config -jar app.jar}</li>
 * <li>Absolute path:
 * {@code java -Dapp.config.dir=/etc/lrharvester -jar app.jar}</li>
 * <li>Docker: {@code java -Dapp.config.dir=/app/config -jar app.jar}</li>
 * </ul>
 * 
 * @author LA Referencia Team
 */
public final class ConfigPathResolver {

    /** Default configuration directory (relative to working directory) */
    public static final String DEFAULT_CONFIG_DIR = "config";

    /** System property name for overriding the config directory */
    public static final String CONFIG_DIR_PROPERTY = "app.config.dir";

    /** Cached resolved path */
    private static String resolvedConfigDir = null;

    private ConfigPathResolver() {
        // Utility class - prevent instantiation
    }

    /**
     * Gets the base configuration directory.
     * <p>
     * Reads from system property {@code app.config.dir}, defaulting to "config".
     * The result is cached after first resolution.
     * </p>
     * 
     * @return the configuration directory path (may be relative or absolute)
     */
    public static String getConfigDir() {
        if (resolvedConfigDir == null) {
            resolvedConfigDir = System.getProperty(CONFIG_DIR_PROPERTY, DEFAULT_CONFIG_DIR);
            System.out.println("[ConfigPathResolver] Using config directory: " + resolvedConfigDir);
        }
        return resolvedConfigDir;
    }

    /**
     * Resolves a path relative to the configuration directory.
     * <p>
     * Example: {@code resolve("application.properties.d")} returns
     * "config/application.properties.d"
     * </p>
     * 
     * @param relativePath the path relative to the config directory
     * @return the full path combining config directory and relative path
     */
    public static String resolve(String relativePath) {
        String configDir = getConfigDir();
        if (configDir.endsWith("/") || configDir.endsWith("\\")) {
            return configDir + relativePath;
        }
        return configDir + "/" + relativePath;
    }

    /**
     * Resolves a path relative to the configuration directory as a {@link Path}
     * object.
     * 
     * @param relativePath the path relative to the config directory
     * @return the resolved Path object
     */
    public static Path resolvePath(String relativePath) {
        return Paths.get(resolve(relativePath));
    }

    /**
     * Gets the absolute path of the configuration directory.
     * 
     * @return the absolute path of the config directory
     */
    public static String getAbsoluteConfigDir() {
        return Paths.get(getConfigDir()).toAbsolutePath().toString();
    }

    /**
     * Resets the cached config directory (useful for testing).
     */
    public static void reset() {
        resolvedConfigDir = null;
    }
}
