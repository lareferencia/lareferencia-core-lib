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

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.springframework.boot.context.event.ApplicationEnvironmentPreparedEvent;
import org.springframework.context.ApplicationListener;
import org.springframework.core.env.ConfigurableEnvironment;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.io.support.ResourcePropertySource;

/**
 * Spring application listener that loads properties from a directory.
 * <p>
 * Loads all {@code *.properties} files from
 * {@code ${app.config.dir}/application.properties.d/} directory,
 * sorted alphabetically. This allows modular configuration where each
 * concern can have its own properties file (e.g., 00-server.properties,
 * 01-dbconnection.properties, etc.).
 * </p>
 * 
 * <h3>Usage:</h3>
 * 
 * <pre>{@code
 * SpringApplicationBuilder builder = new SpringApplicationBuilder(MainApp.class);
 * builder.listeners(new PropertiesDirectoryListener());
 * builder.run(args);
 * }</pre>
 * 
 * @author LA Referencia Team
 * @see ConfigPathResolver
 */
public class PropertiesDirectoryListener
        implements ApplicationListener<ApplicationEnvironmentPreparedEvent> {

    private static final String PROPERTIES_DIR = "application.properties.d";
    private static final String PROPERTIES_EXTENSION = ".properties";
    private static final String LOG_PREFIX = "[PropertiesLoader]";

    @Override
    public void onApplicationEvent(ApplicationEnvironmentPreparedEvent event) {
        Path dir = ConfigPathResolver.resolvePath(PROPERTIES_DIR);

        if (!Files.exists(dir) || !Files.isDirectory(dir)) {
            System.out.println(LOG_PREFIX + " Directory not found: " + dir);
            return;
        }

        try (Stream<Path> stream = Files.list(dir)) {
            ConfigurableEnvironment env = event.getEnvironment();

            List<Path> propertyFiles = stream
                    .filter(p -> p.toString().endsWith(PROPERTIES_EXTENSION))
                    .sorted()
                    .collect(Collectors.toList());

            for (Path file : propertyFiles) {
                try {
                    ResourcePropertySource source = new ResourcePropertySource(
                            "custom-" + file.getFileName().toString(),
                            new FileSystemResource(file.toFile()));
                    env.getPropertySources().addLast(source);
                    System.out.println(LOG_PREFIX + " Loaded: " + file.getFileName());
                } catch (IOException e) {
                    System.err.println(LOG_PREFIX + " Failed to load: " + file + " - " + e.getMessage());
                }
            }

        } catch (IOException e) {
            System.err.println(LOG_PREFIX + " Error listing directory: " + e.getMessage());
        }
    }
}
