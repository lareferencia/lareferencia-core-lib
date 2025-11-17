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

package org.lareferencia.core.service.management;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.lareferencia.core.metadata.ISnapshotStore;
import org.lareferencia.core.metadata.SnapshotMetadata;
import org.lareferencia.core.util.PathUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Stream;

/**
 * Service for managing network snapshot log entries.
 * Stores logs as text files in the snapshot directory.
 * 
 * ARQUITECTURA:
 * - Logs almacenados en {basePath}/{NETWORK}/snapshots/snapshot_{id}/snapshot.log
 * - Append incremental (no se sobrescribe)
 * - Formato: [timestamp] message
 * - Thread-safe mediante sincronización en escritura
 * - Metadata cargada dinámicamente desde ISnapshotStore
 * 
 * USO TÍPICO:
 * 1. Worker llama addEntry(snapshotId, "mensaje")
 * 2. El servicio carga metadata desde SnapshotStore automáticamente
 * 3. Se construye el path y se escribe el log
 * 
 * API SIMPLE: Solo se expone addEntry(snapshotId, message)
 */
@Component
@Scope("singleton")
public class SnapshotLogService {
	
	@Value("${store.basepath:/tmp/data/}")
	private String basePath;
	
	@Autowired
	private ISnapshotStore snapshotStore;
	
	private static final String LOG_FILENAME = "snapshot.log";
	private static final DateTimeFormatter TIMESTAMP_FORMAT = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS");
	
	// Cache de metadata para evitar consultar constantemente el SnapshotStore
	private final Map<Long, SnapshotMetadata> metadataCache = new ConcurrentHashMap<>();

	private static Logger logger = LogManager.getLogger(SnapshotLogService.class);

	/**
	 * Constructs a new SnapshotLogService instance.
	 */
	public SnapshotLogService() {
	}
	
	/**
	 * Precarga metadata de un snapshot en el cache.
	 * Útil para optimizar accesos posteriores cuando ya se tiene la metadata.
	 * 
	 * @param snapshotMetadata la metadata del snapshot
	 */
	public void cacheMetadata(SnapshotMetadata snapshotMetadata) {
		if (snapshotMetadata != null && snapshotMetadata.getSnapshotId() != null) {
			metadataCache.put(snapshotMetadata.getSnapshotId(), snapshotMetadata);
		}
	}
	
	/**
	 * Invalida la metadata de un snapshot del cache.
	 * 
	 * @param snapshotId el ID del snapshot
	 */
	public void invalidateMetadataCache(Long snapshotId) {
		if (snapshotId != null) {
			metadataCache.remove(snapshotId);
		}
	}
	
	/**
	 * Limpia todo el cache de metadata.
	 */
	public void clearMetadataCache() {
		metadataCache.clear();
	}
	
	/**
	 * Obtiene el path del archivo de log para un snapshot.
	 * Carga la metadata dinámicamente desde el SnapshotStore solo si no está en cache.
	 * 
	 * @param snapshotId el ID del snapshot
	 * @return Path al archivo snapshot.log
	 */
	private Path getLogFilePath(Long snapshotId) {
		if (snapshotId == null) {
			throw new IllegalArgumentException("SnapshotId cannot be null");
		}
		
		try {
			// Intentar obtener metadata del cache
			SnapshotMetadata metadata = metadataCache.get(snapshotId);
			
			// Si no está en cache, cargar desde SnapshotStore
			if (metadata == null) {
				metadata = snapshotStore.getSnapshotMetadata(snapshotId);
				if (metadata == null) {
					logger.warn("Metadata not found in SnapshotStore for snapshot {}", snapshotId);
					throw new IllegalArgumentException("Snapshot not found: " + snapshotId);
				}
				// Guardar en cache para futuros accesos
				metadataCache.put(snapshotId, metadata);
				logger.debug("Loaded metadata for snapshot {} from SnapshotStore and cached", snapshotId);
			}
			
			// Construir path: {basePath}/{NETWORK}/snapshots/snapshot_{id}/snapshot.log
			String snapshotDir = PathUtils.getSnapshotPath(basePath, metadata);
			return Paths.get(snapshotDir, LOG_FILENAME);
			
		} catch (IllegalArgumentException e) {
			throw e;
		} catch (Exception e) {
			logger.error("Error loading metadata for snapshot {}: {}", snapshotId, e.getMessage(), e);
			throw new IllegalArgumentException("Cannot load snapshot metadata: " + snapshotId, e);
		}
	}
	
	/**
	 * Adds a log entry for a specific snapshot.
	 * Appends the message to the snapshot.log file with timestamp.
	 * Message will be sanitized by removing any line breaks so each stored entry
	 * remains on a single line in the log file.
	 *
	 * @param snapshotId the ID of the snapshot
	 * @param message the log message to add
	 */
	public synchronized void addEntry(Long snapshotId, String message) {
		if (snapshotId == null || message == null) {
			return;
		}
		
		try {
			Path logFile = getLogFilePath(snapshotId);
			
			// Crear directorio si no existe
			Files.createDirectories(logFile.getParent());
			
			// Formatear mensaje con timestamp
			String timestamp = LocalDateTime.now().format(TIMESTAMP_FORMAT);
			// Sanitizar el mensaje eliminando saltos de línea para garantizar
			// que cada entrada ocupe una sola línea en el archivo de log.
			String safeMessage = message.replaceAll("[\\r\\n]+", " ").trim();
			String logEntry = String.format("[%s] %s%n", timestamp, safeMessage);
			
			// Append al archivo (crea si no existe)
			Files.writeString(logFile, logEntry, StandardCharsets.UTF_8, 
			                  StandardOpenOption.CREATE, 
			                  StandardOpenOption.APPEND);
			
		} catch (IOException e) {
			logger.error("Error writing log for snapshot {}: {}", snapshotId, e.getMessage(), e);
		} catch (IllegalArgumentException | IllegalStateException e) {
			logger.error("Error getting log path for snapshot {}: {}", snapshotId, e.getMessage());
		}
	}
	
	/**
	 * Deletes the log file associated with a specific snapshot.
	 *
	 * @param snapshotId the ID of the snapshot whose logs should be deleted
	 */
	public synchronized void deleteSnapshotLog(Long snapshotId) {
		if (snapshotId == null) {
			return;
		}
		
		try {
			Path logFile = getLogFilePath(snapshotId);
			
			if (Files.exists(logFile)) {
				Files.delete(logFile);
			}
			
		} catch (IOException e) {
			logger.error("Error deleting log for snapshot {}: {}", snapshotId, e.getMessage(), e);
		} catch (IllegalArgumentException | IllegalStateException e) {
			logger.error("Error getting log path for snapshot {}: {}", snapshotId, e.getMessage());
		}
	}

	/**
	 * Retrieves log entries for a specific snapshot with pagination.
	 * 
	 * @param snapshotId the snapshot ID
	 * @param page page number (0-indexed)
	 * @param size page size
	 * @return LogQueryResult with paginated entries
	 */
	public LogQueryResult getLogEntries(Long snapshotId, int page, int size) {
		LogQueryResult result = new LogQueryResult();
		
		if (snapshotId == null) {
			logger.warn("Invalid snapshot ID: null");
			return result;
		}
		
		try {
			Path logFile = getLogFilePath(snapshotId);
			logger.debug("Reading log file: {}", logFile);
			
			// Leer todas las líneas del archivo
			List<LogEntry> allEntries = new ArrayList<>();
			if (Files.exists(logFile)) {
				try (Stream<String> lines = Files.lines(logFile)) {
					lines.forEach(line -> {
						LogEntry entry = parseLogLine(line);
						if (entry != null) {
							allEntries.add(entry);
						}
					});
				}
			}
			
			// Ordenar por timestamp descendente (últimas primero)
			allEntries.sort((a, b) -> b.timestamp.compareTo(a.timestamp));
			
			// Aplicar paginación
			int totalElements = allEntries.size();
			int fromIndex = Math.min(page * size, totalElements);
			int toIndex = Math.min(fromIndex + size, totalElements);
			
			List<LogEntry> pageContent = allEntries.subList(fromIndex, toIndex);
			
			result.setEntries(pageContent);
			result.setPageInfo(page, size, totalElements);
			result.setSuccess(true);
			
			logger.debug("Retrieved {} log entries for snapshot {} (page={}, size={})", 
				pageContent.size(), snapshotId, page, size);
			
		} catch (IOException e) {
			logger.error("Error reading log file for snapshot {}: {}", snapshotId, e.getMessage(), e);
			result.setError("Error reading log file: " + e.getMessage());
		} catch (Exception e) {
			logger.error("Unexpected error retrieving logs for snapshot {}: {}", snapshotId, e.getMessage(), e);
			result.setError("Unexpected error: " + e.getMessage());
		}
		
		return result;
	}
	
	/**
	 * Parses a log line in format: [timestamp] message
	 * 
	 * @param line the log line to parse
	 * @return LogEntry if parsing successful, null otherwise
	 */
	private LogEntry parseLogLine(String line) {
		try {
			if (line == null || line.trim().isEmpty()) {
				return null;
			}
			
			// Formato esperado: [2025-11-12 12:45:30.123] message
			int endBracketIndex = line.indexOf(']');
			if (endBracketIndex <= 0) {
				// Si no tiene formato, usar la línea completa
				return new LogEntry(line, line);
			}
			
			String timestamp = line.substring(1, endBracketIndex).trim();
			String message = line.substring(endBracketIndex + 1).trim();
			
			return new LogEntry(timestamp, message);
		} catch (Exception e) {
			logger.warn("Error parsing log line: {}", line, e);
			return null;
		}
	}

	/**
	 * Clase interna para representar una entrada de log.
	 */
	public static class LogEntry {
		private String timestamp;
		private String message;
		
		public LogEntry(String timestamp, String message) {
			this.timestamp = timestamp;
			this.message = message;
		}
		
		public LogEntry() {}
		
		public String getTimestamp() { return timestamp; }
		public void setTimestamp(String timestamp) { this.timestamp = timestamp; }
		
		public String getMessage() { return message; }
		public void setMessage(String message) { this.message = message; }
	}
	
	/**
	 * Resultado de una consulta de logs con paginación.
	 */
	public static class LogQueryResult {
		private List<LogEntry> entries;
		private int currentPage;
		private int pageSize;
		private int totalElements;
		private int totalPages;
		private boolean success;
		private String error;
		
		public LogQueryResult() {
			this.entries = new ArrayList<>();
			this.success = true;
		}
		
		public void setEntries(List<LogEntry> entries) {
			this.entries = entries != null ? entries : new ArrayList<>();
		}
		
		public void setPageInfo(int currentPage, int pageSize, int totalElements) {
			this.currentPage = currentPage;
			this.pageSize = pageSize;
			this.totalElements = totalElements;
			this.totalPages = (totalElements + pageSize - 1) / pageSize;
		}
		
		public void setSuccess(boolean success) {
			this.success = success;
		}
		
		public void setError(String error) {
			this.error = error;
			this.success = false;
		}
		
		// Getters
		public List<LogEntry> getEntries() { return entries; }
		public int getCurrentPage() { return currentPage; }
		public int getPageSize() { return pageSize; }
		public int getTotalElements() { return totalElements; }
		public int getTotalPages() { return totalPages; }
		public boolean isSuccess() { return success; }
		public String getError() { return error; }
	}

}
