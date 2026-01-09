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

package org.lareferencia.core.repository.validation;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.lareferencia.core.metadata.SnapshotMetadata;
import org.lareferencia.core.worker.IPaginator;
import org.lareferencia.core.worker.PaginatorException;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageImpl;
import org.springframework.data.domain.PageRequest;

import javax.sql.DataSource;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Paginator for validation records backed by SQLite.
 * 
 * Features:
 * - Lazy initialization: opens database on first page request
 * - OFFSET/LIMIT pagination native to SQLite
 * - Only reads basic fields needed for indexing (no rule results)
 * 
 * Usage:
 * 
 * <pre>
 * ValidationRecordPaginator paginator = new ValidationRecordPaginator(
 *         snapshotMetadata, dbManager);
 * paginator.setPageSize(1000);
 * worker.setPaginator(paginator);
 * </pre>
 */
public class ValidationRecordPaginator implements IPaginator<ValidationRecord> {

    private static final Logger logger = LogManager.getLogger(ValidationRecordPaginator.class);
    private static final DateTimeFormatter ISO_FORMATTER = DateTimeFormatter.ISO_LOCAL_DATE_TIME;

    private final SnapshotMetadata snapshotMetadata;
    private final ValidationDatabaseManager dbManager;

    private int pageSize = 1000;
    private int currentPage = 0;
    private int totalPages = 0;
    private long totalCount = 0;
    private boolean initialized = false;

    /**
     * Creates a paginator for validation records.
     * 
     * @param snapshotMetadata Metadata of the snapshot to iterate
     * @param dbManager        Database manager for SQLite connections
     */
    public ValidationRecordPaginator(SnapshotMetadata snapshotMetadata,
            ValidationDatabaseManager dbManager) {
        this.snapshotMetadata = snapshotMetadata;
        this.dbManager = dbManager;
    }

    /**
     * Lazy initialization: opens database and calculates total pages.
     */
    private void ensureInitialized() {
        if (initialized) {
            return;
        }

        Long snapshotId = snapshotMetadata.getSnapshotId();
        logger.info("VALIDATION PAGINATOR: Initializing for snapshot {}", snapshotId);

        try {
            // Open database for reading (if not already open)
            dbManager.openSnapshotForRead(snapshotMetadata);

            // Count total records
            totalCount = executeCount();
            totalPages = (int) Math.ceil((double) totalCount / pageSize);

            logger.info("VALIDATION PAGINATOR: Found {} records, {} pages of size {}",
                    totalCount, totalPages, pageSize);

            initialized = true;

        } catch (IOException e) {
            throw new PaginatorException("Failed to initialize validation paginator: " + e.getMessage(), e);
        }
    }

    @Override
    public int getStartingPage() {
        return 1; // Pages are 1-indexed for BaseBatchWorker
    }

    @Override
    public int getTotalPages() {
        ensureInitialized();
        return totalPages;
    }

    @Override
    public Page<ValidationRecord> nextPage() {
        ensureInitialized();

        List<ValidationRecord> records = queryPage(currentPage, pageSize);
        Page<ValidationRecord> page = new PageImpl<>(
                records,
                PageRequest.of(currentPage, pageSize),
                totalCount);

        currentPage++;
        return page;
    }

    @Override
    public void setPageSize(int size) {
        if (initialized) {
            throw new IllegalStateException("Cannot change page size after initialization");
        }
        this.pageSize = size;
    }

    /**
     * Gets the page size.
     */
    public int getPageSize() {
        return pageSize;
    }

    /**
     * Counts total records in the validation table.
     */
    private long executeCount() {
        Long snapshotId = snapshotMetadata.getSnapshotId();
        DataSource ds = dbManager.getDataSource(snapshotId);

        if (ds == null) {
            logger.warn("VALIDATION PAGINATOR: No DataSource for snapshot {}", snapshotId);
            return 0;
        }

        String sql = "SELECT COUNT(*) FROM record_validation";

        try (Connection conn = ds.getConnection();
                PreparedStatement stmt = conn.prepareStatement(sql);
                ResultSet rs = stmt.executeQuery()) {

            if (rs.next()) {
                return rs.getLong(1);
            }
            return 0;

        } catch (SQLException e) {
            logger.error("VALIDATION PAGINATOR: Error counting records: {}", e.getMessage());
            return 0;
        }
    }

    /**
     * Queries a page of validation records.
     * Only reads basic fields needed for indexing.
     */
    private List<ValidationRecord> queryPage(int page, int size) {
        Long snapshotId = snapshotMetadata.getSnapshotId();
        DataSource ds = dbManager.getDataSource(snapshotId);

        if (ds == null) {
            logger.warn("VALIDATION PAGINATOR: No DataSource for snapshot {}", snapshotId);
            return Collections.emptyList();
        }

        int offset = page * size;
        String sql = """
                SELECT identifier_hash, identifier, datestamp, is_valid,
                       is_transformed, published_metadata_hash
                FROM record_validation
                ORDER BY identifier_hash
                LIMIT ? OFFSET ?
                """;

        List<ValidationRecord> records = new ArrayList<>();

        try (Connection conn = ds.getConnection();
                PreparedStatement stmt = conn.prepareStatement(sql)) {

            stmt.setInt(1, size);
            stmt.setInt(2, offset);

            try (ResultSet rs = stmt.executeQuery()) {
                while (rs.next()) {
                    records.add(mapBasicRecord(rs));
                }
            }

            logger.debug("VALIDATION PAGINATOR: Read {} records for page {} (offset {})",
                    records.size(), page, offset);

        } catch (SQLException e) {
            logger.error("VALIDATION PAGINATOR: Error reading page {}: {}", page, e.getMessage());
        }

        return records;
    }

    /**
     * Maps a ResultSet row to ValidationRecord with only basic fields.
     * Rule results are not populated (not needed for indexing).
     */
    private ValidationRecord mapBasicRecord(ResultSet rs) throws SQLException {
        ValidationRecord record = new ValidationRecord();
        record.setIdentifierHash(rs.getString("identifier_hash"));
        record.setIdentifier(rs.getString("identifier"));

        String datestampStr = rs.getString("datestamp");
        if (datestampStr != null && !datestampStr.isEmpty()) {
            record.setDatestamp(LocalDateTime.parse(datestampStr, ISO_FORMATTER));
        }

        record.setValid(rs.getInt("is_valid") == 1);
        record.setTransformed(rs.getInt("is_transformed") == 1);
        record.setPublishedMetadataHash(rs.getString("published_metadata_hash"));

        // Rule results not populated - not needed for indexing
        record.setRuleResults(Collections.emptyMap());

        return record;
    }
}
