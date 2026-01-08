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
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Repository;

import javax.sql.DataSource;
import java.io.IOException;
import java.sql.*;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * JDBC repository for validation records in SQLite.
 * 
 * OPERATIONS:
 * - insertBatch(): Batch insert with dynamic columns
 * - streamAll(): Stream all records for iteration
 * - queryByRule(): Query by specific rule validity
 * - count(), countValid(), countTransformed(): Aggregation queries
 * 
 * DYNAMIC SCHEMA:
 * - Columns rule_<id> are created based on validator rules
 * - INSERT statements are generated dynamically per snapshot
 */
@Repository
public class RecordValidationRepository {

    private static final Logger logger = LogManager.getLogger(RecordValidationRepository.class);

    private static final DateTimeFormatter ISO_FORMATTER = DateTimeFormatter.ISO_LOCAL_DATE_TIME;

    @Value("${validation.batch.size:1000}")
    private int batchSize;

    @Autowired
    private ValidationDatabaseManager dbManager;

    // Cached PreparedStatement SQL per snapshot (dynamic based on rules)
    private final Map<Long, String> insertSqlCache = new HashMap<>();
    private final Map<Long, List<Long>> ruleIdsCache = new HashMap<>();

    /**
     * Registers the rule IDs for a snapshot (needed for dynamic column binding).
     */
    public void registerRuleIds(Long snapshotId, List<Long> ruleIds) {
        ruleIdsCache.put(snapshotId, new ArrayList<>(ruleIds));

        // Build dynamic INSERT SQL
        StringBuilder columns = new StringBuilder(
                "INSERT INTO record_validation (identifier_hash, identifier, datestamp, is_valid, is_transformed, published_metadata_hash");
        StringBuilder placeholders = new StringBuilder("VALUES (?, ?, ?, ?, ?, ?");

        for (Long ruleId : ruleIds) {
            columns.append(", rule_").append(ruleId);
            placeholders.append(", ?");
        }
        columns.append(")");
        placeholders.append(")");

        String sql = columns + " " + placeholders;
        insertSqlCache.put(snapshotId, sql);

        logger.debug("VALIDATION REPO: Registered {} rules for snapshot {}", ruleIds.size(), snapshotId);
    }

    /**
     * Inserts a batch of validation records.
     * 
     * @param snapshotId Snapshot ID
     * @param records    List of validation record data
     * @throws IOException if insert fails
     */
    public void insertBatch(Long snapshotId, List<ValidationRecord> records) throws IOException {
        if (records == null || records.isEmpty()) {
            return;
        }

        DataSource ds = dbManager.getDataSource(snapshotId);
        if (ds == null) {
            throw new IOException("Snapshot " + snapshotId + " not initialized");
        }

        String sql = insertSqlCache.get(snapshotId);
        List<Long> ruleIds = ruleIdsCache.get(snapshotId);

        if (sql == null || ruleIds == null) {
            throw new IOException("Rule IDs not registered for snapshot " + snapshotId);
        }

        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);

            try (PreparedStatement stmt = conn.prepareStatement(sql)) {
                int count = 0;
                for (ValidationRecord record : records) {
                    setRecordParameters(stmt, record, ruleIds);
                    stmt.addBatch();
                    count++;

                    if (count % batchSize == 0) {
                        stmt.executeBatch();
                        logger.debug("VALIDATION REPO: Executed batch of {} records", batchSize);
                    }
                }

                // Execute remaining
                if (count % batchSize != 0) {
                    stmt.executeBatch();
                }

                conn.commit();
                logger.debug("VALIDATION REPO: Committed {} records", count);

            } catch (SQLException e) {
                conn.rollback();
                throw e;
            }

        } catch (SQLException e) {
            throw new IOException("Failed to insert validation records: " + e.getMessage(), e);
        }
    }

    /**
     * Inserts a single validation record.
     */
    public void insert(Long snapshotId, ValidationRecord record) throws IOException {
        insertBatch(snapshotId, Collections.singletonList(record));
    }

    /**
     * Streams all validation records.
     */
    public Stream<ValidationRecord> streamAll(Long snapshotId) throws IOException {
        DataSource ds = dbManager.getDataSource(snapshotId);
        if (ds == null) {
            return Stream.empty();
        }

        List<Long> ruleIds = ruleIdsCache.getOrDefault(snapshotId, Collections.emptyList());
        String sql = "SELECT * FROM record_validation";

        try {
            Connection conn = ds.getConnection();
            PreparedStatement stmt = conn.prepareStatement(sql);
            ResultSet rs = stmt.executeQuery();

            Iterator<ValidationRecord> iterator = new ResultSetIterator(rs, stmt, conn, ruleIds);
            Spliterator<ValidationRecord> spliterator = Spliterators.spliteratorUnknownSize(
                    iterator, Spliterator.ORDERED | Spliterator.NONNULL);

            return StreamSupport.stream(spliterator, false)
                    .onClose(() -> closeResources(rs, stmt, conn));

        } catch (SQLException e) {
            throw new IOException("Failed to stream validation records: " + e.getMessage(), e);
        }
    }

    /**
     * Counts total records.
     */
    public long count(Long snapshotId) {
        return executeCount(snapshotId, "SELECT COUNT(*) FROM record_validation");
    }

    /**
     * Counts valid records.
     */
    public long countValid(Long snapshotId) {
        return executeCount(snapshotId, "SELECT COUNT(*) FROM record_validation WHERE is_valid = 1");
    }

    /**
     * Counts transformed records.
     */
    public long countTransformed(Long snapshotId) {
        return executeCount(snapshotId, "SELECT COUNT(*) FROM record_validation WHERE is_transformed = 1");
    }

    /**
     * Counts records that fail a specific rule.
     */
    public long countInvalidByRule(Long snapshotId, Long ruleId) {
        String sql = "SELECT COUNT(*) FROM record_validation WHERE rule_" + ruleId + " = 0";
        return executeCount(snapshotId, sql);
    }

    /**
     * Counts records that pass a specific rule.
     */
    public long countValidByRule(Long snapshotId, Long ruleId) {
        String sql = "SELECT COUNT(*) FROM record_validation WHERE rule_" + ruleId + " = 1";
        return executeCount(snapshotId, sql);
    }

    /**
     * Queries records by rule validity with pagination.
     */
    public List<ValidationRecord> queryByRule(Long snapshotId, Long ruleId, boolean isValid, int offset, int limit)
            throws IOException {
        DataSource ds = dbManager.getDataSource(snapshotId);
        if (ds == null) {
            return Collections.emptyList();
        }

        List<Long> ruleIds = ruleIdsCache.get(snapshotId);
        String sql = "SELECT * FROM record_validation WHERE rule_" + ruleId
                + " = ? ORDER BY identifier LIMIT ? OFFSET ?";

        List<ValidationRecord> results = new ArrayList<>();

        try (Connection conn = ds.getConnection();
                PreparedStatement stmt = conn.prepareStatement(sql)) {

            stmt.setInt(1, isValid ? 1 : 0);
            stmt.setInt(2, limit);
            stmt.setInt(3, offset);

            try (ResultSet rs = stmt.executeQuery()) {
                while (rs.next()) {
                    results.add(mapRowToRecord(rs, ruleIds));
                }
            }

        } catch (SQLException e) {
            throw new IOException("Failed to query by rule: " + e.getMessage(), e);
        }

        return results;
    }

    /**
     * Gets a single record by identifier.
     */
    public ValidationRecord getByIdentifier(Long snapshotId, String identifier) throws IOException {
        DataSource ds = dbManager.getDataSource(snapshotId);
        if (ds == null) {
            return null;
        }

        List<Long> ruleIds = ruleIdsCache.get(snapshotId);
        String sql = "SELECT * FROM record_validation WHERE identifier = ?";

        try (Connection conn = ds.getConnection();
                PreparedStatement stmt = conn.prepareStatement(sql)) {

            stmt.setString(1, identifier);

            try (ResultSet rs = stmt.executeQuery()) {
                if (rs.next()) {
                    return mapRowToRecord(rs, ruleIds);
                }
            }

        } catch (SQLException e) {
            throw new IOException("Failed to get by identifier: " + e.getMessage(), e);
        }

        return null;
    }

    /**
     * Queries records with pagination and optional filters.
     */
    /**
     * Helper class for filter processing
     */

    private void applyFilters(StringBuilder sql, List<String> filters, List<Object> params) {
        if (filters == null || filters.isEmpty()) {
            return;
        }

        List<String> conditions = new ArrayList<>();

        for (String filter : filters) {
            if (filter == null)
                continue;

            String field = null;
            String value = null;

            // Support both @@ and : separators
            if (filter.contains("@@")) {
                String[] parts = filter.split("@@", 2);
                field = parts[0].trim();
                value = parts[1].trim();
            } else if (filter.contains(":")) {
                String[] parts = filter.split(":", 2);
                field = parts[0].trim();
                value = parts[1].trim();
            } else {
                continue;
            }

            // Remove quotes if present
            value = value.replace("\"", "");

            switch (field) {
                case "identifier":
                    conditions.add("identifier LIKE ?");
                    params.add("%" + value + "%"); // Partial match typical for search
                    break;
                case "is_valid":
                case "record_is_valid":
                    conditions.add("is_valid = ?");
                    params.add("true".equalsIgnoreCase(value) ? 1 : 0);
                    break;
                case "is_transformed":
                case "record_is_transformed":
                    conditions.add("is_transformed = ?");
                    params.add("true".equalsIgnoreCase(value) ? 1 : 0);
                    break;
                case "valid_rules":
                    conditions.add("rule_" + value + " = 1");
                    // No param needed for column selection, assuming rule ID is safe (numeric)
                    break;
                case "invalid_rules":
                    conditions.add("rule_" + value + " = 0");
                    break;
            }
        }

        if (!conditions.isEmpty()) {
            if (sql.toString().toUpperCase().contains(" WHERE ")) {
                sql.append(" AND ");
            } else {
                sql.append(" WHERE ");
            }
            sql.append(String.join(" AND ", conditions));
        }
    }

    public List<ValidationRecord> queryWithPagination(Long snapshotId, List<String> filters, int offset, int limit)
            throws IOException {
        DataSource ds = dbManager.getDataSource(snapshotId);
        if (ds == null) {
            return Collections.emptyList();
        }

        List<Long> ruleIds = ruleIdsCache.get(snapshotId);
        StringBuilder sql = new StringBuilder("SELECT * FROM record_validation");
        List<Object> params = new ArrayList<>();

        applyFilters(sql, filters, params);

        sql.append(" ORDER BY identifier LIMIT ? OFFSET ?");
        params.add(limit);
        params.add(offset);

        List<ValidationRecord> results = new ArrayList<>();

        try (Connection conn = ds.getConnection();
                PreparedStatement stmt = conn.prepareStatement(sql.toString())) {

            for (int i = 0; i < params.size(); i++) {
                stmt.setObject(i + 1, params.get(i));
            }

            try (ResultSet rs = stmt.executeQuery()) {
                while (rs.next()) {
                    results.add(mapRowToRecord(rs, ruleIds));
                }
            }

        } catch (SQLException e) {
            throw new IOException("Failed to query with pagination: " + e.getMessage(), e);
        }

        return results;
    }

    public long countWithFilters(Long snapshotId, List<String> filters) {
        StringBuilder sql = new StringBuilder("SELECT COUNT(*) FROM record_validation");
        List<Object> params = new ArrayList<>();

        applyFilters(sql, filters, params);

        return executeCountWithParams(snapshotId, sql.toString(), params);
    }

    public long countValidWithFilters(Long snapshotId, List<String> filters) {
        StringBuilder sql = new StringBuilder("SELECT COUNT(*) FROM record_validation WHERE is_valid = 1");
        List<Object> params = new ArrayList<>();
        applyFilters(sql, filters, params);
        return executeCountWithParams(snapshotId, sql.toString(), params);
    }

    public long countTransformedWithFilters(Long snapshotId, List<String> filters) {
        StringBuilder sql = new StringBuilder("SELECT COUNT(*) FROM record_validation WHERE is_transformed = 1");
        List<Object> params = new ArrayList<>();
        applyFilters(sql, filters, params);
        return executeCountWithParams(snapshotId, sql.toString(), params);
    }

    public long countRuleWithFilters(Long snapshotId, Long ruleId, boolean isValid, List<String> filters) {
        StringBuilder sql = new StringBuilder("SELECT COUNT(*) FROM record_validation WHERE rule_")
                .append(ruleId)
                .append(" = ")
                .append(isValid ? "1" : "0");
        List<Object> params = new ArrayList<>();
        applyFilters(sql, filters, params);
        return executeCountWithParams(snapshotId, sql.toString(), params);
    }

    // DTO for aggregated statistics
    @Data
    @NoArgsConstructor
    public static class AggregatedStats {
        private long totalRecords;
        private long validRecords;
        private long transformedRecords;
        private Map<Long, Long> validRuleCounts = new HashMap<>();
        private Map<Long, Long> invalidRuleCounts = new HashMap<>();
    }

    public AggregatedStats getAggregatedStats(Long snapshotId, List<String> filters, List<Long> ruleIds) {
        AggregatedStats stats = new AggregatedStats();

        StringBuilder sql = new StringBuilder("SELECT COUNT(*) as total, ");
        sql.append("SUM(is_valid) as valid_count, ");
        sql.append("SUM(is_transformed) as transformed_count");

        for (Long ruleId : ruleIds) {
            sql.append(", SUM(rule_").append(ruleId).append(") as rule_").append(ruleId).append("_valid");
            sql.append(", SUM(CASE WHEN rule_").append(ruleId).append(" = 0 THEN 1 ELSE 0 END) as rule_").append(ruleId)
                    .append("_invalid");
        }

        sql.append(" FROM record_validation");
        List<Object> params = new ArrayList<>();

        applyFilters(sql, filters, params);

        DataSource ds = dbManager.getDataSource(snapshotId);
        try (Connection conn = ds.getConnection();
                PreparedStatement stmt = conn.prepareStatement(sql.toString())) {

            for (int i = 0; i < params.size(); i++) {
                stmt.setObject(i + 1, params.get(i));
            }

            try (ResultSet rs = stmt.executeQuery()) {
                if (rs.next()) {
                    stats.setTotalRecords(rs.getLong("total"));
                    stats.setValidRecords(rs.getLong("valid_count"));
                    stats.setTransformedRecords(rs.getLong("transformed_count"));

                    for (Long ruleId : ruleIds) {
                        long valid = rs.getLong("rule_" + ruleId + "_valid");
                        if (valid > 0) {
                            stats.getValidRuleCounts().put(ruleId, valid);
                        }

                        long invalid = rs.getLong("rule_" + ruleId + "_invalid");
                        if (invalid > 0) {
                            stats.getInvalidRuleCounts().put(ruleId, invalid);
                        }
                    }
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException("Error calculating aggregated stats", e);
        }

        return stats;
    }

    // ========================================
    // PRIVATE HELPERS
    // ========================================

    private void setRecordParameters(PreparedStatement stmt, ValidationRecord record, List<Long> ruleIds)
            throws SQLException {
        int idx = 1;
        stmt.setString(idx++, record.getIdentifierHash());
        stmt.setString(idx++, record.getIdentifier());
        stmt.setString(idx++, record.getDatestamp() != null ? record.getDatestamp().format(ISO_FORMATTER) : null);
        stmt.setInt(idx++, record.isValid() ? 1 : 0);
        stmt.setInt(idx++, record.isTransformed() ? 1 : 0);
        stmt.setString(idx++, record.getPublishedMetadataHash());

        // Set rule columns
        for (Long ruleId : ruleIds) {
            Boolean ruleResult = record.getRuleResults().get(ruleId);
            stmt.setInt(idx++, ruleResult != null && ruleResult ? 1 : 0); // Handle null as false/0
        }
    }

    private ValidationRecord mapRowToRecord(ResultSet rs, List<Long> ruleIds) throws SQLException {
        ValidationRecord record = new ValidationRecord();
        record.setIdentifierHash(rs.getString("identifier_hash"));
        record.setIdentifier(rs.getString("identifier"));

        String datestampStr = rs.getString("datestamp");
        if (datestampStr != null) {
            record.setDatestamp(LocalDateTime.parse(datestampStr, ISO_FORMATTER));
        }

        record.setValid(rs.getInt("is_valid") == 1);
        record.setTransformed(rs.getInt("is_transformed") == 1);
        record.setPublishedMetadataHash(rs.getString("published_metadata_hash"));

        // Map rule columns
        Map<Long, Boolean> ruleResults = new HashMap<>();
        for (Long ruleId : ruleIds) {
            int value = rs.getInt("rule_" + ruleId);
            ruleResults.put(ruleId, value == 1);
        }
        record.setRuleResults(ruleResults);

        return record;
    }

    private long executeCount(Long snapshotId, String sql) {
        return executeCountWithParams(snapshotId, sql, Collections.emptyList());
    }

    private long executeCountWithParams(Long snapshotId, String sql, List<Object> params) {
        DataSource ds = dbManager.getDataSource(snapshotId);
        if (ds == null) {
            return 0;
        }

        try (Connection conn = ds.getConnection();
                PreparedStatement stmt = conn.prepareStatement(sql)) {

            for (int i = 0; i < params.size(); i++) {
                stmt.setObject(i + 1, params.get(i));
            }

            try (ResultSet rs = stmt.executeQuery()) {
                if (rs.next()) {
                    return rs.getLong(1);
                }
            }
            return 0;

        } catch (SQLException e) {
            logger.error("Error executing count: {}", e.getMessage(), e);
            return 0;
        }
    }

    private void closeResources(ResultSet rs, PreparedStatement stmt, Connection conn) {
        try {
            if (rs != null)
                rs.close();
        } catch (Exception ignored) {
        }
        try {
            if (stmt != null)
                stmt.close();
        } catch (Exception ignored) {
        }
        try {
            if (conn != null)
                conn.close();
        } catch (Exception ignored) {
        }
    }

    /**
     * Iterator wrapper for ResultSet.
     */
    private class ResultSetIterator implements Iterator<ValidationRecord> {
        private final ResultSet rs;
        private final PreparedStatement stmt;
        private final Connection conn;
        private final List<Long> ruleIds;
        private ValidationRecord next = null;
        private boolean hasNext = false;
        private boolean done = false;

        ResultSetIterator(ResultSet rs, PreparedStatement stmt, Connection conn, List<Long> ruleIds) {
            this.rs = rs;
            this.stmt = stmt;
            this.conn = conn;
            this.ruleIds = ruleIds;
        }

        @Override
        public boolean hasNext() {
            if (done)
                return false;
            if (hasNext)
                return true;

            try {
                if (rs.next()) {
                    next = mapRowToRecord(rs, ruleIds);
                    hasNext = true;
                    return true;
                } else {
                    done = true;
                    closeResources(rs, stmt, conn);
                    return false;
                }
            } catch (SQLException e) {
                done = true;
                closeResources(rs, stmt, conn);
                throw new RuntimeException("Error reading records", e);
            }
        }

        @Override
        public ValidationRecord next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            hasNext = false;
            return next;
        }
    }

    /**
     * Clears cached data for a snapshot.
     */
    public void clearCache(Long snapshotId) {
        insertSqlCache.remove(snapshotId);
        ruleIdsCache.remove(snapshotId);
    }
}
