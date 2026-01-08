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
import java.util.*;

/**
 * JDBC repository for rule occurrences in SQLite.
 * 
 * Only used when detailedDiagnose=true.
 * Stores individual occurrence values for drill-down analysis.
 * 
 * OPERATIONS:
 * - insertBatch(): Batch insert occurrences
 * - countByRule(): Aggregate occurrence counts by value
 * - getOccurrencesByRecord(): Get all occurrences for a record
 */
@Repository
public class RuleOccurrenceRepository {

    private static final Logger logger = LogManager.getLogger(RuleOccurrenceRepository.class);

    private static final String INSERT_SQL = """
            INSERT INTO rule_occurrences (identifier_hash, rule_id, is_valid, occurrence_value)
            VALUES (?, ?, ?, ?)
            """;

    @Value("${validation.batch.size:1000}")
    private int batchSize;

    @Autowired
    private ValidationDatabaseManager dbManager;

    /**
     * Inserts a batch of rule occurrences.
     * 
     * @param snapshotId  Snapshot ID
     * @param occurrences List of occurrence data
     * @throws IOException if insert fails
     */
    public void insertBatch(Long snapshotId, List<RuleOccurrence> occurrences) throws IOException {
        if (occurrences == null || occurrences.isEmpty()) {
            return;
        }

        DataSource ds = dbManager.getDataSource(snapshotId);
        if (ds == null) {
            throw new IOException("Snapshot " + snapshotId + " not initialized");
        }

        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);

            try (PreparedStatement stmt = conn.prepareStatement(INSERT_SQL)) {
                int count = 0;
                for (RuleOccurrence occ : occurrences) {
                    stmt.setString(1, occ.getIdentifierHash());
                    stmt.setInt(2, occ.getRuleId().intValue());
                    stmt.setInt(3, occ.isValid() ? 1 : 0);
                    stmt.setString(4, occ.getOccurrenceValue());
                    stmt.addBatch();
                    count++;

                    if (count % batchSize == 0) {
                        stmt.executeBatch();
                    }
                }

                if (count % batchSize != 0) {
                    stmt.executeBatch();
                }

                conn.commit();
                logger.debug("OCCURRENCE REPO: Committed {} occurrences", count);

            } catch (SQLException e) {
                conn.rollback();
                throw e;
            }

        } catch (SQLException e) {
            throw new IOException("Failed to insert occurrences: " + e.getMessage(), e);
        }
    }

    /**
     * Counts occurrences by value for a specific rule.
     * Used for occurrence drill-down reports.
     * 
     * @param snapshotId Snapshot ID
     * @param ruleId     Rule ID
     * @param isValid    true for valid occurrences, false for invalid
     * @return Map of occurrenceValue -> count
     */
    public Map<String, Integer> countByRuleAndValidity(Long snapshotId, Long ruleId, boolean isValid)
            throws IOException {
        DataSource ds = dbManager.getDataSource(snapshotId);
        if (ds == null) {
            return Collections.emptyMap();
        }

        String sql = """
                SELECT occurrence_value, COUNT(*) as cnt
                FROM rule_occurrences
                WHERE rule_id = ? AND is_valid = ?
                GROUP BY occurrence_value
                ORDER BY cnt DESC
                """;

        Map<String, Integer> result = new LinkedHashMap<>();

        try (Connection conn = ds.getConnection();
                PreparedStatement stmt = conn.prepareStatement(sql)) {

            stmt.setInt(1, ruleId.intValue());
            stmt.setInt(2, isValid ? 1 : 0);

            try (ResultSet rs = stmt.executeQuery()) {
                while (rs.next()) {
                    String value = rs.getString("occurrence_value");
                    int count = rs.getInt("cnt");
                    result.put(value, count);
                }
            }

        } catch (SQLException e) {
            throw new IOException("Failed to count occurrences: " + e.getMessage(), e);
        }

        return result;
    }

    /**
     * Gets all occurrences for a specific rule.
     * Returns both valid and invalid occurrences aggregated.
     * 
     * @param snapshotId Snapshot ID
     * @param ruleId     Rule ID
     * @return Map with "valid" and "invalid" keys, each containing value->count map
     */
    public Map<String, Map<String, Integer>> getOccurrencesByRule(Long snapshotId, Long ruleId) throws IOException {
        Map<String, Map<String, Integer>> result = new HashMap<>();
        result.put("valid", countByRuleAndValidity(snapshotId, ruleId, true));
        result.put("invalid", countByRuleAndValidity(snapshotId, ruleId, false));
        return result;
    }

    /**
     * Gets all occurrences for a specific record.
     * 
     * @param snapshotId     Snapshot ID
     * @param identifierHash Record identifier hash
     * @return List of occurrences
     */
    public List<RuleOccurrence> getOccurrencesByRecord(Long snapshotId, String identifierHash) throws IOException {
        DataSource ds = dbManager.getDataSource(snapshotId);
        if (ds == null) {
            return Collections.emptyList();
        }

        String sql = "SELECT * FROM rule_occurrences WHERE identifier_hash = ?";
        List<RuleOccurrence> results = new ArrayList<>();

        try (Connection conn = ds.getConnection();
                PreparedStatement stmt = conn.prepareStatement(sql)) {

            stmt.setString(1, identifierHash);

            try (ResultSet rs = stmt.executeQuery()) {
                while (rs.next()) {
                    RuleOccurrence occ = new RuleOccurrence();
                    occ.setIdentifierHash(rs.getString("identifier_hash"));
                    occ.setRuleId((long) rs.getInt("rule_id"));
                    occ.setValid(rs.getInt("is_valid") == 1);
                    occ.setOccurrenceValue(rs.getString("occurrence_value"));
                    results.add(occ);
                }
            }

        } catch (SQLException e) {
            throw new IOException("Failed to get occurrences: " + e.getMessage(), e);
        }

        return results;
    }
}
