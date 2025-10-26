package com.example.cbprofileutils.repository;

import jakarta.persistence.PersistenceContext;
import jakarta.persistence.EntityManager;
import org.hibernate.Session;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

import java.sql.*;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Efficient batch upsert implementation for profile IDs.
 *
 * - Preserves order & dedupe.
 * - SELECT existing ids first.
 * - INSERT missing names in chunks using a single INSERT ... VALUES(...),(...)
 *   with placeholders and RETURNING id,name.
 * - Finally selects any remaining ids for rows that conflicted (defensive).
 */
@Repository
class PsqlProfileIdsUpsertRepoImpl implements PsqlProfileIdsUpsertRepo {
    @PersistenceContext
    private EntityManager em;

    // Maximum number of rows to include in a single multi-row INSERT statement.
    // Tune to your environment (500 - 5000). Too large → huge SQL; too small → more round-trips.
    private static final int INSERT_CHUNK = 1000;

    @Override
    @Transactional(transactionManager = "transactionManager")
    public Long upsertAndReturnId(String name, Long profileTypeId, Long parentId) {
        final String sql = """
                INSERT INTO profile (name, profile_type_id, parent_id)
                VALUES (:name, :type, :parent)
                ON CONFLICT (name, profile_type_id)
                DO UPDATE SET name = EXCLUDED.name
                RETURNING id
                """;
        Object r = em.createNativeQuery(sql)
                .setParameter("name", name)
                .setParameter("type", profileTypeId)
                .setParameter("parent", parentId)
                .getSingleResult();
        return ((Number) r).longValue();
    }

    @Override
    @Transactional(transactionManager = "transactionManager")
    public Map<String, Long> batchUpsertAndReturnIds(Collection<String> names, Long profileTypeId) {
        if (names == null || names.isEmpty()) return Collections.emptyMap();

        // 1) Preserve input order and dedupe
        List<String> distinct = new ArrayList<>(new LinkedHashSet<>(names));
        Map<String, Long> result = new HashMap<>(distinct.size());

        // Use low-level JDBC via Hibernate Session.doWork for robust array & RETURNING handling
        em.unwrap(Session.class).doWork(connection -> {
            // 2) SELECT existing rows using SQL array (one round-trip)
            String selectSql = "SELECT id, name FROM profile WHERE name = ANY(?)";
            try (PreparedStatement selectPs = connection.prepareStatement(selectSql)) {
                Array sqlArray = connection.createArrayOf("text", distinct.toArray(new String[0]));
                selectPs.setArray(1, sqlArray);
                try (ResultSet rs = selectPs.executeQuery()) {
                    while (rs.next()) {
                        long id = rs.getLong("id");
                        String name = rs.getString("name");
                        result.put(name, id);
                    }
                } finally {
                    if (sqlArray != null) try { sqlArray.free(); } catch (Exception ignored) {}
                }
            }

            // 3) Determine which names still need to be created
            List<String> toCreate = distinct.stream().filter(n -> !result.containsKey(n)).collect(Collectors.toList());

            // 4) Insert missing names in chunks (single INSERT statement per chunk with many placeholders)
            for (int start = 0; start < toCreate.size(); start += INSERT_CHUNK) {
                int end = Math.min(start + INSERT_CHUNK, toCreate.size());
                List<String> chunk = toCreate.subList(start, end);

                if (chunk.isEmpty()) continue;

                // Build placeholders: "(?, ?, ?), (?, ?, ?), ..."
                StringBuilder sb = new StringBuilder();
                sb.append("INSERT INTO profile (name, profile_type_id, parent_id) VALUES ");
                for (int i = 0; i < chunk.size(); i++) {
                    if (i > 0) sb.append(", ");
                    sb.append("(?, ?, ?)");
                }
                sb.append(" ON CONFLICT (name, profile_type_id) DO NOTHING RETURNING id, name");

                String insertSql = sb.toString();
                try (PreparedStatement insertPs = connection.prepareStatement(insertSql)) {
                    int idx = 1;
                    for (String nm : chunk) {
                        insertPs.setString(idx++, nm);
                        insertPs.setLong(idx++, profileTypeId);
                        insertPs.setNull(idx++, Types.BIGINT);
                    }

                    // executeQuery will return the RETURNING rows for newly inserted records
                    try (ResultSet rs = insertPs.executeQuery()) {
                        while (rs.next()) {
                            long id = rs.getLong("id");
                            String name = rs.getString("name");
                            result.put(name, id);
                        }
                    }
                }
            }

            // 5) Defensive: if any names are still missing (shouldn't happen), SELECT them
            if (result.size() < distinct.size()) {
                List<String> missing = distinct.stream().filter(n -> !result.containsKey(n)).collect(Collectors.toList());
                if (!missing.isEmpty()) {
                    String selectSql2 = "SELECT id, name FROM profile WHERE name = ANY(?)";
                    try (PreparedStatement ps2 = connection.prepareStatement(selectSql2)) {
                        Array arr = connection.createArrayOf("text", missing.toArray());
                        ps2.setArray(1, arr);
                        try (ResultSet rs = ps2.executeQuery()) {
                            while (rs.next()) {
                                long id = rs.getLong("id");
                                String name = rs.getString("name");
                                result.put(name, id);
                            }
                        } finally {
                            if (arr != null) try { arr.free(); } catch (Exception ignored) {}
                        }
                    }
                }
            }
        });

        // 6) Final sanity check
        for (String n : distinct) {
            if (!result.containsKey(n)) {
                throw new IllegalStateException("No id found for name after upsert/select: " + n);
            }
        }

        // Return mapping for distinct names
        return result;
    }
}
