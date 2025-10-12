package com.example.cbprofileutils.mapping;

import com.couchbase.client.java.json.JsonObject;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.*;

public final class CompiledMapping {

    public static final class FieldSpec {
        public final String biHeader;
        public final String[] pathTokens;
        public final Type type;
        public final DateTimeFormatter inFmt;
        public final DateTimeFormatter outFmt;

        public enum Type { STRING, NUMBER, BOOLEAN, DATE }

        public FieldSpec(String biHeader, String targetPath, String type, String inFmt, String outFmt) {
            this.biHeader = Objects.requireNonNull(biHeader, "biHeader");
            this.pathTokens = Objects.requireNonNull(targetPath, "targetPath").split("\\.");
            this.type = switch (type == null ? "string" : type.toLowerCase()) {
                case "number" -> Type.NUMBER;
                case "boolean" -> Type.BOOLEAN;
                case "date" -> Type.DATE;
                default -> Type.STRING;
            };
            this.inFmt = (inFmt != null && !inFmt.isBlank()) ? DateTimeFormatter.ofPattern(inFmt) : null;
            this.outFmt = (outFmt != null && !outFmt.isBlank()) ? DateTimeFormatter.ofPattern(outFmt) : null;
        }
    }

    public final String idField;
    public final String profileKeyPrefix;
    public final String msisdnKeyPrefix;
    public final String rawSavePath; // optional
    public final List<FieldSpec> fields;

    public CompiledMapping(String idField, String profileKeyPrefix, String msisdnKeyPrefix,
                           String rawSavePath, List<FieldSpec> fields) {
        this.idField = Objects.requireNonNull(idField);
        this.profileKeyPrefix = Objects.requireNonNull(profileKeyPrefix);
        this.msisdnKeyPrefix = Objects.requireNonNull(msisdnKeyPrefix);
        this.rawSavePath = rawSavePath;
        this.fields = List.copyOf(fields);
    }

    /** Build a new profile doc from a base template by applying fields. */
    public JsonObject apply(Map<String,String> biRow, JsonObject baseProfileTemplate, boolean saveRaw) {
        JsonObject profile = deepCopy(baseProfileTemplate);
        for (FieldSpec f : fields) {
            String raw = biRow.get(f.biHeader);
            if (raw == null || raw.isEmpty()) continue;
            Object value = convert(f, raw);
            if (value != null) put(profile, f.pathTokens, value);
        }
        if (saveRaw && rawSavePath != null) {
            JsonObject raw = JsonObject.create();
            biRow.forEach(raw::put);
            put(profile, rawSavePath.split("\\."), raw);
        }
        return profile;
    }

    public static Object convert(FieldSpec f, String raw) {
        try {
            return switch (f.type) {
                case NUMBER -> raw.contains(".") ? Double.parseDouble(raw) : Long.parseLong(raw);
                case BOOLEAN -> "true".equalsIgnoreCase(raw) || "1".equals(raw) || "y".equalsIgnoreCase(raw);
                case DATE -> {
                    if (f.inFmt == null || f.outFmt == null) yield raw;
                    String formatted = f.outFmt.format(LocalDate.parse(raw, f.inFmt));
                    yield formatted;
                }
                default -> raw;
            };
        } catch (Exception e) {
            return null;
        }
    }

    /** Minimal deep copy for JsonObject (no arrays here except copied as-is). */
    public static JsonObject deepCopy(JsonObject src) {
        JsonObject out = JsonObject.create();
        for (String k : src.getNames()) {
            Object v = src.get(k);
            if (v instanceof JsonObject jo) out.put(k, deepCopy(jo));
            else out.put(k, v);
        }
        return out;
    }

    /** Fast nested put via dot-path tokens. */
    public static void put(JsonObject root, String[] tokens, Object value) {
        JsonObject cur = root;
        for (int i = 0; i < tokens.length - 1; i++) {
            JsonObject next = cur.getObject(tokens[i]);
            if (next == null) { next = JsonObject.create(); cur.put(tokens[i], next); }
            cur = next;
        }
        cur.put(tokens[tokens.length - 1], value);
    }
}
