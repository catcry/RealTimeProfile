package com.example.cbprofileutils.mapping;

import com.couchbase.client.java.json.JsonObject;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.function.Function;

public final class CompiledMapping {
    public final String idField;
    public final String profileKeyPrefix;
    public final String msisdnKeyPrefix;
    public final String rawSavePath;
    public final List<FieldSpec> fields;
    public CompiledMapping(String idField, String profileKeyPrefix, String msisdnKeyPrefix,
                           String rawSavePath, List<FieldSpec> fields) {
        this.idField = Objects.requireNonNull(idField);
        this.profileKeyPrefix = Objects.requireNonNull(profileKeyPrefix);
        this.msisdnKeyPrefix = Objects.requireNonNull(msisdnKeyPrefix);
        this.rawSavePath = rawSavePath;
        this.fields = List.copyOf(fields);
    }

    public static Object convert(FieldSpec f, String raw) {
        try {
            return f.converter.apply(raw);
        } catch (Exception e) {
            return null;
        }
    }

    public static JsonObject deepCopy(JsonObject src) {
        JsonObject out = JsonObject.create();
        for (String k : src.getNames()) {
            Object v = src.get(k);
            if (v instanceof JsonObject) {
                out.put(k, deepCopy((JsonObject) v));
            } else {
                out.put(k, v);
            }
        }
        return out;
    }

    public static void put(JsonObject root, String[] tokens, Object value) {
        JsonObject cur = root;
        for (int i = 0; i < tokens.length - 1; i++) {
            JsonObject next = cur.getObject(tokens[i]);
            if (next == null) {
                next = JsonObject.create();
                cur.put(tokens[i], next);
            }
            cur = next;
        }
        cur.put(tokens[tokens.length - 1], value);
    }

    public JsonObject apply(Map<String, String> biRow, JsonObject baseProfileTemplate, boolean saveRaw) {
        // shallow copy of base template (faster than deep copy)
        JsonObject profile = JsonObject.create();
        for (String k : baseProfileTemplate.getNames()) profile.put(k, baseProfileTemplate.get(k));
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

    public static final class FieldSpec {
        public final String biHeader;
        public final String[] pathTokens;
        public final String path;
        public final Type type;
        public final DateTimeFormatter inFmt;
        public final DateTimeFormatter outFmt;
        public final Function<String, Object> converter;

        public FieldSpec(String biHeader, String targetPath, String type, String inFmt, String outFmt) {
            this.biHeader = Objects.requireNonNull(biHeader, "biHeader");
            this.pathTokens = Objects.requireNonNull(targetPath, "targetPath").split("\\.");
            this.path = targetPath;
            String t = (type == null ? "string" : type.trim().toLowerCase());
            this.type = switch (t) {
                case "number" -> Type.NUMBER;
                case "boolean" -> Type.BOOLEAN;
                case "date" -> Type.DATE;
                default -> Type.STRING;
            };
            this.inFmt = (inFmt != null && !inFmt.isBlank()) ? DateTimeFormatter.ofPattern(inFmt) : null;
            this.outFmt = (outFmt != null && !outFmt.isBlank()) ? DateTimeFormatter.ofPattern(outFmt) : null;

            // Precompile converter function for speed
            this.converter = switch (this.type) {
                case NUMBER -> s -> {
                    if (s == null || s.isBlank()) return null;
                    return s.contains(".") ? Double.parseDouble(s) : Long.parseLong(s);
                };
                case BOOLEAN -> s -> s != null && ("true".equalsIgnoreCase(s) || "1".equals(s) || "y".equalsIgnoreCase(s));
                case DATE -> s -> {
                    if (s == null || s.isBlank()) return null;
                    if (inFmt == null || outFmt == null) return s;
                    try {
                        LocalDate dt = LocalDate.parse(s, this.inFmt);
                        return this.outFmt.format(dt);
                    } catch (Exception ex) {
                        return null;
                    }
                };
                default -> s -> s;
            };
        }

        public enum Type {STRING, NUMBER, BOOLEAN, DATE}
    }
}
