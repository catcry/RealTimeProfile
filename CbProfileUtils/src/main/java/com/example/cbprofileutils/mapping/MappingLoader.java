package com.example.cbprofileutils.mapping;

import com.couchbase.client.java.json.JsonObject;
import org.yaml.snakeyaml.Yaml;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.*;

public final class MappingLoader {

    @SuppressWarnings("unchecked")
    public static CompiledMapping loadMapping(InputStream yamlIn) {
        Map<String, Object> y = new Yaml().load(yamlIn);
        String idField = (String) y.getOrDefault("id_field", "MSISDN");
        String pfxP = (String) y.getOrDefault("profile_key_prefix", "p::");
        String pfxM = (String) y.getOrDefault("msisdn_key_prefix", "MSISDN::");
        String rawPath = (String) y.get("save_raw_under");

        Map<String, Map<String, Object>> fields = (Map<String, Map<String, Object>>) y.get("fields");
        List<CompiledMapping.FieldSpec> compiled = new ArrayList<>();
        fields.forEach((biHeader, spec) -> compiled.add(
                new CompiledMapping.FieldSpec(
                        biHeader,
                        (String) spec.get("target"),
                        (String) spec.get("type"),
                        (String) spec.get("in_format"),
                        (String) spec.get("out_format")
                )
        ));
        return new CompiledMapping(idField, pfxP, pfxM, rawPath, compiled);
    }

    public static JsonObject loadTemplate(InputStream jsonIn) throws Exception {
        String s = new String(jsonIn.readAllBytes(), StandardCharsets.UTF_8);
        return JsonObject.fromJson(s);
    }

    private MappingLoader() {}
}
