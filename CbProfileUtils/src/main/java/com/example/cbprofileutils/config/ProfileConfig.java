package com.example.cbprofileutils.config;

import com.couchbase.client.java.json.JsonObject;
import com.example.cbprofileutils.mapping.CompiledMapping;
import com.example.cbprofileutils.mapping.MappingLoader;
import jakarta.annotation.PostConstruct;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.Resource;
import org.springframework.stereotype.Component;

@Component
public class ProfileConfig {

    @Value("${mapping.yaml.path}")
    private Resource mappingRes;

    @Value("${profile.template.path}")
    private Resource profileTemplateRes;

    @Value("${msisdn.template.path}")
    private Resource msisdnTemplateRes;

    private volatile CompiledMapping mapping;
    private volatile JsonObject profileTemplate;
    private volatile JsonObject msisdnTemplate;

    @PostConstruct
    public void init() throws Exception {
        try (var in = mappingRes.getInputStream()) {
            this.mapping = MappingLoader.loadMapping(in);
        }
        try (var in = profileTemplateRes.getInputStream()) {
            this.profileTemplate = MappingLoader.loadTemplate(in);
        }
        try (var in = msisdnTemplateRes.getInputStream()) {
            this.msisdnTemplate = MappingLoader.loadTemplate(in);
        }
    }

    public CompiledMapping mapping() { return mapping; }
    public JsonObject profileTemplate() { return profileTemplate; }
    public JsonObject msisdnTemplate() { return msisdnTemplate; }
}
