package com.example.cbprofileutils.couchbase.entity;

import lombok.Getter;
import lombok.Setter;
import nonapi.io.github.classgraph.json.Id;
import org.springframework.data.couchbase.core.mapping.Document;

@Getter
@Setter
@Document
public class CbProfileEntity {

    public static final String PROFILE_ID_PREFIX = "MSISDN::";

    @Id
    private String id;
    private CbProfileInfoEntity[] profiles = new CbProfileInfoEntity[1];
    private String type;
    private String value;
}
