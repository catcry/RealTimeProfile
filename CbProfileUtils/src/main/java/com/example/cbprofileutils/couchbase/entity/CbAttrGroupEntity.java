package com.example.cbprofileutils.couchbase.entity;

import lombok.Getter;
import lombok.Setter;
import nonapi.io.github.classgraph.json.Id;
import org.springframework.data.couchbase.core.mapping.Document;

@Getter
@Setter
@Document
public class CbAttrGroupEntity {
    @Id
    private String id;
    private CbRtdEntity rtd;
    private CbBIEntity BI;
}
