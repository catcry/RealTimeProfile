package com.example.cbprofileutils.couchbase.entity;

import lombok.Getter;
import lombok.Setter;
import org.springframework.data.annotation.Id;
import org.springframework.data.couchbase.core.mapping.Document;

@Getter
@Setter
@Document
public class CbProfileDetailEntity {

    public static final String PROFILE_DETAIL_ID_PREFIX = "p::";

    private CbAttrGroupEntity attrGrps;
    @Id
    private String profileDetailId;
    private String id;
    private String name;
    private String type;
}
