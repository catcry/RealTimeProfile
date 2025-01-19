package com.example.cbprofileutils.couchbase.entity;

import lombok.Getter;
import lombok.Setter;
import nonapi.io.github.classgraph.json.Id;
import org.springframework.data.couchbase.core.mapping.Document;

@Getter
@Setter
@Document
public class CbBIEntity {

    @Id
    private String id;
    private String MO_KEY;
    private String MSISDN;
    private String IMEI;
    private String PKG_TO_TOTAL_VOICE;
    private String PKG_TO_TOTAL_DATA;
    private String ROAM_INT_REV_TO_ARPU;
    private String VAS_COST_TO_ARPU;
    private String PAYG_DATA_REV_TO_ARPU;
    private String ONNET_TO_OFFNET_REV;
    private String ARPU;
    private String IS_MULTI_SIM_USER;
    private String SEGMENT;
    private String AVG_ONNET_VOICE_MIN;
    private String AVG_OFFNET_VOICE_MIN;
    private String AVG_SMS_CNT;
    private String AVG_DATA_USAGE;
    private String SEGMENT2;
}
