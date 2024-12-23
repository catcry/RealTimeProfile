package com.example.cbprofileutils.couchbase.entity;


import lombok.Getter;
import lombok.Setter;
import nonapi.io.github.classgraph.json.Id;
import org.springframework.data.couchbase.core.mapping.Document;

@Getter
@Setter
@Document
public class CbRtdEntity {
    @Id
    private String id;
    private String age;
    private String arpu;
    private String arpu_segment;
    private String avg_business_data_usage;
    private String avg_business_voice_usage;
    private String avg_daily_2g_data_usage;
    private String avg_daily_3g_data_usage;
    private String avg_daily_4g_data_usage;
    private String avg_data_usage;
    private String avg_monthly_topup_cost;
    private String avg_monthly_topup_nmb;
    private String avg_nmb_sms_sent;
    private String avg_non_business_data_usage;
    private String avg_non_business_voice_usage;
    private String avg_offnet_voice_min;
    private String avg_onnet_voice_min;
    private String brand;
    private String churn_propensity;
    private String daily_data_pkg_end_time;
    private String daily_data_pkg_start_time;
    private String data_package_size;
    private String enterprise_national_id;
    private String gender;
    private String industry;
    private String is_multi_sim_phone;
    private String is_smart_phone_user;
    private String model;
    private String monthly_data_pkg_end_time;
    private String monthly_data_pkg_start_time;
    private String onnet_to_offnet_rev;
    private String os;
    private String payg_data_rev_to_arpu;
    private String payment_type;
    private String peak_type_data;
    private String peak_type_voice;
    private String pkg_to_payg_data;
    private String portout_propensity;
    private String provience;
    private String roam_int_rev_to_arpu;
    private String status;
    private String loyalty_score;
    private String is_debtor;
    private String sub_balance;
    private String sub_balance_timestamp;
    private String subscriber_segment;
    private String tenure;
    private String vas_cost_to_arpu;
    private String weekly_data_pkg_end_time;
    private String weekly_data_pkg_start_time;

}
