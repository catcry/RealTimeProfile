package com.example.cbprofileutils.couchbase.entity;


import com.couchbase.client.java.json.JsonObject;
import com.example.cbprofileutils.util.RandomUtil;
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

    public static JsonObject getRandomJsonRtd() {
        JsonObject rtd = JsonObject.create();
        Integer rtdAge = RandomUtil.generateRandomInteger(1, 1200);
        rtd.put("age", rtdAge.toString());
        String arpuValue = RandomUtil.generateRandomDoubleWithCountOfDigitsString(1.0, 1000000.0, 2);
        rtd.put("arpu", arpuValue);
        if (Double.parseDouble(arpuValue) <= 200000.0) {
            rtd.put("arpu_segment", "Low ARPU");
            rtd.put("loyalty_score", RandomUtil.generateRandomInteger(1, 30).toString());
        } else if (Double.parseDouble(arpuValue) > 200000.0 &&
                Double.parseDouble(arpuValue) <= 700000.0) {
            rtd.put("arpu_segment", "Mid ARPU");
            rtd.put("loyalty_score", RandomUtil.generateRandomInteger(31, 70).toString());
        } else {
            rtd.put("arpu_segment", "High ARPU");
            rtd.put("loyalty_score", RandomUtil.generateRandomInteger(71, 100).toString());
        }
        rtd.put("avg_business_data_usage", "2701.76");
        rtd.put("avg_business_voice_usage","1810.67");
        rtd.put("avg_daily_2g_data_usage", "43");
        rtd.put("avg_daily_3g_data_usage", "180");
        rtd.put("avg_daily_4g_data_usage", "315");
        rtd.put("avg_data_usage", "41564796069");
        rtd.put("avg_monthly_topup_cost", "152500");
        rtd.put("avg_monthly_topup_nmb", "0.67");
        rtd.put("avg_nmb_sms_sent", "181703.95");
        rtd.put("avg_non_business_data_usage", "3615.4");
        rtd.put("avg_non_business_voice_usage", "721");
        rtd.put("avg_offnet_voice_min", "32760.93");
        rtd.put("avg_onnet_voice_min", "79.63");
        rtd.put("brand", "SAMSUNG");
        rtd.put("churn_propensity", "Low");
        rtd.put("daily_data_pkg_end_time", "14001119161518");
        rtd.put("daily_data_pkg_start_time", "14001118161518");
        rtd.put("data_package_size", "s");
        rtd.put("enterprise_national_id", "0370483081");
        rtd.put("gender", RandomUtil.generateRandomBoolean() ? "Male" : "Female");
        rtd.put("industry", "Others");
        rtd.put("is_multi_sim_phone", "No");
        rtd.put("is_smart_phone_user", "No");
        rtd.put("model", "SM-A305F_GALAXY A30_");
        rtd.put("monthly_data_pkg_end_time","14000930232734");
        rtd.put("monthly_data_pkg_start_time","14000929232736");
        rtd.put("onnet_to_offnet_rev", "9.9");
        rtd.put("os", "Android");
        rtd.put("payg_data_rev_to_arpu","0");
        rtd.put("payment_type", "prepaid");
        rtd.put("peak_type_data", "peak");
        rtd.put("peak_type_voice", "peak");
        rtd.put("pkg_to_payg_data", "1");
        rtd.put("portout_propensity","Low");
        rtd.put("provience", "قم");
        rtd.put("roam_int_rev_to_arpu", "28612.74");
        rtd.put("status", "Active");
        rtd.put("is_debtor", RandomUtil.generateRandomBoolean() ? "Yes" : "No");
        rtd.put("sub_balance", "75834257");
        rtd.put("sub_balance_timestamp", "20240506223126");
        rtd.put("subscriber_segment", "HDR");
        rtd.put("tenure", String.valueOf(RandomUtil.generateRandomInteger(1, rtdAge)));
        rtd.put("vas_cost_to_arpu", "444648.15");
        rtd.put("weekly_data_pkg_end_time","14010104091131");
        rtd.put("weekly_data_pkg_start_time",  "14001226091131");
        return rtd;
    }
}
