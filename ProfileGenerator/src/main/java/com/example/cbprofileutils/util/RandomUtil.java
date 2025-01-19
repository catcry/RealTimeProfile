package com.example.cbprofileutils.util;

import lombok.Getter;

import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

public class RandomUtil {

    @Getter
    private static final String iranCode = "98";

    private static final Map<String, Integer> countryCodeDigitsMap = new HashMap<>();

    static {
        countryCodeDigitsMap.put("1", 10);   // USA
        countryCodeDigitsMap.put("44", 10);  // UK
        countryCodeDigitsMap.put("61", 10);  // Australia
        countryCodeDigitsMap.put("33", 10);  // France
        countryCodeDigitsMap.put("49", 10);  // Germany
        countryCodeDigitsMap.put("81", 10);  // Japan
        countryCodeDigitsMap.put("86", 11);  // China
        countryCodeDigitsMap.put("7", 10);   // Russia
        countryCodeDigitsMap.put("91", 10);  // India
        countryCodeDigitsMap.put("55", 11);  // Brazil
        countryCodeDigitsMap.put("39", 10);  // Italy
        countryCodeDigitsMap.put("82", 10);  // South Korea
        countryCodeDigitsMap.put("34", 9);   // Spain
        countryCodeDigitsMap.put("52", 10);  // Mexico
        countryCodeDigitsMap.put("20", 10);  // Egypt
        countryCodeDigitsMap.put("90", 10);  // Turkey
        countryCodeDigitsMap.put("62", 11);  // Indonesia
        countryCodeDigitsMap.put("966", 9);  // Saudi Arabia
        countryCodeDigitsMap.put("98", 10);  // Iran
        countryCodeDigitsMap.put("971", 10); // United Arab Emirates
    }

    public static Integer generateRandomInteger(Integer min, Integer max) {
        Random random = new Random();
        return random.nextInt(max - min + 1) + min;
    }

    public static Integer generateRandomInteger(Integer max) {
        Random random = new Random();
        return random.nextInt(max);
    }

    public static boolean generateRandomBoolean() {
        Random random = new Random();
        return random.nextBoolean();
    }

    public static Double generateRandomDouble(Double origin, Double bound) {
        Random random = new Random();
        return random.nextDouble(origin, bound);
    }

    public static String generateRandomDoubleWithCountOfDigitsString(Double origin, Double bound, Integer digitCount) {
        Double randomDouble = generateRandomDouble(origin, bound);
        return String.format("%.2f", randomDouble);
    }

    public static String getRandomCsvEventType() {
        String[] eventTypes = {"1", "2", "3", "4", "8"};
        return eventTypes[RandomUtil.generateRandomInteger(5)];
    }


    public static String generateRandomPhoneNumber(String countryCode, Integer phoneNumberRange) {
        StringBuilder phoneNumber = new StringBuilder();
        phoneNumber.append(generateRandomInteger(1, 9));
        for (int i = 1; i < phoneNumberRange; i++) {
            phoneNumber.append(generateRandomInteger(10));
        }
        return countryCode + phoneNumber;
    }

    public static Map.Entry<String, Integer> getRandomCountryCode() {
        int index = generateRandomInteger(countryCodeDigitsMap.size());
        int i = 0;
        for (Map.Entry<String, Integer> entry : countryCodeDigitsMap.entrySet()) {
            if (i == index) {
                return entry;
            }
            i++;
        }
        return null;
    }

    public static String generateIranRandomPhoneNumber() {
        return generateRandomPhoneNumber(iranCode + "91", 8);
    }

    public static String getUnixTimeString() {
        return String.valueOf(getUnixTime());
    }

    public static Long getUnixTime() {
        return System.currentTimeMillis();
    }

    public static String getTimestamp() {
        return new SimpleDateFormat("yyyyMMddHHmmss").format(new java.util.Date());
    }
}
