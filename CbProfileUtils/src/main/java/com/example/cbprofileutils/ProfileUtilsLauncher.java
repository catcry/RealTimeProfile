package com.example.cbprofileutils;


import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
@org.springframework.boot.autoconfigure.domain.EntityScan(
        basePackages = "com.example.cbprofileutils.entity"
)
public class ProfileUtilsLauncher {
    public static void main(String[] args) {
        new org.springframework.boot.builder.SpringApplicationBuilder(ProfileUtilsLauncher.class)
                .web(org.springframework.boot.WebApplicationType.NONE)
                .run(args);
    }
}
