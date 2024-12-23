package com.example.cbprofileutils.couchbase.entity;

import lombok.Getter;
import lombok.Setter;
import nonapi.io.github.classgraph.json.Id;
import org.springframework.data.couchbase.core.mapping.Document;

import java.util.ArrayList;
import java.util.List;

@Getter
@Setter
@Document
public class CbProfileInfoEntity {
    @Id
    private String infoId;
    private ArrayList<String> attr = new ArrayList<>();
    private String id;
    private Integer priority;
    private ArrayList<String> roles = new ArrayList<>(List.of("Owner"));
}
