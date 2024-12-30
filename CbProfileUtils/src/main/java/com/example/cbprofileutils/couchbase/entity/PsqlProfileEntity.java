package com.example.cbprofileutils.couchbase.entity;


import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.Getter;
import lombok.Setter;

@Entity
@Table(name = "profile")
@Getter
@Setter
public class PsqlProfileEntity {

    @Id
    private Long id;
    @Column
    private String name;
}
