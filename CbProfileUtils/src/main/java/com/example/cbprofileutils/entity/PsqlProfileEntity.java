package com.example.cbprofileutils.entity;


import jakarta.persistence.*;
import lombok.Getter;
import lombok.Setter;
import org.hibernate.annotations.ColumnDefault;
import org.hibernate.annotations.DynamicInsert;


@Entity
@Table(name = "profile")
@Getter
@Setter
public class PsqlProfileEntity {

    @Id
    @GeneratedValue(strategy = GenerationType.SEQUENCE, generator = "profile_seq_gen")
    @SequenceGenerator(
            name = "profile_seq_gen",
            sequenceName = "profile_id_seq", // must match your DB
            allocationSize = 1
    )
    private Long id;
    @Column
    private String name;
    @Column(name = "parent_id")
    private Long parentId;
    @Column(name = "profile_type_id", nullable = false)
    @ColumnDefault("52") // optional, pairs nicely with @DynamicInsert + DB default
    private Long profileTypeId;

    @PrePersist
    void applyDefaults() {
        if (profileTypeId == null) profileTypeId = 52L; // <- default
    }
}
