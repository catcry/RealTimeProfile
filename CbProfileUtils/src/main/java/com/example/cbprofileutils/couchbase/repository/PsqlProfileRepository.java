package com.example.cbprofileutils.couchbase.repository;

import com.example.cbprofileutils.couchbase.entity.PsqlProfileEntity;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.List;


@Repository
public interface PsqlProfileRepository  extends JpaRepository<PsqlProfileEntity, Long> {
    List<PsqlProfileEntity> findByName(String name);
}
