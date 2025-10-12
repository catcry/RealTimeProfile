package com.example.cbprofileutils.repository;

import com.example.cbprofileutils.entity.CbProfileEntity;
import org.springframework.data.couchbase.repository.CouchbaseRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface CbProfileRepository extends CouchbaseRepository<CbProfileEntity, String> {
}
