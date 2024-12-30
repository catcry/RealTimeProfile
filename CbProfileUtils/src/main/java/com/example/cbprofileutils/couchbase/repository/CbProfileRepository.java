package com.example.cbprofileutils.couchbase.repository;

import com.example.cbprofileutils.couchbase.entity.CbProfileEntity;
import org.springframework.data.couchbase.repository.CouchbaseRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface CbProfileRepository extends CouchbaseRepository<CbProfileEntity, String> {
}
