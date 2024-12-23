package com.example.cbprofileutils.couchbase.repository;

import com.example.cbprofileutils.couchbase.entity.CbProfileEntity;
import org.springframework.data.couchbase.repository.CouchbaseRepository;
import org.springframework.data.repository.CrudRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface ProfileRepository extends CouchbaseRepository<CbProfileEntity, String> {
}
