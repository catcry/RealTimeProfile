package com.example.cbprofileutils.couchbase.repository;

import com.example.cbprofileutils.couchbase.entity.CbBIEntity;
import com.example.cbprofileutils.couchbase.entity.CbProfileDetailEntity;
import org.springframework.data.couchbase.repository.CouchbaseRepository;
import org.springframework.data.couchbase.repository.Query;
import org.springframework.data.repository.CrudRepository;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
public interface ProfileDetailRepository extends CouchbaseRepository<CbProfileDetailEntity, String> {
//public interface ProfileDetailRepository extends CrudRepository<CbProfileDetailEntity, String> {
    @Query("#{#n1ql.selectEntity} WHERE META().id IN $1")
    List<CbProfileDetailEntity> findAllByIdIn(List<String> profileDetailIdList);

    @Query("UPDATE `rtpBucket` SET attrGrps.BI = $bi WHERE id = $id AND `_class` = \"com.example.cbprofileutils.couchbase.entity.ProfileDetailEntity\"")
    void updateProfileDetail(CbBIEntity bi, String id);

}
