package com.example.cbprofileutils.couchbase.repository;

import com.couchbase.client.java.json.JsonObject;
import com.example.cbprofileutils.couchbase.entity.CbBIEntity;
import com.example.cbprofileutils.couchbase.entity.CbProfileDetailEntity;
import org.springframework.data.couchbase.repository.CouchbaseRepository;
import org.springframework.data.couchbase.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
public interface CbProfileDetailRepository extends CouchbaseRepository<CbProfileDetailEntity, String> {
    @Query("#{#n1ql.selectEntity} WHERE META().id IN $1")
    List<CbProfileDetailEntity> findAllByIdIn(List<String> profileDetailIdList);

//    @Query("UPDATE `rtpBucket` SET attrGrps.BI = $1 WHERE id = $2")
    @Query("UPDATE `rtpBucket` USE KEYS $2 SET attrGrps.BI = $1")
    void updateProfileDetail(CbBIEntity bi, String id);

//    @Query("UPDATE `rtpBucket` SET attrGrps.BI = $bi WHERE META().id = $id")
    @Query("UPDATE `rtpBucket` USE KEYS $id SET attrGrps.BI = $bi")
    void updateProfileDetail(JsonObject bi, String id);

    @Query("UPDATE `rtpBucket` SET attrGrps.BI = $1 WHERE name = $2")
    void updateProfileDetailByName(@Param("bi") JsonObject bi, @Param("name") String name);
}
