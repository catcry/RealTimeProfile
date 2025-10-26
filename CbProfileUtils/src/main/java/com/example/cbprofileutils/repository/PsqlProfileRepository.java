package com.example.cbprofileutils.repository;

import com.example.cbprofileutils.entity.PsqlProfileEntity;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;
import java.util.List;
import java.util.Optional;

@Repository
public interface PsqlProfileRepository extends JpaRepository<PsqlProfileEntity, Long> , PsqlProfileIdsUpsertRepo {
    Optional<PsqlProfileEntity> findFirstByNameAndProfileTypeId(String name, Long profileTypeId);
    List<PsqlProfileEntity> findAllByNameIn(List<String> names);
}