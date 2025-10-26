package com.example.cbprofileutils.repository;

import java.util.Collection;
import java.util.Map;

public interface PsqlProfileIdsUpsertRepo {
    Long upsertAndReturnId(String name, Long profileTypeId, Long parentId);
    Map<String, Long> batchUpsertAndReturnIds(Collection<String> names, Long profileTypeId);

}