package com.example.cbprofileutils.service;

import com.example.cbprofileutils.repository.PsqlProfileRepository;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Service
@Transactional
public class PsqlProfileIdsService {
    private static final long DEFAULT_PROFILE_TYPE_ID = 52L;
    private final PsqlProfileRepository repo;

    public PsqlProfileIdsService(PsqlProfileRepository repo) {
        this.repo = repo;
    }

    @Transactional(transactionManager = "transactionManager", propagation = Propagation.REQUIRES_NEW)
    public Long getOrCreateId(String msisdn) {
        String name = normalizeName(msisdn);
        return repo.upsertAndReturnId(name, DEFAULT_PROFILE_TYPE_ID, null);
    }

    @Transactional(transactionManager = "transactionManager", propagation = Propagation.REQUIRES_NEW)
    public Map<String, Long> getOrCreateIdsBatch(Collection<String> msisdns) {
        List<String> normalizedNames = msisdns.stream()
                .map(this::normalizeName).toList();
        return repo.batchUpsertAndReturnIds(normalizedNames, DEFAULT_PROFILE_TYPE_ID);
    }

    private String normalizeName(String msisdn) {
        if (msisdn == null || msisdn.isBlank()) {
            throw new IllegalArgumentException("msisdn must not be null/blank");
        }
        String trimmed = msisdn.trim();
        final String prefix = "FAA_";
        if (trimmed.regionMatches(true, 0, prefix, 0, prefix.length())) {
            return prefix + trimmed.substring(prefix.length());
        }
        return prefix + trimmed;
    }
}