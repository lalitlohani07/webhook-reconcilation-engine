package com.example.webhookengine.security.repository;

import com.example.webhookengine.security.domain.SecurityUser;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.Optional;
import java.util.UUID;

@Repository
public interface SecurityUserRepository extends JpaRepository<SecurityUser, UUID> {

    Optional<SecurityUser> findByUsername(String username);

    boolean existsByUsername(String username);
}
