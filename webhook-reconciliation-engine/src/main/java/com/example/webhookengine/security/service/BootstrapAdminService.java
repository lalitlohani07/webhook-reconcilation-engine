package com.example.webhookengine.security.service;

import com.example.webhookengine.security.config.SecurityBootstrapProperties;
import com.example.webhookengine.security.domain.SecurityRole;
import com.example.webhookengine.security.domain.SecurityUser;
import com.example.webhookengine.security.repository.SecurityUserRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.UUID;

@Service
public class BootstrapAdminService implements ApplicationRunner {

    private static final Logger log = LoggerFactory.getLogger(BootstrapAdminService.class);

    private final SecurityUserRepository securityUserRepository;
    private final SecurityBootstrapProperties properties;
    private final PasswordEncoder passwordEncoder;

    public BootstrapAdminService(
        SecurityUserRepository securityUserRepository,
        SecurityBootstrapProperties properties,
        PasswordEncoder passwordEncoder
    ) {
        this.securityUserRepository = securityUserRepository;
        this.properties = properties;
        this.passwordEncoder = passwordEncoder;
    }

    @Override
    public void run(ApplicationArguments args) {
        String username = properties.getUsername();
        String password = properties.getPassword();

        if (username == null || username.isBlank() || password == null || password.isBlank()) {
            log.info("Bootstrap administrator configuration not provided or incomplete. Skipping bootstrap admin creation.");
            return;
        }

        createAdminIfNotExists(username.trim(), password);
    }

    @Transactional
    public void createAdminIfNotExists(String normalizedUsername, String password) {
        if (securityUserRepository.existsByUsername(normalizedUsername)) {
            log.info("Bootstrap administrator '{}' already exists. Skipping administrator creation.", normalizedUsername);
            return;
        }

        SecurityUser adminUser = new SecurityUser(
            UUID.randomUUID(),
            normalizedUsername,
            passwordEncoder.encode(password),
            SecurityRole.ADMIN,
            true
        );
        securityUserRepository.save(adminUser);
        log.info("Bootstrap administrator '{}' successfully created with role ADMIN.", normalizedUsername);
    }
}
