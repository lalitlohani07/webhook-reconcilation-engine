package com.example.webhookengine.security.service;

import com.example.webhookengine.security.config.SecurityBootstrapProperties;
import com.example.webhookengine.security.domain.SecurityRole;
import com.example.webhookengine.security.domain.SecurityUser;
import com.example.webhookengine.security.repository.SecurityUserRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.boot.DefaultApplicationArguments;
import org.springframework.security.crypto.password.PasswordEncoder;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class BootstrapAdminServiceTest {

    @Mock
    private SecurityUserRepository securityUserRepository;

    @Mock
    private SecurityBootstrapProperties properties;

    @Mock
    private PasswordEncoder passwordEncoder;

    @InjectMocks
    private BootstrapAdminService bootstrapAdminService;

    @Test
    @DisplayName("Bootstrap is skipped when credentials are missing or blank")
    void shouldSkipBootstrapWhenCredentialsAreBlank() {
        when(properties.getUsername()).thenReturn("");
        when(properties.getPassword()).thenReturn(null);

        bootstrapAdminService.run(new DefaultApplicationArguments());

        verify(securityUserRepository, never()).existsByUsername(anyString());
        verify(securityUserRepository, never()).save(any());
    }

    @Test
    @DisplayName("Bootstrap does not overwrite existing administrator password")
    void shouldNotOverwriteExistingAdministrator() {
        when(properties.getUsername()).thenReturn("admin");
        when(properties.getPassword()).thenReturn("securePass123!");
        when(securityUserRepository.existsByUsername("admin")).thenReturn(true);

        bootstrapAdminService.run(new DefaultApplicationArguments());

        verify(securityUserRepository, never()).save(any());
        verify(passwordEncoder, never()).encode(any());
    }

    @Test
    @DisplayName("Bootstrap creates administrator with hashed password when user does not exist")
    void shouldCreateAdministratorWhenNotExists() {
        when(properties.getUsername()).thenReturn("admin");
        when(properties.getPassword()).thenReturn("securePass123!");
        when(securityUserRepository.existsByUsername("admin")).thenReturn(false);
        when(passwordEncoder.encode("securePass123!")).thenReturn("$2a$10$hashedAdminPassword");

        bootstrapAdminService.run(new DefaultApplicationArguments());

        ArgumentCaptor<SecurityUser> userCaptor = ArgumentCaptor.forClass(SecurityUser.class);
        verify(securityUserRepository).save(userCaptor.capture());

        SecurityUser savedUser = userCaptor.getValue();
        assertThat(savedUser.getUsername()).isEqualTo("admin");
        assertThat(savedUser.getPasswordHash()).isEqualTo("$2a$10$hashedAdminPassword");
        assertThat(savedUser.getPasswordHash()).isNotEqualTo("securePass123!");
        assertThat(savedUser.getRole()).isEqualTo(SecurityRole.ADMIN);
        assertThat(savedUser.isEnabled()).isTrue();
        assertThat(savedUser.getId()).isNotNull();
    }
}
