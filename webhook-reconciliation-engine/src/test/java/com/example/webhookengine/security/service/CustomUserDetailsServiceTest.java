package com.example.webhookengine.security.service;

import com.example.webhookengine.security.domain.SecurityRole;
import com.example.webhookengine.security.domain.SecurityUser;
import com.example.webhookengine.security.repository.SecurityUserRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.security.core.userdetails.UserDetails;
import org.springframework.security.core.userdetails.UsernameNotFoundException;

import java.util.Optional;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class CustomUserDetailsServiceTest {

    @Mock
    private SecurityUserRepository securityUserRepository;

    @InjectMocks
    private CustomUserDetailsService userDetailsService;

    private SecurityUser adminUser;

    @BeforeEach
    void setUp() {
        adminUser = new SecurityUser(
            UUID.randomUUID(),
            "admin_user",
            "$2a$10$hashedpasswordstring",
            SecurityRole.ADMIN,
            true
        );
    }

    @Test
    @DisplayName("loadUserByUsername returns UserDetails with correct authorities when user exists and is enabled")
    void shouldLoadUserByUsernameSuccessfully() {
        when(securityUserRepository.findByUsername("admin_user")).thenReturn(Optional.of(adminUser));

        UserDetails userDetails = userDetailsService.loadUserByUsername("admin_user");

        assertThat(userDetails).isNotNull();
        assertThat(userDetails.getUsername()).isEqualTo("admin_user");
        assertThat(userDetails.getPassword()).isEqualTo("$2a$10$hashedpasswordstring");
        assertThat(userDetails.isEnabled()).isTrue();
        assertThat(userDetails.getAuthorities())
            .extracting("authority")
            .containsExactly("ROLE_ADMIN");
    }

    @Test
    @DisplayName("loadUserByUsername throws UsernameNotFoundException when user is not found")
    void shouldThrowWhenUserNotFound() {
        when(securityUserRepository.findByUsername("unknown")).thenReturn(Optional.empty());

        assertThatThrownBy(() -> userDetailsService.loadUserByUsername("unknown"))
            .isInstanceOf(UsernameNotFoundException.class)
            .hasMessageContaining("User not found");
    }

    @Test
    @DisplayName("loadUserByUsername throws UsernameNotFoundException when user account is disabled")
    void shouldThrowWhenUserIsDisabled() {
        adminUser.setEnabled(false);
        when(securityUserRepository.findByUsername("admin_user")).thenReturn(Optional.of(adminUser));

        assertThatThrownBy(() -> userDetailsService.loadUserByUsername("admin_user"))
            .isInstanceOf(UsernameNotFoundException.class)
            .hasMessageContaining("User account is disabled");
    }

    @Test
    @DisplayName("loadUserByUsername throws UsernameNotFoundException when username is blank")
    void shouldThrowWhenUsernameIsBlank() {
        assertThatThrownBy(() -> userDetailsService.loadUserByUsername("   "))
            .isInstanceOf(UsernameNotFoundException.class)
            .hasMessageContaining("Username cannot be empty");
    }
}
