package com.example.webhookengine.security.service;

import com.example.webhookengine.security.domain.SecurityUser;
import com.example.webhookengine.security.repository.SecurityUserRepository;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.security.core.userdetails.User;
import org.springframework.security.core.userdetails.UserDetails;
import org.springframework.security.core.userdetails.UserDetailsService;
import org.springframework.security.core.userdetails.UsernameNotFoundException;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.Collections;

@Service
public class CustomUserDetailsService implements UserDetailsService {

    private final SecurityUserRepository securityUserRepository;

    public CustomUserDetailsService(SecurityUserRepository securityUserRepository) {
        this.securityUserRepository = securityUserRepository;
    }

    @Override
    @Transactional(readOnly = true)
    public UserDetails loadUserByUsername(String username) throws UsernameNotFoundException {
        if (username == null || username.isBlank()) {
            throw new UsernameNotFoundException("Username cannot be empty");
        }

        SecurityUser user = securityUserRepository.findByUsername(username.trim())
            .orElseThrow(() -> new UsernameNotFoundException("User not found: " + username));

        if (!user.isEnabled()) {
            throw new UsernameNotFoundException("User account is disabled: " + username);
        }

        return new User(
            user.getUsername(),
            user.getPasswordHash(),
            user.isEnabled(),
            true,
            true,
            true,
            Collections.singletonList(new SimpleGrantedAuthority(user.getRole().getAuthority()))
        );
    }
}
