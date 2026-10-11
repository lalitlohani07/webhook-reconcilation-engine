package com.example.webhookengine.security.config;

import com.example.webhookengine.security.handler.RestAccessDeniedHandler;
import com.example.webhookengine.security.handler.RestAuthenticationEntryPoint;
import com.example.webhookengine.security.service.CustomUserDetailsService;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.HttpMethod;
import org.springframework.security.authentication.AuthenticationManager;
import org.springframework.security.authentication.dao.DaoAuthenticationProvider;
import org.springframework.security.config.annotation.authentication.configuration.AuthenticationConfiguration;
import org.springframework.security.config.annotation.method.configuration.EnableMethodSecurity;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.configuration.EnableWebSecurity;
import org.springframework.security.config.annotation.web.configurers.AbstractHttpConfigurer;
import org.springframework.security.config.http.SessionCreationPolicy;
import org.springframework.security.crypto.bcrypt.BCryptPasswordEncoder;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.security.web.SecurityFilterChain;

@Configuration
@EnableWebSecurity
@EnableMethodSecurity
public class SecurityConfig {

    private final RestAuthenticationEntryPoint restAuthenticationEntryPoint;
    private final RestAccessDeniedHandler restAccessDeniedHandler;
    private final CustomUserDetailsService userDetailsService;

    public SecurityConfig(
        RestAuthenticationEntryPoint restAuthenticationEntryPoint,
        RestAccessDeniedHandler restAccessDeniedHandler,
        CustomUserDetailsService userDetailsService
    ) {
        this.restAuthenticationEntryPoint = restAuthenticationEntryPoint;
        this.restAccessDeniedHandler = restAccessDeniedHandler;
        this.userDetailsService = userDetailsService;
    }

    @Bean
    public PasswordEncoder passwordEncoder() {
        return new BCryptPasswordEncoder();
    }

    @Bean
    public DaoAuthenticationProvider authenticationProvider() {
        DaoAuthenticationProvider authProvider = new DaoAuthenticationProvider();
        authProvider.setUserDetailsService(userDetailsService);
        authProvider.setPasswordEncoder(passwordEncoder());
        return authProvider;
    }

    @Bean
    public AuthenticationManager authenticationManager(AuthenticationConfiguration config) throws Exception {
        return config.getAuthenticationManager();
    }

    @Bean
    public SecurityFilterChain filterChain(HttpSecurity http) throws Exception {
        http
            .csrf(AbstractHttpConfigurer::disable)
            .cors(AbstractHttpConfigurer::disable)
            .sessionManagement(sm -> sm.sessionCreationPolicy(SessionCreationPolicy.STATELESS))
            .authenticationProvider(authenticationProvider())
            .exceptionHandling(ex -> ex
                .authenticationEntryPoint(restAuthenticationEntryPoint)
                .accessDeniedHandler(restAccessDeniedHandler)
            )
            .httpBasic(basic -> basic
                .authenticationEntryPoint(restAuthenticationEntryPoint)
            )
            .authorizeHttpRequests(auth -> auth
                // Public webhook ingestion endpoint (subject to Redis rate limiting)
                .requestMatchers(HttpMethod.POST, "/api/v1/webhooks").permitAll()
                // Minimum Actuator health endpoint
                .requestMatchers("/actuator/health", "/actuator/health/**").permitAll()
                // OpenAPI / Swagger documentation
                .requestMatchers("/api-docs/**", "/swagger-ui/**", "/swagger-ui.html").permitAll()
                // Operational dead-letter endpoints
                .requestMatchers(HttpMethod.POST, "/api/v1/dead-letters/*/replay").hasAnyRole("ADMIN", "OPERATOR")
                .requestMatchers(HttpMethod.GET, "/api/v1/dead-letters", "/api/v1/dead-letters/**").hasAnyRole("ADMIN", "OPERATOR", "VIEWER")
                // Operational reconciliation endpoints
                .requestMatchers(HttpMethod.POST, "/api/v1/reconciliation/runs").hasAnyRole("ADMIN", "OPERATOR")
                .requestMatchers(HttpMethod.GET, "/api/v1/reconciliation/records", "/api/v1/reconciliation/records/**").hasAnyRole("ADMIN", "OPERATOR", "VIEWER")
                // All other unlisted application endpoints require authentication by default
                .anyRequest().authenticated()
            );

        return http.build();
    }
}
