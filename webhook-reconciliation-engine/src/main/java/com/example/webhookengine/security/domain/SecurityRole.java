package com.example.webhookengine.security.domain;

public enum SecurityRole {
    ADMIN,
    OPERATOR,
    VIEWER;

    public String getAuthority() {
        return "ROLE_" + name();
    }
}
