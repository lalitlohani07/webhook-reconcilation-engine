-- V3__create_security_users_table.sql
-- Create security_users table for operational authentication and role-based access control

CREATE TABLE security_users (
    id UUID PRIMARY KEY,
    username VARCHAR(100) NOT NULL,
    password_hash VARCHAR(255) NOT NULL,
    role VARCHAR(50) NOT NULL,
    enabled BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL,
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL,
    CONSTRAINT uk_security_users_username UNIQUE (username)
);

CREATE INDEX idx_security_users_username ON security_users (username);
