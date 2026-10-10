-- These passwords are disposable values for the local development environment.
CREATE ROLE jungle_app LOGIN PASSWORD 'localpassword' NOSUPERUSER NOCREATEDB NOCREATEROLE;
CREATE ROLE keycloak LOGIN PASSWORD 'localpassword' NOSUPERUSER NOCREATEDB NOCREATEROLE;
CREATE DATABASE keycloak OWNER keycloak;
CREATE DATABASE jungle_test OWNER jungle;
REVOKE ALL ON DATABASE jungle FROM PUBLIC;
REVOKE ALL ON DATABASE jungle_test FROM PUBLIC;
GRANT CONNECT ON DATABASE jungle TO jungle_app;
GRANT CONNECT ON DATABASE jungle_test TO jungle_app;
\connect jungle
REVOKE CREATE ON SCHEMA public FROM PUBLIC;
\connect jungle_test
REVOKE CREATE ON SCHEMA public FROM PUBLIC;
