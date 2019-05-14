DROP FUNCTION IF EXISTS frieze_user_create(name, text, text, boolean);

CREATE FUNCTION frieze_user_create(
    username name,
    password text default NULL,
    acl_group text default NULL,
    can_create boolean default false
) RETURNS text AS $$
DECLARE
BEGIN
    IF NOT EXISTS(select 1 from pg_roles where rolname=$1) THEN
        EXECUTE FORMAT('CREATE ROLE "%I"', username);
    END IF;

    /* Set password if required*/
    IF $2 IS NOT NULL THEN
        EXECUTE FORMAT('ALTER USER "%I" WITH LOGIN PASSWORD %L', username, password);
    END IF;

    /* If ACL is set, this is a raw user. Assign username to acl_group

       If ACL is *not* set, this is an access group. Assign it connection rights,
       as well as creation rights if can_create is true*/
    if $3 IS NULL THEN
        EXECUTE FORMAT('GRANT CONNECT ON DATABASE openarc TO "%I"', username);
        IF $4 IS TRUE THEN
            EXECUTE FORMAT('GRANT CREATE ON DATABASE openarc TO "%I"', username);
        END IF;
    ELSE
        EXECUTE FORMAT('GRANT "%I" TO "%I"', acl_group, username);
    END IF;

    RETURN (SELECT rolname FROM pg_roles WHERE rolname=$1);
END;
$$ LANGUAGE plpgsql;
ALTER FUNCTION public.frieze_user_create(name, text, text, boolean) OWNER TO kchoudhu;

DROP FUNCTION IF EXISTS frieze_schema_create(text);
CREATE FUNCTION frieze_schema_create(
    schemaname text
) returns void as $$
DECLARE
BEGIN
    IF NOT EXISTS(select 1 from information_schema.schemata where schema_name=$1) THEN
        EXECUTE FORMAT('CREATE SCHEMA "%I"', schemaname);
    END IF;

    -- set readonly permissions
    EXECUTE FORMAT('GRANT USAGE ON SCHEMA "%I" TO read', schemaname);
    EXECUTE FORMAT('GRANT SELECT ON ALL TABLES IN SCHEMA "%I" TO read', schemaname);
    EXECUTE FORMAT('ALTER DEFAULT PRIVILEGES IN SCHEMA "%I" GRANT SELECT ON TABLES TO read', schemaname);

    -- set readwrite permissions
    EXECUTE FORMAT('GRANT USAGE, CREATE ON SCHEMA "%I" TO write', schemaname);
    EXECUTE FORMAT('GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA "%I" TO write', schemaname);
    EXECUTE FORMAT('ALTER DEFAULT PRIVILEGES IN SCHEMA "%I" GRANT INSERT, UPDATE, DELETE ON TABLES TO write', schemaname);
    EXECUTE FORMAT('GRANT USAGE ON ALL SEQUENCES IN SCHEMA "%I" TO write', schemaname);
    EXECUTE FORMAT('ALTER DEFAULT PRIVILEGES IN SCHEMA "%I" GRANT USAGE ON SEQUENCES TO write', schemaname);
END;
$$ LANGUAGE plpgsql;
ALTER FUNCTION public.frieze_schema_create(text) OWNER TO kchoudhu;

-- Create users
SELECT frieze_user_create(username:='read');
SELECT frieze_user_create(username:='write', can_create:=true);
SELECT frieze_user_create(username:='openarc_ro', password:='openarc_ro', acl_group:='read');
SELECT frieze_user_create(username:='openarc_rw', password:='openarc_rw', acl_group:='read');
SELECT frieze_user_create(username:='openarc_rw', password:='openarc_rw', acl_group:='write');

-- Set system parameters
ALTER SYSTEM SET synchronous_commit=off;
