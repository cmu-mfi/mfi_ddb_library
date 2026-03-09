import psycopg2
from pg_config import load_config

def create_tables():
    """ Create tables in the PostgreSQL database"""
    commands = (
        """
        CREATE TABLE user (
            user_id            VARCHAR(50) NOT NULL,
            domain             VARCHAR(50) NOT NULL,
            created_by_user_id VARCHAR(50),
            created_by_domain  VARCHAR(50),
            email              VARCHAR(254),
            name               VARCHAR(50),
            created_at         TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            updated_at         TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            CONSTRAINT pk_user PRIMARY KEY (user_id, domain),
            CONSTRAINT fk_user_created_by FOREIGN KEY (created_by_user_id, created_by_domain) REFERENCES user(user_id, domain) DEFERRABLE INITIALLY DEFERRED
        );        
        """,        
        """
        CREATE TABLE project (
            project_id           UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            name      	         VARCHAR(50),
            created_at           TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            updated_at           TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            created_by_user_id   VARCHAR(50),
            created_by_domain    VARCHAR(50),
            details	             JSONB,
            CONSTRAINT pk_project PRIMARY KEY (project_id),
            CONSTRAINT fk_project_created_by FOREIGN KEY (created_by_user_id, created_by_domain) REFERENCES user(user_id, domain)
        );
        """,
        """
        DO $$
        BEGIN
        IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'user_role') THEN
            -- 'admin' and 'maintainer' have the same roles
            -- 'operator' and 'researcher' have the same roles
            CREATE TYPE user_role AS ENUM ('admin', 'operator', 'maintainer', 'researcher');
        END IF;
        END$$;
        """,
        """
        CREATE TABLE user_project_role_linking (
            id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            user_id     VARCHAR(50) NOT NULL,
            domain      VARCHAR(50) NOT NULL,
            project_id  UUID NOT NULL REFERENCES project(project_id),
            role        user_role NOT NULL,
            created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            updated_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            CONSTRAINT pk_user_project_role_linking PRIMARY KEY (id),
            CONSTRAINT fk_user FOREIGN KEY (user_id, domain) REFERENCES user(user_id, domain),
            CONSTRAINT unique_user_project_role UNIQUE (user_id, domain, project_id)
        );
        """,
        """
        CREATE TABLE trial (
            id               UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            trial_name       VARCHAR(255),
            user_id          VARCHAR(50),
            user_domain      VARCHAR(50),
            project_id       UUID REFERENCES project(project_id),
            birth_timestamp  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            death_timestamp  TIMESTAMPTZ,
            clean_exit	     BOOLEAN DEFAULT FALSE,
            metadata         JSONB,
            created_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            updated_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            CONSTRAINT pk_trial PRIMARY KEY (id),
            CONSTRAINT fk_trial_user FOREIGN KEY (user_id, user_domain) REFERENCES user(user_id, domain)
        );        
        """,
        """
        CREATE OR REPLACE FUNCTION set_updated_at_timestamp()
        RETURNS TRIGGER AS $$
        BEGIN
            NEW.updated_at = NOW();
            RETURN NEW;
        END;
        $$ LANGUAGE plpgsql;        
        """,
        """
        DROP TRIGGER IF EXISTS project_set_updated_at ON project;
        CREATE TRIGGER project_set_updated_at
        BEFORE UPDATE ON project
        FOR EACH ROW
        EXECUTE PROCEDURE set_updated_at_timestamp();        
        """,
        """
        DROP TRIGGER IF EXISTS trial_set_updated_at ON trial;
        CREATE TRIGGER trial_set_updated_at
        BEFORE UPDATE ON trial
        FOR EACH ROW
        EXECUTE PROCEDURE set_updated_at_timestamp();
        """,
        """
        DROP TRIGGER IF EXISTS user_set_updated_at ON user;
        CREATE TRIGGER user_set_updated_at
        BEFORE UPDATE ON user
        FOR EACH ROW
        EXECUTE PROCEDURE set_updated_at_timestamp();
        """,
        """
        DROP TRIGGER IF EXISTS user_project_role_linking_set_updated_at ON user_project_role_linking;
        CREATE TRIGGER user_project_role_linking_set_updated_at
        BEFORE UPDATE ON user_project_role_linking
        FOR EACH ROW
        EXECUTE PROCEDURE set_updated_at_timestamp();
        """,
        """
        CREATE TABLE graph_edges (
            edge_id            UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            source_trial_id    UUID NOT NULL REFERENCES trial(id),
            target_entity_id   VARCHAR(255) NOT NULL,
            target_entity_type VARCHAR(50) NOT NULL --  ('trial', 'project', 'tag'),
            CONSTRAINT pk_graph_edges PRIMARY KEY (edge_id)
        );
        """
        )
    try:
        config = load_config()
        with psycopg2.connect(**config) as conn:
            with conn.cursor() as cur:
                # execute the CREATE TABLE statement
                ctr = 1;
                for command in commands:
                    print(f"Executing command {ctr}/{len(commands)}...")
                    cur.execute(command)
                    ctr += 1
    except (psycopg2.DatabaseError, Exception) as error:
        print(error)

if __name__ == '__main__':
    create_tables()