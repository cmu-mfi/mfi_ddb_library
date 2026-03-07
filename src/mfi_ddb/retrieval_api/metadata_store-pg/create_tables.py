import psycopg2
from config import load_config

def create_tables():
    """ Create tables in the PostgreSQL database"""
    commands = (
        """
        CREATE TABLE user_detail (
            user_id            VARCHAR(50) NOT NULL,
            domain             VARCHAR(50) NOT NULL,
            created_by_user_id VARCHAR(50),
            created_by_domain  VARCHAR(50),
            email              VARCHAR(254),
            name               VARCHAR(50),
            created_at         TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            updated_at         TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            PRIMARY KEY (user_id, domain),
            CONSTRAINT fk_user_created_by FOREIGN KEY (created_by_user_id, created_by_domain) REFERENCES user_detail(user_id, domain) DEFERRABLE INITIALLY DEFERRED
        );        
        """,        
        """
        CREATE TABLE project_detail (
            project_id           UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            name      	         VARCHAR(50),
            created_at           TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            updated_at           TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            created_by_user_id   VARCHAR(50),
            created_by_domain    VARCHAR(50),
            details	             JSONB,
            CONSTRAINT fk_project_created_by FOREIGN KEY (created_by_user_id, created_by_domain) REFERENCES user_detail(user_id, domain)
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
            project_id  UUID NOT NULL REFERENCES project_detail(project_id),
            role        user_role NOT NULL,
            created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            updated_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            CONSTRAINT fk_user_detail FOREIGN KEY (user_id, domain) REFERENCES user_detail(user_id, domain),
            CONSTRAINT unique_user_project_role UNIQUE (user_id, domain, project_id)
        );
        """,
        """
        CREATE TABLE trial_detail (
            id               UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            trial_name       VARCHAR(255),
            user_id          VARCHAR(50),
            user_domain      VARCHAR(50),
            project_id       UUID REFERENCES project_detail(project_id),
            birth_timestamp  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            death_timestamp  TIMESTAMPTZ,
            clean_exit	     BOOLEAN DEFAULT FALSE,
            metadata         JSONB,
            created_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            updated_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            CONSTRAINT fk_trial_user FOREIGN KEY (user_id, user_domain) REFERENCES user_detail(user_id, domain)
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
        DROP TRIGGER IF EXISTS project_detail_set_updated_at ON project_detail;
        CREATE TRIGGER project_detail_set_updated_at
        BEFORE UPDATE ON project_detail
        FOR EACH ROW
        EXECUTE PROCEDURE set_updated_at_timestamp();        
        """,
        """
        DROP TRIGGER IF EXISTS trial_detail_set_updated_at ON trial_detail;
        CREATE TRIGGER trial_detail_set_updated_at
        BEFORE UPDATE ON trial_detail
        FOR EACH ROW
        EXECUTE PROCEDURE set_updated_at_timestamp();
        """,
        """
        DROP TRIGGER IF EXISTS user_detail_set_updated_at ON user_detail;
        CREATE TRIGGER user_detail_set_updated_at
        BEFORE UPDATE ON user_detail
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
            source_trial_id    UUID NOT NULL REFERENCES trial_detail(id),
            target_entity_id   VARCHAR(255) NOT NULL,
            target_entity_type VARCHAR(50) NOT NULL --  ('trial', 'project', 'tag')
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