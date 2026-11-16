"""Transactional pgvector index and durable sync leases in the existing MDS DB."""

from contextlib import contextmanager
from uuid import uuid4

from app.ai.config import Principal
from app.services.pg_config import load_config
from psycopg2.extras import Json, RealDictCursor, execute_values
from psycopg2.pool import ThreadedConnectionPool

# Always evaluate permissions against CURRENT trial ownership and project roles.
# The indexed text and any cached project attributes are never an ACL authority.
ACCESS = """(
  (%(user_id)s = 'superadmin' AND %(domain)s = 'superadmin')
  OR (t.user_id = %(user_id)s AND t.user_domain IS NOT DISTINCT FROM %(domain)s)
  OR EXISTS (SELECT 1 FROM user_project_role_linking r
             WHERE r.project_id = t.project_id AND r.user_id = %(user_id)s
               AND r.domain IS NOT DISTINCT FROM %(domain)s
               AND r.role IN ('admin', 'maintainer'))
)"""


def scope_params(identity: Principal):
    return {"user_id": identity.user_id, "domain": identity.domain}


def vector_literal(values):
    return "[" + ",".join(str(float(x)) for x in values) + "]"


class Repository:
    def __init__(self, settings):
        self.settings = settings
        self.pool = None

    @contextmanager
    def cursor(self):
        conn = self.pool.getconn()
        try:
            with conn, conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute("SET LOCAL statement_timeout = '30s'")
                yield cursor
        finally:
            self.pool.putconn(conn)

    def close(self):
        if self.pool:
            self.pool.closeall()
            self.pool = None

    def initialize(self):
        self.pool = ThreadedConnectionPool(1, 5, **load_config(self.settings.pg_config))
        dimensions = self.settings.embedding_dimensions
        with self.cursor() as cur:
            cur.execute("SELECT pg_advisory_xact_lock(hashtext('mfi-ddb-ai-schema'))")
            # This is an explicit opt-in initialization. Missing extension binaries
            # cause startup to fail rather than silently degrading to keyword search.
            cur.execute("CREATE EXTENSION IF NOT EXISTS vector")
            cur.execute("CREATE SCHEMA IF NOT EXISTS ddb_ai")
            cur.execute("""CREATE TABLE IF NOT EXISTS ddb_ai.configuration (
                singleton boolean PRIMARY KEY DEFAULT true CHECK(singleton),
                embedding_signature text NOT NULL)""")
            cur.execute(
                "INSERT INTO ddb_ai.configuration VALUES (true, %s) ON CONFLICT DO NOTHING",
                (self.settings.embedding_signature,),
            )
            cur.execute(
                "SELECT embedding_signature FROM ddb_ai.configuration WHERE singleton"
            )
            if (
                cur.fetchone()["embedding_signature"]
                != self.settings.embedding_signature
            ):
                raise ValueError(
                    "AI embedding model/dimension differs from the existing index; rebuild the index explicitly"
                )
            cur.execute("""CREATE TABLE IF NOT EXISTS ddb_ai.sync_state (
                trial_uuid uuid PRIMARY KEY REFERENCES trial(uuid) ON DELETE CASCADE,
                status text NOT NULL CHECK(status IN ('processing','synced','failed')),
                source_version text,
                lease_token uuid,
                lease_until timestamptz,
                last_checked_at timestamptz NOT NULL DEFAULT now(),
                indexed_at timestamptz,
                next_attempt_at timestamptz NOT NULL DEFAULT now(),
                attempts integer NOT NULL DEFAULT 0,
                error text)""")
            cur.execute(f"""CREATE TABLE IF NOT EXISTS ddb_ai.chunks (
                id uuid PRIMARY KEY,
                trial_uuid uuid NOT NULL REFERENCES trial(uuid) ON DELETE CASCADE,
                source_version text NOT NULL,
                content text NOT NULL,
                provenance jsonb NOT NULL,
                embedding vector({dimensions}) NOT NULL,
                search_text tsvector GENERATED ALWAYS AS
                    (to_tsvector('simple', content)) STORED,
                indexed_at timestamptz NOT NULL DEFAULT now())""")
            cur.execute(
                "CREATE INDEX IF NOT EXISTS ddb_ai_chunks_trial ON ddb_ai.chunks(trial_uuid)"
            )
            cur.execute(
                "CREATE INDEX IF NOT EXISTS ddb_ai_chunks_fts ON ddb_ai.chunks USING gin(search_text)"
            )
            # Exact vector search keeps recall predictable under per-user filters.
            # Approximate indexes should be introduced after corpus-size measurement.

    def candidates(self):
        with self.cursor() as cur:
            cur.execute(
                """SELECT t.* FROM trial t LEFT JOIN ddb_ai.sync_state s ON s.trial_uuid=t.uuid
                WHERE t.clean_exit IS TRUE AND t.death_timestamp IS NOT NULL
                  AND t.death_timestamp >= t.birth_timestamp
                  AND GREATEST(t.death_timestamp, t.updated_at) < now() - make_interval(secs => %s)
                  AND (s.trial_uuid IS NULL OR
                       (s.next_attempt_at <= now() AND (s.lease_until IS NULL OR s.lease_until < now())))
                ORDER BY s.last_checked_at NULLS FIRST, t.updated_at, t.uuid LIMIT %s""",
                (self.settings.settle_seconds, self.settings.batch_size),
            )
            return [dict(row) for row in cur.fetchall()]

    def claim(self, trial_uuid):
        token = str(uuid4())
        with self.cursor() as cur:
            cur.execute(
                """INSERT INTO ddb_ai.sync_state
                (trial_uuid,status,lease_token,lease_until,attempts)
                VALUES (%s,'processing',%s,now()+make_interval(secs => %s),1)
                ON CONFLICT(trial_uuid) DO UPDATE SET status='processing',lease_token=EXCLUDED.lease_token,
                  lease_until=EXCLUDED.lease_until,last_checked_at=now(),attempts=ddb_ai.sync_state.attempts+1,error=NULL
                WHERE (ddb_ai.sync_state.lease_until IS NULL OR ddb_ai.sync_state.lease_until < now())
                  AND ddb_ai.sync_state.next_attempt_at <= now()
                RETURNING lease_token""",
                (str(trial_uuid), token, self.settings.trial_timeout + 60),
            )
            return token if cur.fetchone() else None

    def is_current(self, trial_uuid, version):
        with self.cursor() as cur:
            cur.execute(
                """SELECT EXISTS(SELECT 1 FROM ddb_ai.chunks WHERE trial_uuid=%s AND source_version=%s) AS current""",
                (str(trial_uuid), version),
            )
            return cur.fetchone()["current"]

    def complete(self, trial_uuid, token, version, chunks=None, vectors=None):
        with self.cursor() as cur:
            cur.execute(
                "SELECT lease_token FROM ddb_ai.sync_state WHERE trial_uuid=%s FOR UPDATE",
                (str(trial_uuid),),
            )
            row = cur.fetchone()
            if not row or str(row["lease_token"]) != token:
                raise RuntimeError(
                    "Sync lease was superseded; index replacement refused"
                )
            if chunks is not None:
                if not chunks or vectors is None or len(chunks) != len(vectors):
                    raise ValueError(
                        "An index replacement requires a complete embedding batch"
                    )
                cur.execute(
                    "DELETE FROM ddb_ai.chunks WHERE trial_uuid=%s", (str(trial_uuid),)
                )
                values = [
                    (
                        c.id,
                        str(trial_uuid),
                        version,
                        c.content,
                        Json(c.provenance),
                        vector_literal(v),
                    )
                    for c, v in zip(chunks, vectors)
                ]
                execute_values(
                    cur,
                    """INSERT INTO ddb_ai.chunks
                    (id,trial_uuid,source_version,content,provenance,embedding) VALUES %s""",
                    values,
                    template="(%s,%s,%s,%s,%s,%s::vector)",
                )
            # New evidence and the success acknowledgment commit in one transaction.
            cur.execute(
                """UPDATE ddb_ai.sync_state SET status='synced',source_version=%s,indexed_at=now(),
                last_checked_at=now(),next_attempt_at=now()+make_interval(secs => %s),
                lease_token=NULL,lease_until=NULL,error=NULL WHERE trial_uuid=%s""",
                (version, self.settings.reconcile_seconds, str(trial_uuid)),
            )

    def fail(self, trial_uuid, token, error):
        with self.cursor() as cur:
            cur.execute(
                """UPDATE ddb_ai.sync_state SET status='failed',error=%s,last_checked_at=now(),
                next_attempt_at=now()+make_interval(secs => %s),lease_token=NULL,lease_until=NULL
                WHERE trial_uuid=%s AND lease_token=%s""",
                (error[:1000], self.settings.retry_seconds, str(trial_uuid), token),
            )

    def trial(self, trial_uuid, identity):
        with self.cursor() as cur:
            cur.execute(
                f"SELECT t.* FROM trial t WHERE t.uuid=%(trial)s AND {ACCESS}",
                {**scope_params(identity), "trial": str(trial_uuid)},
            )
            row = cur.fetchone()
            return dict(row) if row else None

    def discover(self, identity, query="", project_id=None, limit=20):
        with self.cursor() as cur:
            cur.execute(
                f"""SELECT t.uuid,t.trial_name,t.project_id,t.birth_timestamp,t.death_timestamp,
                t.clean_exit,t.metadata,t.data_topics FROM trial t WHERE {ACCESS}
                AND (%(project)s::uuid IS NULL OR t.project_id=%(project)s::uuid)
                AND (%(query)s='' OR t.trial_name ILIKE %(like)s OR t.metadata::text ILIKE %(like)s)
                ORDER BY t.birth_timestamp DESC,t.uuid LIMIT %(limit)s""",
                {
                    **scope_params(identity),
                    "project": project_id,
                    "query": query,
                    "like": f"%{query}%",
                    "limit": limit,
                },
            )
            return [dict(row) for row in cur.fetchall()]

    def search(
        self, identity, query, embedding, limit=5, project_id=None, trial_uuid=None
    ):
        params = {
            **scope_params(identity),
            "query": query,
            "vector": vector_literal(embedding),
            "project": project_id,
            "trial": trial_uuid,
            "pool": max(20, limit * 4),
            "limit": limit,
        }
        # ACL filtering is inside the eligible set, BEFORE either retrieval ranking.
        # MATERIALIZED also prevents approximate-index filtering surprises if an
        # operator later adds an ANN index independently.
        with self.cursor() as cur:
            cur.execute(
                f"""WITH eligible AS MATERIALIZED (
                SELECT c.* FROM ddb_ai.chunks c JOIN trial t ON t.uuid=c.trial_uuid
                WHERE {ACCESS}
                  AND (%(project)s::uuid IS NULL OR t.project_id=%(project)s::uuid)
                  AND (%(trial)s::uuid IS NULL OR t.uuid=%(trial)s::uuid)
            ), dense AS (
                SELECT id, row_number() OVER(ORDER BY embedding <=> %(vector)s::vector,id) AS rank
                FROM eligible ORDER BY embedding <=> %(vector)s::vector,id LIMIT %(pool)s
            ), lexical AS (
                SELECT id,row_number() OVER(ORDER BY
                    ts_rank_cd(search_text,websearch_to_tsquery('simple',%(query)s)) DESC,id) AS rank
                FROM eligible WHERE search_text @@ websearch_to_tsquery('simple',%(query)s)
                ORDER BY ts_rank_cd(search_text,websearch_to_tsquery('simple',%(query)s)) DESC,id LIMIT %(pool)s
            ), scores AS (
                SELECT id,sum(score) AS score FROM (
                    SELECT id,1.0/(60+rank) AS score FROM dense
                    UNION ALL SELECT id,1.0/(60+rank) AS score FROM lexical
                ) ranks GROUP BY id
            ) SELECT e.id AS citation_id,e.trial_uuid,e.content,e.provenance,e.indexed_at,
                e.source_version,s.score,1-(e.embedding <=> %(vector)s::vector) AS similarity
              FROM eligible e JOIN scores s ON s.id=e.id
              ORDER BY s.score DESC,e.id LIMIT %(limit)s""",
                params,
            )
            return [dict(row) for row in cur.fetchall()]

    def status(self, identity):
        with self.cursor() as cur:
            cur.execute(
                f"""SELECT s.status,count(*) AS trials,max(s.indexed_at) AS latest_indexed_at
                FROM ddb_ai.sync_state s JOIN trial t ON t.uuid=s.trial_uuid WHERE {ACCESS}
                GROUP BY s.status ORDER BY s.status""",
                scope_params(identity),
            )
            return [dict(row) for row in cur.fetchall()]
