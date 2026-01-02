import os
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

def get_connection():
    return psycopg2.connect(os.environ['DATABASE_URL'])


def setup_schema():
    conn=get_connection()
    conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    curr=conn.cursor()

    curr.execute("""
    CREATE TABLE IF NOT EXISTS repositories(
                 id BIGINT PRIMARY KEY,
                 owner VARCHAR(255) NOT NULL,
                 name VARCHAR(255) NOT NULL,
                 full_name VARCHAR(512) NOT NULL UNIQUE,
                 created_at TIMESTAMPTZ,
                 updated_at TIMESTAMPTZ,
                 last_crawled_at TIMESTAMPTZ DEFAULT NOW(),
                 CONSTRAINT uniquue_owner_name UNIQUE(owner,name)
                 );
""")
    
    curr.execute("""
    CREATE TABLE IF NOT EXISTS repository_stars(
                 id BIGSERIAL PRIMARY KEY,
                 repository_id BIGINT NOT NULL REFERENCES repositories(id) ON DELETE CASCADE,
                 star_count INTEGER NOT NULL,
                 observed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                 CONSTRAINT unique_repo_observation UNIQUE(repository_id,observed_at)
                 );
""")
    
    curr.execute("""
    CREATE INDEX IF NOT EXISTS idx_repos_full_name ON repositories(full_name);
""")
    curr.execute("""
    CREATE INDEX IF NOT EXISTS idx_repos_last_crawled_at ON repositories(last_crawled_at);
""")
    curr.execute("""
    CREATE INDEX IF NOT EXISTS idx_stars_repo_id ON repository_stars(repository_id);
""")
    curr.execute("""
    CREATE INDEX IF NOT EXISTS idx_stars_observed_at ON repository_stars(observed_at DESC);
""")
    

    curr.execute(
        """
    CREATE OR REPLACE VIEW latest_repository_stars AS 
    SELECT DISTINCT ON (repository_id)
        rs.repository_id,
        r.full_name,
        rs.star_count,
        rs.observed_at
    FROM repositories r
    JOIN repository_stars rs
    ON r.id = rs.repository_id
    ORDER BY repository_id,observed_at DESC;
"""
    )

    conn.commit()
    curr.close()
    conn.close()


    print("Database schema successfully")


if __name__=="__main__":
    setup_schema()


