import os
import json
import csv
from datetime import datetime
import psycopg2


def get_connection():
    return psycopg2.connect(os.environ['DATABASE_URL'])


def export_to_csv():

    conn = get_connection()
    cur = conn.cursor()
 
    os.makedirs('exports', exist_ok=True)
    
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
   

    cur.execute("""
        SELECT 
            r.id,
            r.full_name,
            r.owner,
            r.name,
            rs.star_count,
            r.created_at,
            r.updated_at,
            rs.observed_at as last_star_count_at
        FROM repositories r
        JOIN LATERAL (
            SELECT star_count, observed_at
            FROM repository_stars
            WHERE repository_id = r.id
            ORDER BY observed_at DESC
            LIMIT 1
        ) rs ON true
        ORDER BY rs.star_count DESC
    """)
    
    rows = cur.fetchall()
    columns = ['id', 'full_name', 'owner', 'name', 'star_count', 
               'created_at', 'updated_at', 'last_star_count_at']
    

    csv_path = f'exports/repositories_{timestamp}.csv'
    with open(csv_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(columns)
        writer.writerows(rows)
    
    print(f"Exported {len(rows)} repositories to {csv_path}")
    

    cur.execute("""
        SELECT 
            COUNT(*) as total_repos,
            SUM(star_count) as total_stars,
            AVG(star_count) as avg_stars,
            MAX(star_count) as max_stars,
            MIN(star_count) as min_stars
        FROM latest_repository_stars
    """)
    
    stats = cur.fetchone()
    
    cur.close()
    conn.close()
    
    return csv_path, stats


def export_to_json():

    conn = get_connection()
    cur = conn.cursor()
    
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    
    cur.execute("""
        SELECT 
            r.id,
            r.full_name,
            r.owner,
            r.name,
            rs.star_count,
            r.created_at::text,
            r.updated_at::text,
            rs.observed_at::text as last_star_count_at
        FROM repositories r
        JOIN LATERAL (
            SELECT star_count, observed_at
            FROM repository_stars
            WHERE repository_id = r.id
            ORDER BY observed_at DESC
            LIMIT 1
        ) rs ON true
        ORDER BY rs.star_count DESC
        LIMIT 1000
    """)
    
    rows = cur.fetchall()
    columns = ['id', 'full_name', 'owner', 'name', 'star_count', 
               'created_at', 'updated_at', 'last_star_count_at']
    

    data = [dict(zip(columns, row)) for row in rows]
    
    json_path = f'exports/top_1000_repositories_{timestamp}.json'
    with open(json_path, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=2)
    
    print(f"Exported top 1000 repositories to {json_path}")
    
    cur.close()
    conn.close()
    
    return json_path


def main():
    
    print("Exporting crawled data...")
    
    csv_path, stats = export_to_csv()
    json_path = export_to_json()
    
    print("\n" + "="*60)
    print("Export Summary:")
    print(f"Total repositories: {stats[0]:,}")
    print(f"Total stars: {stats[1]:,}")
    print(f"Average stars: {stats[2]:.2f}")
    print(f"Max stars: {stats[3]:,}")
    print(f"Min stars: {stats[4]:,}")
    print("="*60)


if __name__ == "__main__":
    main()