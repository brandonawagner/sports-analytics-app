import psycopg2 as p
import query as q

# Connect to AWS PostgreSQL Database
if __name__ == "__main__":
    # Connect to the Postgres database
    conn = p.connect(
        host='sports-analytics-v1.c821y2xj1v0x.us-east-1.rds.amazonaws.com',
        port=5432,
        user='postgres',
        password='3j4nSwHRKWYbL2uyYhbv',
        dbname='sa_prod'
    )
    # Open a cursor to perform database operations
    cur = conn.cursor()

    # Execute commands
    # need to drop in this order or else would need to use CASCADE in drop sql of dependent tables
    cur.execute(q.DROP_RECEIVING)
    cur.execute(q.DROP_PASSING)
    cur.execute(q.DROP_RUSHING)
    cur.execute(q.DROP_SCORING)
    cur.execute(q.DROP_DEFENSE)
    cur.execute(q.DROP_PLAYER_TO_TEAM)
    cur.execute(q.DROP_TEAM_TO_CONFERENCE)
    cur.execute(q.DROP_PLAYER)
    cur.execute(q.DROP_TEAM)
    cur.execute(q.DROP_CONFERENCE)

    conn.commit()
    # Close communication with the database
    cur.close()
    conn.close()
