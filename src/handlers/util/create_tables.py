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

    # base tables
    #cur.execute(q.CREATE_CONFERENCE)
    #cur.execute(q.CREATE_TEAM)
    #cur.execute(q.CREATE_PLAYER)
    #cur.execute(q.CREATE_TEAM_TO_CONFERENCE)
    #cur.execute(q.CREATE_PLAYER_TO_TEAM)

    # detail tables
    cur.execute(q.CREATE_DEFENSE)
    cur.execute(q.CREATE_PUNTING)
    cur.execute(q.CREATE_PASSING)
    cur.execute(q.CREATE_RECEIVING)
    cur.execute(q.CREATE_RUSHING)
    cur.execute(q.CREATE_SCORING)
    conn.commit()

    # Close communication with the database
    cur.close()
    conn.close()
