import sqlite3

def create_connection(db_file):
    conn = None
    try:
        conn = sqlite3.connect(db_file)
        return conn
    except Error as e:
        print(e)

    return conn

def update_msg(conn, task):
    print("updating:", task)
    sql = '''UPDATE messages SET msg_state = ? WHERE msg_id = ?'''
    cur = conn.cursor()
    cur.execute(sql, task)
    conn.commit()

def select(conn):
    cur = conn.cursor()
    cur.execute("SELECT * FROM messages")

    rows = cur.fetchall()
    for rec in rows: print(rec); update_msg(conn, ("sent",rec[1]))

conn=create_connection("ronmessages.db")
select(conn)
