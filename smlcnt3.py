from telethon.sync import TelegramClient
from telethon import connection
from telethon.tl.functions.messages import GetHistoryRequest
from datetime import datetime
import time
import sqlite3

clnt = TelegramClient("MessageForwarder", "3942375", "bf38e2a4bf06badf277788987594d4b6")

def create_connection(db_file):
    conn = None
    try:
        conn = sqlite3.connect(db_file)
        return conn
    except Exception as e:
        print(e)

    return conn
def create_table(conn, create_table_sql):
    try:
        c = conn.cursor()
        c.execute(create_table_sql)
    except Exception as e:
        print(e)
def add_msg(conn, task):
    sql = '''INSERT INTO 
        messages(msg_id, msg_text, msg_peer_id, msg_state)
        VALUES(?,?,?,?)'''
    cur = conn.cursor()
    cur.execute(sql, task)
    conn.commit()
    return cur.lastrowid 
def update_msg(conn, task):
    print("updating:", task)
    sql = '''UPDATE messages SET msg_state = ? WHERE msg_id = ?'''
    cur = conn.cursor()
    cur.execute(sql, task)
    conn.commit()
async def select_all_messages(conn):
    print("##select_all_messages")
    cur = conn.cursor()
    cur.execute("SELECT * FROM messages where msg_state='new'")

    rows = cur.fetchall()

    for row in rows: 
        if len(row[2])!=0 and row[4]=="new":
            print("#", len(row[2]), "#", row[0], row[1], row[2][0:40])
            await clnt.send_message("the_schrodinger_cat", row[2])
            update_msg(conn, ("saved", row[1]))
            check_req=select_message_byid(conn, row[1])
            print(check_req[0], "#", check_req[1][0][4])
            time.sleep(10)
    return "ok"

def select_all_messages_sync(conn):
    cur = conn.cursor()
    cur.execute("SELECT * FROM messages")

    rows = cur.fetchall()

    for row in rows: 
        print(row[0], row[1], row[2][0:40], row[3], row[4])
    return "ok"

def select_message_byid(conn, msg_id):
    cur = conn.cursor()
    cur.execute("SELECT * FROM messages where msg_id=?", (msg_id,))

    rows = cur.fetchall()

    #for row in rows: print(row)
    return len(rows), rows

async def main():
    channel = await clnt.get_entity('Bid_Kogan')
    messages = await clnt.get_messages(channel, limit = 10)
    print("ttl len:", len(messages))
    for msg in messages:
        #print(msg)
        #print(dir(msg))
        #print(msg.id, msg.text)
        #print("="*20)
        #print(msg.to_dict())
        #print(msg.id, msg.peer_id)

        if select_message_byid(conn, str(msg.id))[0]==0:
            data=(str(msg.id), msg.text, str(msg.peer_id), "new")
            add_msg(conn, data)
            print("adding new msg:", str(msg.id), msg.text[0:40])

        #break
    
    await select_all_messages(conn)
    
    pass

async def main_test():
    #await clnt.send_message("the_schrodinger_cat", "Shalom.")
    await select_all_messages(conn)

conn=create_connection("ronmessages.db")
sql_create_projects_table = """ CREATE TABLE IF NOT EXISTS messages (
                                        id integer PRIMARY KEY,
                                        msg_id text NOT NULL,
                                        msg_text text,
                                        msg_peer_id text,
                                        msg_state text NOT NULL
                                    ); """
create_table(conn, sql_create_projects_table)

with clnt:
    clnt.loop.run_until_complete(main())
clnt.disconnect()

select_all_messages_sync(conn)