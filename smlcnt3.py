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
    """ create a table from the create_table_sql statement
    :param conn: Connection object
    :param create_table_sql: a CREATE TABLE statement
    :return:
    """
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
        #update_msg(conn, ("saved", row[1]))
        #time.sleep(10)
    return "ok"

def select_message_byid(conn, msg_id):
    """
    Query all rows in the tasks table
    :param conn: the Connection object
    :return:
    """
    cur = conn.cursor()
    cur.execute("SELECT * FROM messages where msg_id=?", (msg_id,))

    rows = cur.fetchall()

    #for row in rows: print(row)
    return len(rows), rows

async def main222():
    #async for d in clnt.iter_dialogs():
    #    if "Kogan" in d.title:
    #        print(d.id, d.title, d.name)
    #        print("#"*30)
    #    #break
    #await clnt.send_message("the_schrodinger_cat", "Shalom.")

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

async def main():
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

#now=datetime.timestamp(datetime.now())
#data=(str(now)+" msgId", "msgText khdkashdakhdakhdad", "msgPeerId 12313123123", "msg state")
#add_msg(conn, data)

#print("num:", select_all_message_byid(conn, "1618351747.48352 msgId"))
with clnt:
    clnt.loop.run_until_complete(main222())
clnt.disconnect()

select_all_messages_sync(conn)