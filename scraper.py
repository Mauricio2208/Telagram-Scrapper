from telethon.sync import TelegramClient
from telethon.tl.functions.messages import GetDialogsRequest
from telethon.tl.types import InputPeerEmpty
from telethon import functions, types
from telethon.tl.functions.users import GetFullUserRequest
from mysql.connector import Error
import os, sys
import configparser
import csv
import time
import mysql.connector

re="\033[1;31m"
gr="\033[1;32m"
cy="\033[1;36m"

def banner():
    print(f"""
{re}╔╦╗{cy}┌─┐┬  ┌─┐{re}╔═╗  ╔═╗{cy}┌─┐┬─┐┌─┐┌─┐┌─┐┬─┐
{re} ║ {cy}├┤ │  ├┤ {re}║ ╦  ╚═╗{cy}│  ├┬┘├─┤├─┘├┤ ├┬┘
{re} ╩ {cy}└─┘┴─┘└─┘{re}╚═╝  ╚═╝{cy}└─┘┴└─┴ ┴┴  └─┘┴└─

            version : 3.1
youtube.com/channel/UCnknCgg_3pVXS27ThLpw3xQ
        """)



def checkUser(id):
    cursor.execute("select count(*) as count from user where id=%s;", [id])
    record = cursor.fetchone()
    
    if record[0] == 0:
        result = client(functions.users.GetFullUserRequest(
            id=id
        ))
        user = result.user
        bot = 1
        if user.bot == False:
            bot = 0
        cursor.execute("insert into user (id, is_bot, first_name, last_name, username) values (%s, %s, %s, %s, %s)", [user.id, bot, user.first_name, user.last_name, user.username])

cpass = configparser.RawConfigParser()
cpass.read('config.data')

try:
    api_id = cpass['cred']['id']
    api_hash = cpass['cred']['hash']
    phone = cpass['cred']['phone']
    client = TelegramClient(phone, api_id, api_hash)

    host = cpass['db']['host']
    database = cpass['db']['database']
    user = cpass['db']['user']
    password = cpass['db']['password']
except KeyError:
    os.system('clear')
    banner()
    print(re+"[!] run python3 setup.py first !!\n")
    sys.exit(1)

client.connect()
if not client.is_user_authorized():
    client.send_code_request(phone)
    os.system('clear')
    banner()
    client.sign_in(phone, input(gr+'[+] Enter the code: '+re))
 
os.system('clear')
banner()
chats = []
last_date = None
chunk_size = 200

print('Conectando mysql')

try:
    connection_config_dict = {
        'user': user,
        'password': password,
        'host': host,
        'database': database,
        'raise_on_warnings': True,
        'use_pure': False,
        'autocommit': True,
        'pool_size': 5
    }
    connection = mysql.connector.connect(**connection_config_dict)

    if connection.is_connected():
        db_Info = connection.get_server_info()
        print("Connected to MySQL Server version ", db_Info)

except Error as e:
    print("Error while connecting to MySQL", e)

cursor = connection.cursor()

result = client(GetDialogsRequest(
             offset_date=last_date,
             offset_id=0,
             offset_peer=InputPeerEmpty(),
             limit=chunk_size,
             hash = 0
         ))
chats.extend(result.chats)

for chat in chats:
    try:
        for participant in client.iter_participants(chat):
            checkUser(participant.id)

        cursor.execute("select count(*) as count from chat where id=%s;", [chat.id])
        record = cursor.fetchone()
        if record[0] == 0:
            cursor.execute("insert into chat (id, type, title) values (%s, %s, %s)", [chat.id, 'custom', chat.title])

        cursor.execute("select max(id) from message where chat_id=%s;", [chat.id])
        maxId = cursor.fetchone()[0]
        
        if maxId == None:
            maxId = 0

        for message in client.iter_messages(chat, max_id=maxId):
            cursor.execute("select count(*) as count from message where message_id=%s;", [message.id])
            record = cursor.fetchone()
            if record[0] == 0:
                checkUser(message.from_id.user_id)
                cursor.execute("insert into message (message_id, chat_id, user_id, date, text) values (%s, %s, %s, %s, %s)", [message.id, chat.id, message.from_id.user_id, message.date, message.message])

    except:
        continue


if connection.is_connected():
    connection.close()
    print("MySQL connection is closed")







