from pymongo import MongoClient
from pprint import pprint
from mysql.connector import errorcode
from datetime import date, datetime, timedelta
import mysql.connector

global target_database 
global target_host
global target_port
global target_login
global target_password

target_database = "stagearea"
target_host = "localhost"
target_port = 3306
target_login = "etl"
target_password = "etletl"

try:
    target_conn = mysql.connector.connect(user=target_login, password=target_password, host=target_host, port =target_port, database=target_database)
except mysql.connector.Error as err:
    if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
        print("Something is wrong with your user name={} and password for target database={}".format(target_login, target_database))
        exit()
    elif err.errno == errorcode.ER_BAD_DB_ERROR:
        print("Target database does not exist:"+target_database)
        exit()
    else:
        print("Error dusing opening the target database={} Message={}".format(target_database, err))
        exit()
delta = 10
today_delta = datetime.now().date() - timedelta(minutes=10)
try:
    client = MongoClient("mongodb://localhost:27017") 
    db = client['eb_mongo']
    collections = db.list_collection_names()
    queryfile = open('C:/Users/vital/source/repos/PythonGlueUpApplication/Queryfile.txt','w',encoding="utf-8")
    for collection in collections:
        cntr=0;
        start_datetime = datetime.now()
        target_cursor = target_conn.cursor(buffered=True)
        documents = db[collection].find({})
        for document in documents:
            data=str(document).replace("'","\"")
            id ="0"
            if "_id" in document.keys():
                id = document["_id"]
            query=('insert into stagearea.`stg_col_{}`(id,JSON_document, stg_last_modified, stg_last_modified_by) values("{}",\'{}\',"{}","{}");'.format(collection,id,data,datetime.now(),'Python Step 2'))
            try:
                target_row = target_cursor.execute(query)
                target_conn.commit()
                print("+{}".format(collection))
                queryfile.write(query)
            except mysql.connector.Error as err:
                    print("Message error while executing the query. Message ={}".format(err))
                    errorfile = open('C:/Users/vital/source/repos/PythonGlueUpApplication/Errofile.txt','a')
                    errorfile.write("Message error while executing the query. Message ={}".format(err))
                    errorfile.close()
                    exit()
            #if cntr >=1000 : 
            #    target_conn.commit()
            #    cntr = 0
            #    print("Commit")
        target_cursor.close()
    target_conn.commit()
    print (datetime.now() - start_datetime)
finally: 
    target_conn.close()
    queryfile.close()
