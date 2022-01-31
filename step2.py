from pymongo import MongoClient
# pprint library is used to make the output look more pretty
from pprint import pprint
from mysql.connector import errorcode
from datetime import date, datetime, timedelta
import mysql.connector

global target_database 
global target_host
global target_port
global target_login
global target_password

def find_document(collection, elements, multiple=False):
    """ Function to retrieve single or multiple documents from a provided
    Collection using a dictionary containing a document's elements.
    """
    if multiple:
        results = collection.find(elements)
        return [r for r in results]
    else:
        return collection.find_one(elements)

# main app
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
    # db=client.kpmg_mongo
    collections = db.list_collection_names()
    for collection in collections:
        cntr=0;
        #print("1 =================================")
        start_datetime = datetime.now()
        #print("collection={} started ".format(collection, start_datetime))
        #print("----- target cursor opened -----------")
        target_cursor = target_conn.cursor(buffered=True)
        documents = db[collection].find({})
        for document in documents:
            data=str(document)
            #print("data={}".format(data))
            #query=("insert into stagearea.stg_col_{}(_id, JSON_document, last_modified, last_modified_by) "
            #  "values(%(_id)s,%(JSON_document)s,%(last_modified)s,%(last_modified_by)s".format(id, document,datetime.now(),'PYTHON TOOL STEP 2 '))
            #query=("insert into stagearea.stg_col_{}(JSON_document) values('{}')".format(collection,data))
            print("=========={}".format(collection))
            id ="0"
            if "_id" in document.keys():
                id = document["_id"]
                print(data)
            #query=('insert into stagearea.`stg_col_{}`(id,JSON_document, stg_last_modified, stg_last_modified_by) values(\'{}\',\'{}\',\'{}\',\'{}\');'.format(collection,id,data,datetime.now(),'Python Step 2'))
            # print(query)
            # if collection=="contact_history": 
            # target_row = target_cursor.execute(query)
            if cntr >=10000 : 
                target_conn.commit()
                cntr = 0
                print("Commit")
        target_cursor.close()
    target_conn.commit()
    print (datetime.now() - start_datetime)
finally: target_conn.close()