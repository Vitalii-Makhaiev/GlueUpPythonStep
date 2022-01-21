from pymongo import MongoClient

def find_document(collection, elements, multiple=False):
    """ Function to retrieve single or multiple documents from a provided
    Collection using a dictionary containing a document's elements.
    """
    if multiple:
        results = collection.find(elements)
        return [r for r in results]
    else:
        return collection.find_one(elements)
# main area
client = MongoClient(port=27017)
db=client.kpmg_mongo
#documents_collection = db['task_template']
documents = db.task_template.find({})
for document in documents:
    print(document)


collections = db.list_collection_names()
for collection in collections:
    print("collection_name={}".format(collection))
    documents = db[collection].find({})
    for document in documents:
        print("document={}".format(document))
