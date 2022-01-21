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
collections = db.list_collection_names()
for collection in collections:
    # print("1 =====================================================================================")
    # print("collection={}".format(collection))
    documents = db[collection].find({})
    for document in documents:
        # print("2 =============================================")
        # print("collection={} document={}".format(collection, document))
        for record in document:
            # print("collection={} ! document={} ! record={}".format(collection[0:20], str(document)[0:30], record))
            if (collection=='task_template'): 
                # print("collection={} ! document={} ! record={}".format(collection[0:20], str(document)[0:30], record))
                if record =='categoryTasks':
                    print("collection={} ! document={} ! record={}".format(collection[0:20], str(document)[0:30], record))
                    for rec in record:
                        print('rec={}'.format(rec))
            #print("record={}".format(record))
#            for subrecords in records:
#                print("records={}".format(records))

# 
