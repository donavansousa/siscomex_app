from pymongo import MongoClient
from config import mongo_config
from bson.json_util import dumps

mongo_client = MongoClient(mongo_config.MONGO_URI)
mongo_collection = mongo_client[mongo_config.MONGO_DB][mongo_config.MONGO_COLLECTION]

def save(obj):
    saved = get_by_num_bl(obj['num_bl'])

    if(not saved):
        mongo_collection.insert_one(obj)
    else:
        print('atualização')
        mongo_collection.update_one({"num_bl": obj['num_bl']}, {"$set":{"max_attempts": obj['max_attempts']}}, upsert=True)

    print('Salvou no mongo')

def get_by_num_bl(num_bl): 
    return mongo_collection.find_one({"num_bl": num_bl})

def __run__():
    with mongo_collection.watch() as stream:
        for change in stream:
            print("Mudança detectada:", dumps(change))
