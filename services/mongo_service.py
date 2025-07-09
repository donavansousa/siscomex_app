from pymongo import MongoClient
from config import mongo_config
from bson.json_util import dumps

mongo_client = MongoClient(mongo_config.MONGO_URI)
mongo_collection_record = mongo_client[mongo_config.MONGO_DB][mongo_config.MONGO_COLLECTION]
mongo_collection_data = mongo_client[mongo_config.MONGO_DB][mongo_config.MONGO_COLLECTION]

def save_record(obj):
    saved = get_record_by_num_bl(obj['num_bl'])

    if(not saved):
        mongo_collection_record.insert_one(obj)
    else:
        print('atualização')
        mongo_collection_record.update_one({"num_bl": obj['num_bl']}, {"$set":{"max_attempts": obj['max_attempts']}}, upsert=True)

    print('Salvou no mongo')

def save_data(obj):
    saved = get_data_by_ce_mercante(obj['numero_ce_mercante'])

    if(not saved):
        mongo_collection_data.insert_one(obj)
    else:
        print('atualização')
        mongo_collection_data.update_one({"numero_ce_mercante": obj['numero_ce_mercante']}, {"$set":{"situacao_mercadoria": obj['situacao_mercadoria']}}, upsert=True)

    print('Salvou no mongo')

def get_record_by_num_bl(num_bl): 
    return mongo_collection_record.find_one({"num_bl": num_bl})

def get_data_by_ce_mercante(ce_mercante): 
    return mongo_collection_data.find_one({"numero_ce_mercante": ce_mercante})

def __run__():
    with mongo_collection_record.watch() as stream:
        for change in stream:
            print("Mudança detectada:", dumps(change))
