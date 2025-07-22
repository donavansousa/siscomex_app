from pymongo import MongoClient
from config import mongo_config
from bson.json_util import dumps

mongo_client = MongoClient(mongo_config.MONGO_URI)
mongo_collection_record = mongo_client[mongo_config.MONGO_DB][mongo_config.MONGO_COLLECTION_RECORDS]
mongo_collection_data = mongo_client[mongo_config.MONGO_DB][mongo_config.MONGO_COLLECTION_DATA]

def save_record(obj):
    saveds = list(get_record_by_num_bl(obj['num_bl']))

    if(saveds is not None and len(saveds) > 0 ):
        for saved in saveds:
            if(not saved):
                mongo_collection_record.insert_one(obj)
            else:
                print('atualização')
                mongo_collection_record.update_one({"num_bl": obj['num_bl']}, {"$set":{"max_attempts": obj['max_attempts']}}, upsert=True)
    else:
        mongo_collection_record.insert_one(obj)
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
    return mongo_collection_record.find({"num_bl": num_bl})

def get_data_by_ce_mercante(ce_mercante): 
    return mongo_collection_data.find_one({"numero_ce_mercante": ce_mercante})

def __run__():
    with mongo_collection_record.watch() as stream:
        for change in stream:
            print("Mudança detectada:", dumps(change))
