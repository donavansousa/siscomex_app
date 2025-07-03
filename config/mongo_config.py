import os

MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017/")
MONGO_DB = "siscarga"
MONGO_COLLECTION = "records"

