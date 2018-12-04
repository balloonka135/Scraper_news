import pymongo
import config


class Database:

    def __init__(self, db_name):
        self.db_name = db_name

    def connect_db(self):
        client = pymongo.MongoClient(config.IP)
        db = client[self.db_name]
        return db
