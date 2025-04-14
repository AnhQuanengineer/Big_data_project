from database.mongodb_connect import MongoDBConnect
from database.schema_manager import create_mongodb_schema,validate_mongodb_schema
from config.database_config import get_database_config

def main():
    configMongo = get_database_config()
    with MongoDBConnect(configMongo['mongodb'].uri, configMongo["mongodb"].db_name) as mongo_client:

        create_mongodb_schema(mongo_client.connect())
        print("------------Inserted to MongoDB----------------")
        mongo_client.db.Users.insert_one({
            "user_id": 1,
            "login": "GoogleCodeExporter",
            "gravatar_id": "",
            "avatar_url": "https://avatars.githubusercontent.com/u/9614759?",
            "url": "https://api.github.com/users/GoogleCodeExporter"
        })
        validate_mongodb_schema(mongo_client.connect())


if __name__ == "__main__":
    main()