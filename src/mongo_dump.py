import pandas as pd
from pymongo import MongoClient
import subprocess
from pathlib import Path
import configparser
from logger import Logger
import sys


root_dir = Path(__file__).parent.parent
CONFIG_PATH = str(root_dir / 'config.ini')
CSV_PATH = str(root_dir / 'data' / 'processed_products.csv')
DUMP_PATH = str(root_dir / 'data' / 'mongo-dump')


def create_mongo_dump():
    logger = Logger().get_logger(__name__)
    config = configparser.ConfigParser()
    config.read(CONFIG_PATH)
    db_config = config['DATABASE']

    # Параметры подключения
    port = db_config.getint('port')
    user = db_config.get('user')
    password = db_config.get('password')
    dbname = db_config.get('name')

    try:
        # Подключаемся к MongoDB
        client = MongoClient(f"mongodb://{user}:{password}@localhost:{port}/")
        client.admin.command('ping')  # Проверка подключения
        logger.info("Успешно подключились к MongoDB")
        db = client[dbname]
        collection = db["products"]

        # Удаляем старую коллекцию
        collection.drop()
        logger.info("Старая коллекция 'products' удалена")

        # Чтение CSV порциями и загрузка в базу
        chunk_size = 10000  # Размер пакета
        total_records = 0
        for chunk in pd.read_csv(CSV_PATH, sep='\t', chunksize=chunk_size, dtype=float):
            records = chunk.to_dict('records')
            collection.insert_many(records)
            total_records += len(records)
            logger.info(f"Загружено {total_records} записей")
        logger.info(f"Всего загружено {total_records} записей в коллекцию 'products' базы '{dbname}'")
    except Exception as e:
        logger.error(f"Ошибка при загрузке данных в MongoDB: {e}", exc_info=True)
        sys.exit(1)
    finally:
        client.close()

    # Создаем дамп базы данных
    try:
        subprocess.run([
            "mongodump",
            "--host", "localhost",
            "--port", str(port),
            "--username", user,
            "--password", password,
            "--authenticationDatabase", "admin",
            "--db", dbname,
            "--out", DUMP_PATH
        ], check=True)
        logger.info(f"Дамп успешно создан!")
    except subprocess.CalledProcessError as e:
        logger.error(f"Ошибка при создании дампа: {e}", exc_info=True)
        sys.exit(1)
    

if __name__ == "__main__":
    create_mongo_dump()