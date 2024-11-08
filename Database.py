import os
import logging

# Logging Configuration
LOG_FORMAT = '%(levelname)s | %(asctime)s | %(message)s'
LOG_LEVEL = logging.DEBUG
LOG_DIR = 'log'
LOG_FILE = os.path.join(LOG_DIR, 'client.log')

if not os.path.exists(LOG_DIR):
    os.makedirs(LOG_DIR)

logging.basicConfig(filename=LOG_FILE, format=LOG_FORMAT, level=LOG_LEVEL)


class Database:
    """
    A class to represent a simple in-memory database.
    The database is stored as a dictionary, and it supports basic
    operations such as setting, getting, and deleting values by key.
    """
    def __init__(self, db: dict):
        """
        Initialize the Database with a dictionary.

        :param db: A dictionary representing the database.
        :raises ValueError: If db is not a dictionary.
        """
        if not isinstance(db, dict):
            raise ValueError("DB must be a dictionary.")
        self.DB = db
        logging.info("Database initialized with data: %s", self.DB)

    def set_value(self, key: int, val: int) -> bool:
        """
        Set a key-value pair in the database.

        :param key: The key to set.
        :param val: The value to set.
        :return: True if the value was set successfully, False otherwise.
        """
        try:
            self.DB[key] = val
            logging.info("Value set: %d -> %d", key, val)
            return True
        except Exception as e:
            logging.error("Error setting value for key %d: %s", key, e)
            return False

    def get_value(self, key: int):
        """
        Get the value associated with a key from the database.

        :param key: The key whose value needs to be fetched.
        :return: The value associated with the key, or a message indicating the key doesn't exist.
        """
        try:
            value = self.DB[key]
            logging.info("Fetched value for key %d: %d", key, value)
            return value
        except KeyError:
            logging.warning("Key %d not found in the database.", key)
            return "doesn't exist"

    def del_value(self, key: int) -> bool:
        """
        Delete a key-value pair from the database.

        :param key: The key to delete.
        :return: True if the key was deleted successfully, False otherwise.
        """
        try:
            del self.DB[key]
            logging.info("Deleted key %d from database.", key)
            return True
        except KeyError:
            logging.warning("Key %d not found for deletion.", key)
            return False
