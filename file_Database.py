import pickle
import os
from Database import Database
import logging

# Logging Configuration
LOG_FORMAT = '%(levelname)s | %(asctime)s | %(message)s'
LOG_LEVEL = logging.DEBUG
LOG_DIR = 'log'
LOG_FILE = os.path.join(LOG_DIR, 'client.log')

if not os.path.exists(LOG_DIR):
    os.makedirs(LOG_DIR)

logging.basicConfig(filename=LOG_FILE, format=LOG_FORMAT, level=LOG_LEVEL)


class FileDatabase(Database):
    """
    A subclass of Database that persists data to a file using pickle.
    The data is saved to a `.pickle` file and can be loaded from the file when needed.
    """
    def __init__(self, db: dict):
        """
        Initialize the FileDatabase and load existing data from the file if it exists.

        :param db: A dictionary representing the initial database.
        """
        super().__init__(db)
        self.file_path = 'database.pickle'  # Path for the pickle file
        if os.path.exists(self.file_path):
            self.load_from_file()  # Load existing data from file
        logging.info("File database initialized with file path: %s", self.file_path)

    def save_to_file(self):
        """
        Save the current database to a file.

        :raises Exception: If there is an error while saving the data to the file.
        """
        try:
            with open(self.file_path, 'wb') as file:  # Open for binary writing
                pickle.dump(self.DB, file)  # Save data using pickle
            logging.info("Database saved to file: %s", self.file_path)
        except Exception as e:
            logging.error("Error saving database to file: %s", e)

    def load_from_file(self):
        """
        Load the database from a file.

        :raises Exception: If there is an error while loading the data from the file.
        """
        try:
            with open(self.file_path, 'rb') as file:  # Open for binary reading
                self.DB = pickle.load(file)  # Load data using pickle
            logging.info("Database loaded from file: %s", self.file_path)
        except Exception as e:
            logging.error("Error loading database from file: %s", e)

    def set_value(self, key: int, val: int) -> bool:
        """
        Set a key-value pair in the database, then save it to the file.

        :param key: The key to set.
        :param val: The value to set.
        :return: True if the value was set and saved successfully, False otherwise.
        """
        self.load_from_file()  # Load the latest data before setting the value
        success = super().set_value(key, val)
        if success:
            self.save_to_file()  # Save after setting value
        return success

    def get_value(self, key: int):
        """
        Get the value associated with a key from the database.

        :param key: The key whose value needs to be fetched.
        :return: The value associated with the key.
        """
        self.load_from_file()  # Load the latest data before fetching the value
        return super().get_value(key)

    def del_value(self, key: int) -> bool:
        """
        Delete a key-value pair from the database, then save it to the file.

        :param key: The key to delete.
        :return: True if the key was deleted successfully and saved to the file, False otherwise.
        """
        self.load_from_file()  # Load the latest data before deletion
        success = super().del_value(key)
        if success:
            self.save_to_file()  # Save after deleting value
        return success
