import win32file
import win32con
import win32event
import pywintypes
import pickle
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


class FileDatabase(Database):
    """
    A subclass of Database that persists data to a file using win32file.
    """
    def __init__(self, db: dict):
        """
        Initialize the FileDatabase and load existing data from the file if it exists.

        :param db: A dictionary representing the initial database.
        """
        self.filename = "database.pickle"
        self.file_path = 'database.pickle'

        # Create the file if it doesn't exist (using win32file)
        try:
            handle = win32file.CreateFile(
                self.file_path,
                win32con.GENERIC_READ | win32con.GENERIC_WRITE,
                0,  # Exclusive sharing
                None,
                win32con.CREATE_NEW,  # Creates a new file only if it doesn't exist. Otherwise throws an error
                win32con.FILE_ATTRIBUTE_NORMAL,
                None
            )
            win32file.CloseHandle(handle)
            with open(self.file_path, 'wb') as file:  # Initialize with empty dict.
                pickle.dump({}, file)
        except pywintypes.error as e:
            if e.winerror == 80:  # Error 80 means file already exists
                pass
            else:
                logging.error(f"Error creating file: {e}")

        self.load_from_file()
        super().__init__(self.DB)
        logging.info("File database initialized with data: %s", self.DB)

    def save_to_file(self):
        """
        Save the current database to a file using win32file.
        """
        try:
            handle = win32file.CreateFile(
                self.file_path,
                win32con.GENERIC_WRITE,
                0,  # Exclusive sharing
                None,
                win32con.OPEN_EXISTING,  # Opens an existing file.
                win32con.FILE_ATTRIBUTE_NORMAL,
                None
            )

            data = pickle.dumps(self.DB)
            win32file.WriteFile(handle, data)
            win32file.CloseHandle(handle)
            logging.info("Database saved to file: %s", self.file_path)
        except Exception as e:
            logging.error("Error saving database to file: %s", e)

    def load_from_file(self):
        """
        Load the database from a file using win32file.
        """
        try:
            handle = win32file.CreateFile(
                self.file_path,
                win32con.GENERIC_READ,
                0,  # Exclusive sharing
                None,
                win32con.OPEN_EXISTING,  # Opens an existing file.
                win32con.FILE_ATTRIBUTE_NORMAL,
                None
            )
            file_size = os.path.getsize(self.file_path)
            if file_size > 0:
                (err, data) = win32file.ReadFile(handle, file_size)
                self.DB = pickle.loads(data)
            else:
                self.DB = {} # If file is empty, initialize empty dict.
            win32file.CloseHandle(handle)
            logging.info("Database loaded from file: %s", self.file_path)
        except Exception as e:
            logging.error("Error loading database from file: %s", e)

    def set_value(self, key: int, val: int) -> bool:
        """
        Set a key-value pair in the database and save to file.
        """
        self.load_from_file()
        success = super().set_value(key, val)
        if success:
            self.save_to_file()
        return success

    def get_value(self, key: int):
        """
        Get the value associated with a key from the database.
        """
        self.load_from_file()
        return super().get_value(key)

    def del_value(self, key: int) -> bool:
        """
        Delete a key-value pair from the database and save to file.
        """
        self.load_from_file()
        success = super().del_value(key)
        if success:
            self.save_to_file()
        return success


class SynchronizedDatabase(FileDatabase):
    """
    A subclass of FileDatabase that adds synchronization mechanisms using win32event.
    """
    def __init__(self, db: dict, mode: bool, max_readers=10):
        """
        Initialize the SynchronizedDatabase.

        :param db: Initial database dictionary.
        :param mode: Not used in this implementation (kept for compatibility).
        :param max_readers: Maximum number of concurrent readers.
        """
        super().__init__(db)
        self.max_readers = max_readers

        self.read_event = win32event.CreateEvent(None, False, True, None)  # Auto-reset event for readers
        self.write_mutex = win32event.CreateMutex(None, False, None)  # Mutex for writers
        self.reader_count=0

        logging.info("Using win32event for synchronization.")
        logging.info(f"Max readers: {max_readers}")

    def acquire_read_lock(self):
        """Acquire a read lock."""
        win32event.WaitForSingleObject(self.write_mutex, -1) # wait if writer is in the critical section
        self.reader_count+=1
        if self.reader_count == 1: # if its the first reader, lock the resource
            win32event.WaitForSingleObject(self.read_event, -1)
        win32event.ReleaseMutex(self.write_mutex) # release the mutex so other readers can enter

        logging.debug("Reader acquired read lock.")

    def release_read_semaphore(self):
        """Release the read lock."""
        win32event.WaitForSingleObject(self.write_mutex, -1) # lock the mutex to decrement the counter safely
        self.reader_count-=1
        if self.reader_count == 0: # if its the last reader, release the resource
            win32event.SetEvent(self.read_event)
        win32event.ReleaseMutex(self.write_mutex) # release the mutex

        logging.debug("Reader released read lock.")

    def acquire_write_lock(self):
        """Acquire a write lock (exclusive access)."""
        win32event.WaitForSingleObject(self.read_event, -1) # wait for all readers to finish
        win32event.WaitForSingleObject(self.write_mutex, -1) # lock the resource for the writer
        logging.debug("Writer acquired write lock.")

    def release_write_lock(self):
        """Release the write lock."""
        win32event.ReleaseMutex(self.write_mutex) # release the resource
        win32event.SetEvent(self.read_event) # let the readers know they can continue
        logging.debug("Writer released write lock.")

    def set_value(self, key: int, value: int) -> bool:
        """Set a key-value pair with synchronization."""
        self.acquire_write_lock()
        try:
            return super().set_value(key, value)
        finally:
            self.release_write_lock()

    def get_value(self, key: int):
        """Get a value with synchronization."""
        self.acquire_read_lock()
        try:
            return super().get_value(key)
        finally:
            self.release_read_semaphore()

    def del_value(self, key: int) -> bool:
        """Delete a key-value pair with synchronization."""
        self.acquire_write_lock()
        try:
            return super().del_value(key)
        finally:
            self.release_write_lock()




