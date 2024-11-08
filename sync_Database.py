from file_Database import FileDatabase
import os
import threading
import multiprocessing
import logging

# Logging Configuration
LOG_FORMAT = '%(levelname)s | %(asctime)s | %(message)s'
LOG_LEVEL = logging.DEBUG
LOG_DIR = 'log'
LOG_FILE = os.path.join(LOG_DIR, 'client.log')

if not os.path.exists(LOG_DIR):
    os.makedirs(LOG_DIR)

logging.basicConfig(filename=LOG_FILE, format=LOG_FORMAT, level=LOG_LEVEL)


class SynchronizedDatabase(FileDatabase):
    """
    A subclass of FileDatabase that adds synchronization mechanisms
    for handling concurrent access in multithreaded or multiprocessed environments.
    """
    def __init__(self, db: dict, mode: bool, max_readers=10):
        """
        Initialize the SynchronizedDatabase with the specified concurrency mode (threading or multiprocessing).

        :param db: A dictionary representing the initial database.
        :param mode: If True, use threading for synchronization; otherwise, use multiprocessing.
        :param max_readers: The maximum number of readers allowed to access the database concurrently.
        """
        super().__init__(db)
        self.mode = mode
        self.max_readers = max_readers
        if mode:
            self.semaphore = threading.Semaphore(max_readers)
            self.write_lock = threading.Lock()
            logging.info("Threading mode enabled with max readers: %d", max_readers)
        else:
            self.semaphore = multiprocessing.Semaphore(max_readers)
            self.write_lock = multiprocessing.Lock()
            logging.info("Multiprocessing mode enabled with max readers: %d", max_readers)

    def acquire_read_lock(self):
        """
        Acquire the read lock to allow concurrent readers.
        """
        try:
            logging.debug("Acquiring read lock.")
            self.semaphore.acquire()
        except Exception as e:
            logging.error("Error acquiring read lock: %s", e)

    def release_read_semaphore(self):
        """
        Release the read lock after reading from the database.
        """
        try:
            logging.debug("Releasing read semaphore.")
            self.semaphore.release()
        except Exception as e:
            logging.error("Error releasing read semaphore: %s", e)

    def acquire_write_lock(self):
        """
        Acquire the write lock to ensure exclusive access to the database for writing.
        """
        try:
            logging.debug("Acquiring write lock.")
            self.write_lock.acquire()
            for _ in range(self.max_readers):
                self.semaphore.acquire()
        except Exception as e:
            logging.error("Error acquiring write lock: %s", e)

    def release_write_lock(self):
        """
        Release the write lock and all read locks after writing to the database.
        """
        try:
            logging.debug("Releasing write lock and read semaphores.")
            for _ in range(self.max_readers):
                self.semaphore.release()
            self.write_lock.release()
        except Exception as e:
            logging.error("Error releasing write lock: %s", e)

    def set_value(self, key: int, value: int) -> bool:
        """
        Set a key-value pair in the database with synchronization.

        :param key: The key to set.
        :param value: The value to set.
        :return: True if the value was set and saved successfully, False otherwise.
        """
        self.acquire_write_lock()
        try:
            return super().set_value(key, value)
        finally:
            self.release_write_lock()

    def get_value(self, key: int):
        """
        Get the value associated with a key from the database with synchronization.

        :param key: The key whose value needs to be fetched.
        :return: The value associated with the key.
        """
        self.acquire_read_lock()
        try:
            return super().get_value(key)
        finally:
            self.release_read_semaphore()

    def delete_value(self, key: int) -> bool:
        """
        Delete a key-value pair from the database with synchronization.

        :param key: The key to delete.
        :return: True if the key was deleted successfully, False otherwise.
        """
        self.acquire_write_lock()
        try:
            return super().del_value(key)
        finally:
            self.release_write_lock()
