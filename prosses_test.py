import multiprocessing
import logging
import os
from sync_Database import SynchronizedDatabase

MAX_READERS = 10
READERS_NUM = 10
WRITERS_NUM = 20
DELETERS_NUM = 5
LOOP_TIMES = 10
DATABASE_LENGTH = 20

LOG_FORMAT = '%(levelname)s | %(asctime)s | %(message)s'
LOG_LEVEL = logging.DEBUG
LOG_DIR = 'log'
LOG_FILE = 'client.log'

if not os.path.exists(LOG_DIR):
    os.makedirs(LOG_DIR)

logging.basicConfig(filename=LOG_FILE, format=LOG_FORMAT, level=LOG_LEVEL)


def reader_work(sync_db, key):
    """
    Simulate a reader process that retrieves a value from the database.

    :param sync_db: The synchronized database instance.
    :param key: The key to retrieve from the database.
    """
    try:
        value = sync_db.get_value(key)
        logging.info("Reader got key %d: %s", key, value)
    except Exception as e:
        logging.error("Error in reader process for key %d: %s", key, e)


def writer_work(sync_db, key, value):
    """
    Simulate a writer process that sets a value in the database.

    :param sync_db: The synchronized database instance.
    :param key: The key to set.
    :param value: The value to set.
    """
    try:
        sync_db.set_value(key, value)
        logging.info("Writer set key %d to %d", key, value)
    except Exception as e:
        logging.error("Error in writer process for key %d with value %d: %s", key, value, e)


def deleter_work(sync_db, key):
    """
    Simulate a deleter process that removes a key-value pair from the database.

    :param sync_db: The synchronized database instance.
    :param key: The key to delete.
    """
    try:
        success = sync_db.delete_value(key)
        if success:
            logging.info("Deleter removed key %d", key)
        else:
            logging.warning("Deleter tried to remove key %d but it does not exist", key)
    except Exception as e:
        logging.error("Error in deleter process for key %d: %s", key, e)


def assert_synchronizer_multiprocessing():
    """
    Test the synchronization of multiple processes (readers, writers, deleters) accessing the database.

    - Readers retrieve values from the database.
    - Writers set values in the database.
    - Deleters remove key-value pairs from the database.

    This function creates multiple processes for each type of operation (reading, writing, deleting),
    starts them, and then waits for them to finish. It logs the actions and any errors encountered.
    """
    try:
        db = {i: i + 20 for i in range(DATABASE_LENGTH)}
        sync_db = SynchronizedDatabase(db, mode=False, max_readers=MAX_READERS)

        processes = []

        for i in range(READERS_NUM):
            index = i % DATABASE_LENGTH
            p = multiprocessing.Process(target=reader_work, args=(sync_db, index))
            processes.append(p)

        for i in range(WRITERS_NUM):
            index = (i % (DATABASE_LENGTH // 2)) + (DATABASE_LENGTH // 2)
            p = multiprocessing.Process(target=writer_work, args=(sync_db, index, i))
            processes.append(p)

        for i in range(DELETERS_NUM):
            index = i % DATABASE_LENGTH
            p = multiprocessing.Process(target=deleter_work, args=(sync_db, index))
            processes.append(p)

        for process in processes:
            process.start()

        for process in processes:
            process.join()

        for i in range(DATABASE_LENGTH):
            logging.info("Final state - Key %d: Value %s", i, sync_db.get_value(i))

    except Exception as e:
        logging.error("Error during synchronization test: %s", e)


if __name__ == "__main__":
    assert_synchronizer_multiprocessing()
