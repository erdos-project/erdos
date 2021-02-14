"""Every second, sends the message count to 3 receivers.
One receiver processes messages using a callback,
one uses the blocking read() call,
and one uses the non-blocking try_read() call.
"""

import erdos
import time
import json
import copy
import os

class JSONDatabase:
    """
    Append-Only database with JSON file under the hood. Updates the JSON file on db changes.
    """
    def __init__(self, filename, seed_data=None):
        self.filename = filename

        # Open existing db or create new db
        try:
            filepath = os.path.join(os.path.dirname(__file__), self.filename)
            with open(filepath, 'r') as f:
                self.db = json.load(f)
        except:
            self.db = {}
        
        # Add any seeding data that is specified
        if seed_data is not None:
            self.db = seed_data;
        
        # Publish changes to JSON file
        self.__save()
    
    def __save(self):
        """
        Saves/publishes current database state to json file
        """
        filepath = os.path.join(os.path.dirname(__file__), self.filename)
         # save to file
        with open(filepath, 'w') as f:
            json.dump(self.db, f)

    def update(self, key, val):
        """
        Updates existing key-value pair in database.
        Returns the updated value
        """
        if key not in self.db:
            raise KeyError(f"Key: {key} does not exist in database")

        self.db[key] = val
        self.__save();
        
        return val

    def create(self, key, val):
        """
        Adds a new key-value pair to the database
        Returns the val inserted. All keys must be unique
        """
        if key in self.db:
            raise KeyError(f"Key: {key} already exists in database")
    
        self.db[key] = val
        self.__save();

        return val
    
    def read(self, key, default=None):
        """
        Returns the document stored under the specified key
        Returns the 'default' parameter if the key is not found.
        """
        data = self.db.get(key, default)

        return copy.deepcopy(data) if data is not None else None
    
    def delete(self, key):
        """
        Removes the key-value pair associated with 'key'
        """
        if key not in self.db:
            raise KeyError(f"Key: {key} does not exist in database")
        
        del self.db[key]

        self.__save();

class SendOp(erdos.Operator):
    def __init__(self, write_stream):
        self.write_stream = write_stream
        self.store = JSONDatabase("pipeline.json")

    @staticmethod
    def connect():
        return [erdos.WriteStream()]

    def run(self):
        count = self.__restore("count", default=0)
        while True:
            msg = erdos.Message(erdos.Timestamp(coordinates=[count]), count)
            print("SendOp: sending {msg}".format(msg=msg))
            self.__checkpoint("count", count)
            self.write_stream.send(msg)
            count += 1
            time.sleep(1)
    
    def __checkpoint(self, key, val):
        try:
            self.store.update(key, val)
        except:
            self.store.create(key, val)
        
    def __restore(self, key, default=None):
        return self.store.read(key, default=default)

class CallbackOp(erdos.Operator):
    def __init__(self, read_stream):
        print("initializing  op")
        read_stream.add_callback(CallbackOp.callback)

    @staticmethod
    def callback(msg):
        print("CallbackOp: received {msg}".format(msg=msg))

    @staticmethod
    def connect(read_streams):
        return []


class PullOp(erdos.Operator):
    def __init__(self, read_stream):
        self.read_stream = read_stream

    @staticmethod
    def connect(read_streams):
        return []

    def run(self):
        while True:
            data = self.read_stream.read()
            print("PullOp: received {data}".format(data=data))


class TryPullOp(erdos.Operator):
    def __init__(self, read_stream):
        self.read_stream = read_stream

    @staticmethod
    def connect(read_streams):
        return []

    def run(self):
        while True:
            data = self.read_stream.try_read()
            print("TryPullOp: received {data}".format(data=data))
            time.sleep(0.5)


def main():
    """Creates and runs the dataflow graph."""
    (count_stream, ) = erdos.connect(SendOp, erdos.OperatorConfig(), [])
    erdos.connect(CallbackOp, erdos.OperatorConfig(), [count_stream])
    erdos.connect(PullOp, erdos.OperatorConfig(), [count_stream])
    erdos.connect(TryPullOp, erdos.OperatorConfig(), [count_stream])

    erdos.run()


if __name__ == "__main__":
    main()
