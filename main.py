"""
This is a simple in-memory database implementation.
"""

class Transaction:
    """
    A Transaction does not keep any data,
    but rather keeps track of all the mutation events.
    """
    
    def __init__(self):
        self.events = []

    def add_event(self, event):
        self.events.append(event)

    def get_events(self):
        return self.events

class IMDB:
    """
    IMDB: In-Memory DataBase
    """
    
    def __init__(self):
        self.state = None
        self.db = {}
        self.transactions = []
        self.counts = {}

    def get(self, key):
        return self.db.get(key, "NULL")

    def set(self, key, value):

        # Mutation affects count
        if self.db.get(key):
            self.counts[self.db[key]] -= 1

        if value in self.counts:
            self.counts[value] += 1
        else:
            self.counts[value] = 1

        self.db[key] = value

    def delete(self, key):
        value = self.db.get(key)
        
        # Mutation affects count
        if value in self.counts:
            self.counts[value] -= 1
            
        if key in self.db:
            del self.db[key]

    def count(self, value):
        return self.counts.get(value, 0)

    def end(self):
        self.state = 'END'

    def latest_transaction(self):
        if self.transactions:
            return self.transactions[-1]
        else:
            return None

    def txn_get(self, key):
        """
        Get the value of a key in the current transaction.

        This tries to find the last mutation done to the key:
        whether it's a SET or a DELETE.

        If a DELETE is found first, returns None.
        If a SET is found first, returns the value.

        Args:
            key (str): target key

        Returns:
            str | None: value of the key, None otherwise
        """
    
        current_value = self.get(key)

        # From the latest transaction
        for txn in self.transactions[::-1]:

            # From latest event
            for event in txn.get_events()[::-1]:

                # Key has been deleted, return None 
                if event[0] == 'DELETE' and event[1] == key:
                    return None

                # Key has been set, return value
                elif event[0] == 'SET' and event[1] == key:
                    return event[2]

        return current_value

    def txn_count(self, value):
        """
        Count all keys with the given value in the current transaction.

        Args:
            value (str): the value to count

        Returns:
            int: count of keys with the given value
        """
        current_count = self.count(value)
        keys = set() # keys with the given value in the transactions
        
        # For each transaction
        for t in self.transactions:

            # Get every event
            for event in t.get_events():

                # If key with target value is replaced with a different value
                if event[0] == 'SET' and event[1] in keys and event[2] != value:
                    current_count -= 1
                    keys.remove(event[1])

                # If a new key is given the target value
                elif event[0] == 'SET' and event[1] not in keys and event[2] == value:
                    current_count += 1
                    keys.add(event[1])

                # If key with target value is deleted
                elif event[0] == 'DELETE' and event[1] in keys:
                    current_count -= 1
                    keys.remove(event[1])

        return current_count 

    def apply_transactions(self):
        """
        Applies all events in all transactions to the database.
        """

        # For each transaction
        for t in self.transactions:

            # Get every event
            for event in t.get_events():

                # Apply 'SET' event
                if event[0] == 'SET':
                    _, key, value = event
                    self.db[key] = value

                # Apply 'DELETE' event
                elif event[0] == 'DELETE':
                    _, key = event
                    if key in self.db:
                        del self.db[key]

    def transition(self, command):
        """
        This method is responsible for transitioning the state of the database
        based on the provided command.

        Parameters:
        command (str): The command to execute.

        Returns:
        None
        """

        # 'END' command
        if command.strip().upper() == 'END':
            self.end()

        # 'GET' command
        elif command.strip().upper().startswith('GET'):
            parts = command.strip().split()
            if len(parts) != 2:
                print(f">>> Invalid GET command: '{command}'. Expected format: 'GET <key>'")
            else:
                _, key = parts

                if self.transactions:
                    found_value = self.txn_get(key)

                    if found_value:
                        print(f">>> {found_value}")
                    else:
                        print(">>> NULL")
                else:
                    print(f">>> {self.get(key)}")

        # 'SET' command
        elif command.strip().upper().startswith('SET'):
            parts = command.strip().split(None, 2)
            if len(parts) < 3:
                print(f">>> Invalid SET command: '{command}'. Expected format: 'SET <key> <value>'")
            else:
                _, key, value = parts

                if self.transactions:
                    self.latest_transaction().add_event(('SET', key, value))
                else:
                    self.set(key, value)

        # 'DELETE' command
        elif command.strip().upper().startswith('DELETE'):
            parts = command.split()
            if len(parts) != 2:
                print(f">>> Invalid DELETE command: '{command}'. Expected format: 'DELETE <key>'")
            else:
                _, key = parts

                if self.transactions:
                    self.latest_transaction().add_event(('DELETE', key))
                else:
                    self.delete(key)

        # 'COUNT' command
        elif command.strip().upper().startswith('COUNT'):
            parts = command.strip().split()
            if len(parts) != 2:
                print(f">>> Invalid COUNT command: '{command}'. Expected format: 'COUNT <value>'")
            else:
                _, value = parts
                if self.transactions:
                    print(f">>> {self.txn_count(value)}")
                else:
                    print(f">>> {self.counts.get(value, 0)}")

        # 'BEGIN' command: starts a new transaction
        elif command.strip().upper() == 'BEGIN':
            self.transactions.append(Transaction())

        # 'ROLLBACK' command: rolls back the latest transaction
        elif command.strip().upper() == 'ROLLBACK':
            try:
                self.transactions.pop()
            except IndexError:
                print(">>> TRANSACTION NOT FOUND.")

        # 'COMMIT' command: commits all open transactions
        elif command.strip().upper() == 'COMMIT':
            self.apply_transactions()
            self.transactions.clear()

        # Unknown command
        else:
            print(f">>> Unknown command: '{command}'")

    def run(self):
        try:
            while self.state != 'END':
                command = input(">>> ")
                self.transition(command)
        except KeyboardInterrupt:
            print("\n>>> Interrupted by user. Exiting...")
        finally:
            print(">>> Bye!")

if __name__ == "__main__":
    db = IMDB()
    db.run()
