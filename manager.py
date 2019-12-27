'''PostgreSQL logical replication manager'''

import psycopg2
import psycopg2.extras # DictCursor
from colorama import Fore, Style # Colors in terminal
from prettytable import PrettyTable # Pretty table output
from time import sleep

__author__ = 'Lev Kokotov <lev.kokotov@instacart.com>'
__version__ = 0.1

def _debug(query):
    print(Fore.BLUE, '\bpsql: ', query, Style.RESET_ALL)

class ReplicationSlot:
    @classmethod
    def from_row(cls, conn, row):
        obj = cls(conn)

        obj.name = row['slot_name']
        obj.plugin = row['plugin']
        obj.slot_type = row['slot_type']
        obj.confirmed_flush_lsn = row['confirmed_flush_lsn']
        obj.exists = True

        return obj

    @classmethod
    def create(cls, conn, name):
        slot = ReplicationSlots(conn).get(name)

        # Check if slot exists already, if it does, return it
        if slot is not None:
            return slot

        # Otherwise, create it
        query = "SELECT pg_create_logical_replication_slot(%s, %s)"
        cursor = conn.cursor()

        _debug(cursor.mogrify(query, (name, 'pgoutput')).decode('utf-8'))

        cursor.execute(query, (name, 'pgoutput'))

        conn.commit()

        obj = cls(conn)

        obj.name = name
        obj.plugin = 'pgoutput'
        obj.slot_type = 'logical'
        obj.exists = True

        return obj

    def __init__(self, conn):
        self.name = None
        self.plugin = None
        self.slot_type = None
        self.confirmed_flush_lsn = None
        self.exists = False
        self.conn = conn

    def drop(self):
        slots = ReplicationSlots(self.conn)

        if slots.get(self.name) is not None:
            query = "SELECT pg_drop_replication_slot(%s)"
            cursor = self.conn.cursor()

            _debug(cursor.mogrify(query, (self.name,)).decode('utf-8'))

            cursor.execute(query, (self.name,))

            self.conn.commit()

        self.exists = False

    def to_list(self):
        return [self.name, self.plugin, self.slot_type, self.confirmed_flush_lsn]

    def __str__(self):
        return 'Replication slot: ' + '::'.join(repr(self))

class ReplicationSlots:
    def __init__(self, conn):
        self.conn = conn
        self.cursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        self.slots = []

    def refresh(self):
        self.cursor.execute('SELECT * FROM pg_replication_slots')
        self.slots = [ReplicationSlot.from_row(self.conn, slot) for slot in self.cursor.fetchall()]

    def show(self):
        self.refresh()

        print(Fore.GREEN)
        print('\nReplication Slots\n')

        if len(self.slots) == 0:
            print('No replication slots found.')
        else:
            table = PrettyTable(['Slot name', 'Plugin', 'Slot Type', 'Flushed LSN'])
            
            for slot in self.slots:
                table.add_row(slot.to_list())

            print(table)

        print(Style.RESET_ALL)

    def get(self, name):
        self.refresh()

        for slot in self.slots:
            if slot.name == name:
                return slot
        return None


class Publications:
    def __init__(self, conn):
        self.conn = conn
        self.cursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        self.publications = []

    def refresh(self):
        self.cursor.execute('SELECT * FROM pg_publication')
        self.publications = [Publication.from_row(self.conn, row) for row in self.cursor.fetchall()]

    def get(self, name):
        self.refresh()

        for publication in self.publications:
            if publication.name == name:
                return publication

        return None

    def show(self):
        self.refresh()

        print(Fore.GREEN)
        print('\nPublications\n')

        if len(self.publications) == 0:
            print('No publications found.')
        else:
            table = PrettyTable(['Publication name'])
            
            for publication in self.publications:
                table.add_row(publication.to_list())

            print(table)

        print(Style.RESET_ALL) 


class Publication:
    def __init__(self, conn):
        self.conn = conn
        self.name = None
        self.exists = False

    @classmethod
    def create(cls, conn, name):
        publication = Publications(conn).get(name)

        if publication is not None:
            return publication
        else:
            query = f'CREATE PUBLICATION {name} FOR ALL TABLES'

            _debug(query)
            conn.cursor().execute(query)

            obj = cls(conn)
            obj.name = name
            obj.exists = True

            conn.commit()

            return obj

    def __str__(self):
        return f'Publication: {self.name}'

    @classmethod
    def from_row(cls, conn, row):
        obj = cls(conn)
        obj.name = row['pubname']
        obj.exists = True

        return obj

    def to_list(self):
        return [self.name]

    def drop(self):
        publication = Publications(self.conn).get(self.name)

        if publication is not None:
            query = f'DROP PUBLICATION {self.name}'
            
            _debug(query)
            self.conn.cursor().execute(query)
            self.conn.commit()

        self.exists = False



class Subscription:
    def __init__(self):
        self.name = None
        self.enabled = False
        self.dsn = None
        self.slot = None
        self.publication = None
        self.src = None
        self.dest = None

    @classmethod
    def create(cls, src, dest, name, copy_data=False):
        slot_name = f'{name}_slot'
        publication_name = f'{name}_publication'

        ReplicationSlot.create(src, slot_name)
        Publication.create(src, publication_name)

        subscription = Subscriptions(src, dest).get(name)

        if subscription is None:
            dest.rollback() # Flush all existing transactions
            dest.set_session(autocommit=True)
            copy_data = str(copy_data).lower()
            query = f'CREATE SUBSCRIPTION {name} CONNECTION %s PUBLICATION {publication_name} WITH (copy_data = {copy_data}, slot_name = {slot_name}, create_slot = false)'

            _debug(dest.cursor().mogrify(query, (src.dsn,)).decode('utf-8'))

            dest.cursor().execute(query, (src.dsn,))
            dest.set_session(autocommit=False)

        obj = cls()
        obj.name = name
        obj.enabled = True
        obj.dsn = src.dsn
        obj.slot = ReplicationSlots(src).get(slot_name)
        obj.publication = Publications(src).get(publication_name)
        obj.src = src
        obj.dest = dest

        return obj

    def drop(self):
        subscription = Subscriptions(self.src, self.dest).get(self.name)

        if subscription is not None:
            query1 = f'ALTER SUBSCRIPTION {subscription.name} DISABLE'
            query2 = f'ALTER SUBSCRIPTION {subscription.name} SET (slot_name = NONE)'
            query3 = f'DROP SUBSCRIPTION {subscription.name}'

            _debug(query1)
            self.dest.cursor().execute(query1)
            _debug(query2)
            self.dest.cursor().execute(query2)
            _debug(query3)
            self.dest.cursor().execute(query3)
            self.dest.commit()

        self.slot.drop()
        self.publication.drop()

    def disable(self):
        subscription = Subscriptions(self.src, self.dest).get(self.name)

        if subscription is not None:
            query = f'ALTER SUBSCRIPTION {self.name} DISABLE'

            _debug(query)
            self.dest.cursor().execute(query)
            self.dest.commit()

        self.enabed = False

    def enable(self):
        subscription = Subscriptions(self.src, self.dest).get(self.name)

        if subscription is not None:
            query = f'ALTER SUBSCRIPTION {self.name} ENABLE'

            _debug(query)
            self.dest.cursor().execute(query)
            self.dest.commit()
            

    @classmethod
    def from_row(cls, src, dest, row):
        slot = ReplicationSlots(src).get(row['subslotname'])

        if slot is None:
            slot = ReplicationSlot(None)
            slot.name = 'NONE'

        publication = Publications(src).get(row['subpublications'][0])

        if publication is None:
            raise Exception(f'No publication on destiation {src.dsn} exists.')

        obj = cls()
        obj.name = row['subname']
        obj.enabed = row['subenabled']
        obj.dsn = row['subconninfo']
        obj.slot = slot
        obj.publication = publication
        obj.src = src
        obj.dest = dest

        return obj

    def to_list(self):
        return [self.name, self.enabed, self.dsn, self.slot.name, self.publication.name]


class Subscriptions:
    def __init__(self, src, dest):
        self.src = src
        self.dest = dest
        self.cursor = dest.cursor(cursor_factory=psycopg2.extras.DictCursor)

    def refresh(self):
        self.cursor.execute('SELECT * FROM pg_subscription')
        self.subscriptions = [Subscription.from_row(self.src, self.dest, row) for row in self.cursor.fetchall()]
        
    def show(self):
        self.refresh()

        print(Fore.GREEN)
        print('\nSubscriptions\n')

        if len(self.subscriptions) == 0:
            print('No subscriptions found.')
        else:
            table = PrettyTable(['Subscription name', 'Enabled', 'DSN', 'Slot Name', 'Publication'])

            for subscription in self.subscriptions:
                table.add_row(subscription.to_list())

            print(table)

        print(Style.RESET_ALL)

    def get(self, name):
        self.refresh()

        for subscription in self.subscriptions:
            if subscription.name == name:
                return subscription
        return None


class ReplicationOrigin:
    def __init__(self, conn):
        self.conn = conn
        self.name = None

    @classmethod
    def from_row(cls, conn, row):
        obj = cls(conn)
        obj.name = row['roname']

        return obj

    def rewind(self, lsn: str, subscription: Subscription):
        if lsn is None:
            raise Exception('Cannot rewind replication origin to a NULL LSN.')

        sure = input(Fore.RED + '\bThis is a very dangerous operation. Are you sure? [Y/n]: ' + Style.RESET_ALL)
        if sure.strip() != 'Y':
            print(Fore.RED, '\bAborting. Come back when you\'re sure.\n')
        else:
            lsn_correct = input(Fore.GREEN + f'\bPlease confirm you want this LSN {lsn}. [Y/n]: ' + Style.RESET_ALL)
            if lsn_correct.strip() != 'Y':
                print(Fore.RED, '\bAborting. Come back when you\'re sure.')
            else:
                query = 'SELECT pg_replication_origin_advance(%s, %s)'

                subscription.disable()

                print(Fore.GREEN, '\bGiving the replication worker 5 seconds to shut down...')
                sleep(5.0)

                self.conn.rollback() # Flush all transactions
                self.conn.set_session(autocommit=True)
                _debug(self.conn.cursor().mogrify(query, (self.name, lsn)).decode('utf-8'))
                self.conn.cursor().execute(query, (self.name, lsn))
                self.conn.set_session(autocommit=False)
                subscription.enable()

    def to_list(self):
        return [self.name]


class ReplicationOrigins:
    def __init__(self, conn):
        self.conn = conn
        self.cursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        self.origins = []

    def refresh(self):
        self.cursor.execute('SELECT * FROM pg_replication_origin')
        self.origins = [ReplicationOrigin.from_row(self.conn, row) for row in self.cursor.fetchall()]

    def show(self):
        self.refresh()

        print(Fore.GREEN)
        print('\nReplication Origins\n')

        if len(self.origins) == 0:
            print('No replication origins found.')
        else:
            table = PrettyTable(['Name'])

            for origin in self.origins:
                table.add_row(origin.to_list())

            print(table)

        print(Style.RESET_ALL)

    def get(self, name):
        self.refresh()
        for origin in self.origins:
            print(origin.name, name)
            if origin.name == name:
                return origin
        return None

    def last(self):
        '''Get the last replication origin created.'''
        self.refresh()

        if len(self.origins) == 0:
            print(Fore.GREEN, 'No replication origins available.', Style.RESET_ALL)
        else:
            return self.origins[-1]


src = psycopg2.connect('postgres://localhost:5432/src')
dest = psycopg2.connect('postgres://localhost:5432/dest')

# Check we have nothing
Subscriptions(src, dest).show()
Publications(src).show()
ReplicationSlots(src).show()
ReplicationOrigins(src).show()

# Create subscription
sub = Subscription.create(src, dest, 'test_sub')

# Prove that we did it
Subscriptions(src, dest).show()
Publications(src).show()
ReplicationSlots(src).show()
ReplicationOrigins(src).show()

# This will rewind the subscription to where it is already.
ReplicationOrigins(src).last().rewind(sub.slot.confirmed_flush_lsn, sub)

# Drop everything
sub.drop()

# Show everthing
Subscriptions(src, dest).show()
Publications(src).show()
ReplicationSlots(src).show()
ReplicationOrigins(src).show()
