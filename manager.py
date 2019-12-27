'''PostgreSQL logical replication manager'''

import psycopg2
import psycopg2.extras # DictCursor
from colorama import Fore, Style # Colors in terminal
from prettytable import PrettyTable # Pretty table output

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
        slot = ReplicationSlot.create(src, f'{name}_slot')
        dest.commit()
        publication = Publication.create(src, f'{name}_publication')
        dest.commit()

        subscription = Subscriptions(src, dest).get(name)

        if subscription is None:
            dest.rollback() # Flush all existing transactions
            dest.set_session(autocommit=True)
            copy_data = str(copy_data).lower()
            query = f'CREATE SUBSCRIPTION {name} CONNECTION %s PUBLICATION {publication.name} WITH (copy_data = {copy_data}, slot_name = {slot.name}, create_slot = false)'

            _debug(dest.cursor().mogrify(query, (src.dsn,)).decode('utf-8'))

            dest.cursor().execute(query, (src.dsn,))
            dest.set_session(autocommit=False)

        obj = cls()
        obj.name = name
        obj.enabled = True
        obj.dsn = src.dsn
        obj.slot = slot
        obj.publication = publication
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


src = psycopg2.connect('postgres://localhost:5432/src')
dest = psycopg2.connect('postgres://localhost:5432/dest')

# Check we have nothing
Subscriptions(src, dest).show()
Publications(src).show()
ReplicationSlots(src).show()

sub = Subscription.create(src, dest, 'test_sub')
Subscriptions(src, dest).show()
Publications(src).show()
ReplicationSlots(src).show()

# Drop everything
sub.drop()

Subscriptions(src, dest).show()
Publications(src).show()
ReplicationSlots(src).show()

# try:
#     create_logical_repl_slot(cur, 'test_slot')
#     show_repl_slots(cur, 'test_slot')
#     show_repl_slots(cur, 'sdfsf')
# finally:
#     conn.commit()
#     drop_replication_slot(cur, 'test_slot')
# create_subscription(cur, 'postgres://localhost:5432/hypershield_test', 'all_tables')

# show_subscriptions(cur)