'''PostgreSQL logical replication manager'''

import psycopg2
import psycopg2.extras  # DictCursor
import colorama
from colorama import Fore, Style  # Colors in terminal
from prettytable import PrettyTable  # Pretty table output
from time import sleep
import click
from dotenv import load_dotenv
import os

__author__ = 'Lev Kokotov <lev.kokotov@instacart.com>'
__version__ = '0.4.1'

# Load environment variables from .env
load_dotenv()

# Cross-platform colors!
colorama.init()


def _debug(query):
    print(Fore.BLUE, '\bpsql: ', query, Style.RESET_ALL)


def _lock_key():
    return int(''.join(map(lambda x: str(ord(x) % 7), list('pg-logical-manager'))))


def _lock(conn):
    key = _lock_key()
    cursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
    query = "SELECT pg_try_advisory_lock(%s)"

    _debug(cursor.mogrify(query, (key,)).decode('utf-8'))
    cursor.execute(query, (key,))

    return cursor.fetchone()['pg_try_advisory_lock']


def _unlock(conn):
    key = _lock_key()
    cursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
    query = "SELECT pg_advisory_unlock(%s)"

    _debug(cursor.mogrify(query, (key,)).decode('utf-8'))
    cursor.execute(query, (key,))


def _superuser(conn):
    '''Check if the connected user is a SUPERUSER, which is required.'''
    query = 'SELECT usesuper FROM pg_user WHERE usename = CURRENT_USER'
    cursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

    cursor.execute(query)

    return cursor.fetchone()['usesuper']


def _memberof(conn, role='rds_superuser'):
    '''Check if CURRENT_USER is member of role. Useful for checking if user is RDS Superuser.'''
    cursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

    cursor.execute('SELECT "oid" FROM pg_roles WHERE rolname = %s', (role,))
    row = cursor.fetchone()

    # Role doesn't exist.
    if row is None:
        return False

    superuser_role_id = row['oid']

    cursor.execute('SELECT "oid" FROM pg_roles WHERE rolname = CURRENT_USER')

    my_id = cursor.fetchone()['oid']

    cursor.execute("SELECT (COUNT(*) <> 0) AS superuser FROM pg_auth_members WHERE roleid = %s AND member = %s", (superuser_role_id, my_id))

    return cursor.fetchone()['superuser']


class NotSuperUserError(Exception):
    def __init__(self, dsn):
        super()
        self.dsn = dsn


class BelowMinimumVersion(Exception):
    def __init__(self, dsn, server_version):
        super()
        self.dsn = dsn
        self.server_version = server_version


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

    def refresh(self):
        slot = ReplicationSlots(self.conn).get(self.name)

        if slot is not None:
            self.__dict__.update(slot.__dict__)
        else:
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
        self.slots = [ReplicationSlot.from_row(
            self.conn, slot) for slot in self.cursor.fetchall()]

    def show(self):
        self.refresh()

        print(Fore.GREEN)
        print('\nReplication Slots\n')

        if len(self.slots) == 0:
            print('No replication slots found.')
        else:
            table = PrettyTable(
                ['Slot name', 'Plugin', 'Slot Type', 'Flushed LSN'])

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
        self.publications = [Publication.from_row(
            self.conn, row) for row in self.cursor.fetchall()]

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
        self.all_tables = False

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
            obj.all_tables = True

            conn.commit()

            return obj

    def __str__(self):
        return f'Publication: {self.name}'

    @classmethod
    def from_row(cls, conn, row):
        obj = cls(conn)
        obj.name = row['pubname']
        obj.exists = True
        obj.all_tables = row['puballtables']

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
    def create(cls, src, dest, name, copy_data=False, enabled=True, replication_slot=None):
        slot_name = replication_slot if replication_slot is not None else f'{name}_slot'
        publication_name = f'{name}_publication'

        ReplicationSlot.create(src, slot_name)
        Publication.create(src, publication_name)

        subscription = Subscriptions(src, dest).get(name)

        if subscription is None:
            dest.rollback()  # Flush all existing transactions
            dest.set_session(autocommit=True)
            copy_data = str(copy_data).lower()
            enabled = str(enabled).lower()
            query = f'CREATE SUBSCRIPTION {name} CONNECTION %s PUBLICATION {publication_name} WITH (copy_data = {copy_data}, slot_name = {slot_name}, create_slot = false, enabled = {enabled})'

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

    def lock(self):
        return _lock(self.src) and _lock(self.dest)

    def unlock(self):
        _unlock(self.src)
        _unlock(self.dest)

    def replication_lag(self):
        query1 = 'SELECT pg_current_wal_lsn()'
        query2 = 'SELECT (%s::pg_lsn - %s::pg_lsn) AS replication_lag'
        cursor = self.src.cursor(cursor_factory=psycopg2.extras.DictCursor)

        _debug(query1)
        cursor.execute(query1)

        lsn = cursor.fetchone()['pg_current_wal_lsn']

        self.slot.refresh()

        flushed_lsn = self.slot.confirmed_flush_lsn

        _debug(cursor.mogrify(query2, (lsn, flushed_lsn)).decode('utf-8'))
        cursor.execute(query2, (lsn, flushed_lsn))

        lag = cursor.fetchone()['replication_lag']

        return lag

    def reverse(self):
        '''Publisher becomes subscriber, subscriber become publisher.'''
        sure = input(
            Fore.RED + '\bThis is irreversible. Are you sure? [Y/n]: ' + Style.RESET_ALL)

        if sure != 'Y':
            print(Fore.RED, '\bAborting. Come back when you\'re sure.')
            return

        replication_lag = self.replication_lag()

        if replication_lag != 0:
            proceed = input(
                Fore.RED + f'\bReplication lag is {replication_lag}, are you sure you want to proceed? [Y/n]: ' + Style.RESET_ALL)

            if proceed != 'Y':
                print(Fore.RED, '\bAborting. Good call.', Style.RESET_ALL)
                return

        self.drop()

        dest = self.dest
        src = self.src

        # Note that src is now dest, and dest is now src.
        subscription = Subscription.create(
            dest, src, f'{self.name}_reversed', copy_data=False, enabled=True)

        self.slot = subscription.slot
        self.publication = subscription.publication
        self.name = subscription.name
        self.src = dest
        self.dest = src
        self.dsn = self.src.dsn
        self.enabled = True

        # Reverse the configuration
        _write_config(self.src.dsn, self.dest.dsn)

    @classmethod
    def from_row(cls, src, dest, row):
        slot = ReplicationSlots(src).get(row['subslotname'])

        if slot is None:
            slot = ReplicationSlot(None)
            slot.name = 'NONE'

        publication = Publications(src).get(row['subpublications'][0])

        if publication is None:
            raise Exception(f'No publication on destination {src.dsn} exists.')

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
        return [self.name, self.enabed, self.dsn, self.slot.name, self.publication.name, self.replication_lag(), self.slot.confirmed_flush_lsn]


class Subscriptions:
    def __init__(self, src, dest):
        self.src = src
        self.dest = dest
        self.cursor = dest.cursor(cursor_factory=psycopg2.extras.DictCursor)

    def refresh(self):
        self.cursor.execute('SELECT * FROM pg_subscription')
        self.subscriptions = [Subscription.from_row(
            self.src, self.dest, row) for row in self.cursor.fetchall()]

    def show(self):
        self.refresh()

        if len(self.subscriptions) == 0:
            print(Fore.GREEN)
            print('\nSubscriptions\n')
            print('No subscriptions found.')
        else:
            table = PrettyTable(['Subscription name', 'Enabled', 'DSN',
                                 'Slot Name', 'Publication', 'Replication Lag', 'Flushed LSN'])

            for subscription in self.subscriptions:
                table.add_row(subscription.to_list())

            print(Fore.GREEN)
            print('\nSubscriptions\n')
            print(Fore.GREEN, table)

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
        # Check LSN
        if lsn is None:
            raise Exception('Cannot rewind replication origin to a NULL LSN.')

        # Are you sure?
        sure = input(
            Fore.RED + '\bThis is a very dangerous operation. Are you sure? [Y/n]: ' + Style.RESET_ALL)
        if sure.strip() != 'Y':
            print(Fore.RED, '\bAborting. Come back when you\'re sure.\n',
                  Style.RESET_ALL)
            return

        # Check LSN with user
        lsn_correct = input(
            Fore.GREEN + f'\bPlease confirm you want this LSN {lsn}. [Y/n]: ' + Style.RESET_ALL)
        if lsn_correct.strip() != 'Y':
            print(Fore.RED, '\bAborting. Come back when you\'re sure.',
                  Style.RESET_ALL)
            return

        # Make sure no one else is doing this
        locked = subscription.lock()

        if not locked:
            print(Fore.RED, '\bCould not acquire locks on source and destination DBs. Is there another instance of this app running?', Style.RESET_ALL)
            return

        # Ok go
        query = 'SELECT pg_replication_origin_advance(%s, %s)'

        subscription.disable()

        print(Fore.GREEN, '\bGiving the replication worker 5 seconds to shut down...')
        sleep(5.0)

        self.conn.rollback()  # Flush all transactions
        self.conn.set_session(autocommit=True)
        _debug(self.conn.cursor().mogrify(
            query, (self.name, lsn)).decode('utf-8'))
        self.conn.cursor().execute(query, (self.name, lsn))
        self.conn.set_session(autocommit=False)
        subscription.enable()

        subscription.unlock()

    def to_list(self):
        return [self.name]


class ReplicationOrigins:
    def __init__(self, conn):
        self.conn = conn
        self.cursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        self.origins = []

    def refresh(self):
        self.cursor.execute('SELECT * FROM pg_replication_origin')
        self.origins = [ReplicationOrigin.from_row(
            self.conn, row) for row in self.cursor.fetchall()]

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
            if origin.name == name:
                return origin
        return None

    def last(self):
        '''Get the last replication origin created.'''
        self.refresh()

        if len(self.origins) == 0:
            print(Fore.GREEN, '\bNo replication origins available.', Style.RESET_ALL)
        else:
            return self.origins[-1]


class Table:
    def __init__(self, conn):
        self.conn = conn
        self.name = None
        self.owner = None

    @classmethod
    def from_row(cls, conn, row):
        obj = cls(conn)
        obj.name = row['tablename']
        obj.owner = row['tableowner']

        return obj

    def to_list(self):
        return [self.name, self.owner]


class Tables:
    def __init__(self, conn):
        self.conn = conn
        self.cursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        self.tables = []

    def refresh(self):
        query = "SELECT * FROM pg_tables WHERE schemaname ='public';"

        self.cursor.execute(query)

        self.tables = [Table.from_row(self.conn, row)
                       for row in self.cursor.fetchall()]

    def show(self):
        self.refresh()

        print(Fore.GREEN)
        print('\nTables\n')

        if len(self.tables) == 0:
            print('\bNo tables found.', Style.RESET_ALL)
        else:
            print_table = PrettyTable(['Table name', 'Owner'])

            for table in self.tables:
                print_table.add_row(table.to_list())

            print(print_table)

        print(Style.RESET_ALL)

    def get(self, name):
        self.refresh()

        for table in self.tables:
            if table.name == name:
                return table
        return None


class Column:
    def __init__(self, conn, table):
        self.conn = conn
        self.table = table
        self.name = None
        self.type = None

    @classmethod
    def from_row(cls, conn, table, row):
        obj = cls(conn, table)
        obj.name = row['column_name']
        obj.type = row['data_type']

        return obj

    def to_list(self):
        return [self.name, self.type]


class Columns:
    def __init__(self, conn, table):
        self.conn = conn
        self.cursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        self.table = table
        self.columns = []

    def refresh(self):
        query = "SELECT column_name, data_type FROM information_schema.columns WHERE table_schema = 'public' AND table_name = %s ORDER BY column_name"

        self.cursor.execute(query, (self.table.name,))

        self.columns = [Column.from_row(
            self.conn, self.table, row) for row in self.cursor.fetchall()]

    def show(self):
        self.refresh()

        print(Fore.GREEN)
        print(f'\nColumns in "{self.table.name}"\n')

        if len(self.columns) == 0:
            print('No tables found.')
        else:
            table = PrettyTable(['Column name', 'Data type'])

            for column in self.columns:
                table.add_row(column.to_list())

            print(table)

        print(Style.RESET_ALL)

    def get(self, name):
        self.refresh()

        for column in self.columns:
            if column.name == name:
                return column
        return None


def _ensure_connected(source_only=False):
    src_dsn = os.getenv('SOURCE_DB_DSN')
    dest_dsn = os.getenv('DEST_DB_DSN')

    try:
        print(Fore.BLUE, '\bConnecting to source and destination databases...', Style.RESET_ALL)
        src = psycopg2.connect(src_dsn, connect_timeout=5)

        if not source_only:
            dest = psycopg2.connect(dest_dsn, connect_timeout=5)
        else:
            dest = None

        print(Fore.BLUE, '\bConnection established.', Style.RESET_ALL)

        if not _superuser(src) and not _memberof(src, role='rds_superuser'):
            raise NotSuperUserError(src.dsn)
        if dest is not None and not _superuser(dest) and not _memberof(src, role='rds_superuser'):
            raise NotSuperUserError(dest.dsn)
        if src.server_version < 100000:
            raise BelowMinimumVersion(src.dsn, src.server_version)
        if dest is not None and dest.server_version < 100000:
            raise BelowMinimumVersion(src.dsn, src.server_version)
    except (TypeError, psycopg2.ProgrammingError, psycopg2.OperationalError) as e:
        print(
            Fore.RED, f'\bCould not connect to source/destination DB: {e}', Style.RESET_ALL)
        exit(1)
    except NotSuperUserError as e:
        print(
            Fore.RED, f'\b{e.dsn} is not a SUPERUSER which is required.', Style.RESET_ALL)
        exit(1)
    except BelowMinimumVersion as e:
        print(
            Fore.RED, f'\b{e.dsn} (version: {e.server_version}): PostgreSQL 10 or higher is required.', Style.RESET_ALL)
        exit(1)
    finally:
        print(Fore.BLUE, f'\bSource (primary): {src_dsn}', Style.RESET_ALL)
        print(
            Fore.BLUE, f'\bDestination (replica): {dest_dsn}', Style.RESET_ALL)

    # Never mix them up, heh.
    return src, dest


@click.group()
def main():
    '''PostgreSQL logical replication manager'''
    pass


@main.command()
@click.argument('name', required=True)
def create_replication_slot(name):
    '''Manually create a replication slot. Will be created on the source database.'''
    src, _ = _ensure_connected(source_only=True)

    slot = ReplicationSlots(src).get(name)

    if slot is not None:
        print(Fore.GREEN,
              f'\bReplication slot {name} already exists.', Style.RESET_ALL)
    else:
        ReplicationSlot.create(src, name)


@main.command()
@click.argument('name', required=True)
def drop_replication_slot(name):
    '''Manually drop a replication slot.'''
    src, _ = _ensure_connected(source_only=True)

    slot = ReplicationSlots(src).get(name)

    if slot is None:
        print(Fore.GREEN,
              f'\bReplication slot {name} does not exist.', Style.RESET_ALL)
    else:
        slot.drop()


@main.command()
def list_replication_slots():
    src, _ = _ensure_connected(source_only=True)

    ReplicationSlots(src).show()


@main.command()
def list_subscriptions():
    '''List all current subscriptions.'''
    src, dest = _ensure_connected()
    Subscriptions(src, dest).show()


@main.command()
@click.argument('name')
@click.option('--enabled/--disabled', default=True, help='Start the subscription right after creation. Default is yes.')
@click.option('--copy-data/--no-copy', default=False, help='Copy all existing data from publisher to subscriber. Default is no.')
@click.option('--replication-slot', required=False, help='Replication slot on the source to attach the subscription to.')
def create_subscription(name, enabled, copy_data, replication_slot):
    '''Create a logical replication subscription.'''
    src, dest = _ensure_connected()
    Subscription.create(src, dest, name, copy_data=copy_data, enabled=enabled, replication_slot=replication_slot)


@main.command()
@click.argument('name')
def drop_subscription(name):
    '''Drop a logical replication subscription. This will stop the replication immediately.'''
    src, dest = _ensure_connected()
    sub = Subscriptions(src, dest).get(name)

    if sub is None:
        print(Fore.GREEN,
              f'\bNo subscription with name {name} exists.', Style.RESET_ALL)
    else:
        sub.drop()


@main.command()
@click.argument('name')
def enable_subscription(name):
    '''Enable a logical replication subscription.'''
    src, dest = _ensure_connected()
    sub = Subscriptions(src, dest).get(name)

    if sub is None:
        print(Fore.GREEN,
              f'\bNo subscription with name {name} exists.', Style.RESET_ALL)
    else:
        sub.enable()


@main.command()
@click.argument('name')
def disable_subscription(name):
    '''Disable a logical replication subscription.'''
    src, dest = _ensure_connected()
    sub = Subscriptions(src, dest).get(name)

    if sub is None:
        print(Fore.GREEN,
              f'\bNo subscription with name {name} exists.', Style.RESET_ALL)
    else:
        sub.disable()


@main.command()
def list_replication_origins():
    '''Show all replication origins.'''
    src, _ = _ensure_connected()
    ReplicationOrigins(src).show()


@main.command()
@click.argument('origin')
@click.option('--subscription', '-s', help='The name of the logical subscription using this origin.', required=True)
@click.option('--lsn', '-l', help='The WAL offset (LSN) to rewind to. Example: 0/16EDE8A0', required=True)
def rewind_replication_origin(origin, subscription, lsn):
    '''Rewind logical subscription to LSN. Very dangerous.'''
    src, dest = _ensure_connected()
    origin = ReplicationOrigins(src).get(origin)
    sub = Subscriptions(src, dest).get(subscription)

    if origin is None:
        print(Fore.GREEN,
              f'\bNo origin with name {name} exists.', Style.RESET_ALL)
    elif subscription is None:
        print(Fore.GREEN,
              f'\bNo subscription with name {name} exists.', Style.RESET_ALL)
    else:
        origin.rewind(lsn, sub)


@main.command()
@click.argument('name')
def reverse_subscription(name):
    '''Reverse the subscription. Source becomes destination, destination becomes source.
    Useful when primary becomes the replica and replica is promoted to primary.'''
    src, dest = _ensure_connected()
    sub = Subscriptions(src, dest).get(name)

    if sub is None:
        print(Fore.GREEN,
              f'\bNo subscription with name {name} exists.', Style.RESET_ALL)
    else:
        sub.reverse()


def _write_config(source, destination):
    with open('./.env', 'w') as file:
        file.write(f'SOURCE_DB_DSN={source}\n')
        file.write(f'DEST_DB_DSN={destination}\n')


@main.command()
@click.option('--source', '-s', help='DSN for the source database, i.e. the primary.', required=True)
@click.option('--destination', '-s', help='DSN for the destination database, i.e. the replica.', required=True)
def configure(source, destination):
    '''Write source and destination configuration. Saves it to .env file.'''
    _write_config(source, destination)


@main.command()
def reverse_configuration():
    '''Change source to destination and vice versa. Useful when debugging reversed subscriptions.'''
    src, dest = _ensure_connected()
    _write_config(dest.dsn, src.dsn)
    load_dotenv(override=True)
    src, dest = _ensure_connected()


@main.command()
@click.option('--source/--destination', help='List tables on the source or destination.', required=True)
def list_tables(source):
    '''List the tables on the source/destination.'''
    src, dest = _ensure_connected()

    if source:
        Tables(src).show()
    else:
        Tables(dest).show()


@main.command()
@click.argument('table_name')
@click.option('--source/--destination', help='List the columns on the source or destination table.', required=True)
def list_columns(table_name, source):
    '''List columns in a table. Specify source or destination if they are not in sync.'''
    src, dest = _ensure_connected()

    conn = src if source else dest
    conn_name = 'source' if source else 'destination'
    table = Tables(conn).get(table_name)

    if table is None:
        print(
            Fore.GREEN, f'\bNo table {table_name} exists on {conn_name}.', Style.RESET_ALL)
    else:
        Columns(conn, table).show()

@main.command()
def version():
    print(__version__)


if __name__ == '__main__':
    main()
