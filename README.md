[![Build Status](https://travis-ci.com/levkk/pg-logical-manager.svg?branch=master)](https://travis-ci.com/levkk/pg-logical-manager)

# pg-logical-manager
Manage logical replication for your PostgreSQL cluster. Simply create/drop/enable/disable/list subscriptions.

Includes other more risky but interesting abilities like:
1. rewinding subscriptions, i.e. moving back and forth between LSNs,
2. creating reverse subscriptions, i.e. send data from replica to primary instead after replica promotion.

[Pg Logical Manager Demo](https://i.imgur.com/bXpufEm.gif)

## Setup

## Pypi (production)

Simply `pip install pglogicalmanager`.

### Virtual environment (development)

1. `pip install virtualenv`
2. `virtualenv venv --python=python3`
3. `source .venv/bin/activate`
4. `pip install -e ".[dev]"`

When developing inside the virtual environment, use

```bash
$ python -m pglogicalmanager
```

instead of

```bash
$ pglogicalmanager
```

for all commands.

### Configuration

```bash
$ pglogicalmanager --source=postgres://user:password@primary-db:5432/database --destination=postgres://user:password@replica-db:5432/database
```

This will write a `.env` file in the same current folder. It will contain the DSNs above.

### Make sure it works

```bash
$ pglogicalmanager list-subscriptions
```

## Usage

Check out the help menu:

```bash
$ pglogicalmanager --help
```

## Features

### Basic features

You can easily list, create, drop, disable, and enable subscriptions. These sit directly on top of Postgres primitives (i.e. `CREATE SUBSCRIPTION`, `DROP SUBSCRIPTION`, etc.) and are fairly well-known. You can also list tables in source/destination and list columns in those tables.

### Advanced (read risky) features

Logical replication is powerful and flexible, and it allows you to do things binary replication can't do. Features we found useful and which are implemented here are:

1. rewind subscription to specific LSN,
2. reverse subscriptions,
3. manually create/drop replication slots.

#### Rewind subscription

Rewinding a subscription makes it replicate from a paritcular point-in-time. This works like `pg_rewind` except on a live cluster and without changing the WAL timeline. Note: _this is pretty dangerous_. If you rewind it to a wrong spot, you could create conflicts (unique contraint violations, for example) and the replication can break.

```bash
$ pglogicalmanager list-replication-origins
$ pglogicalmanager rewind-replication-origin --help
```

TODO: Document use cases.

#### Reverse subscription

Reversing a subscription is switching roles between the primary and the replica: the replica becomes the primary and the primary becomes the replica. This makes sense if you are promoting the replica to become the new primary and you want the old primary to be kept around for backup/rollback purposes. This is not as risky as rewinding, but it is irreverisble: once done, the replica must be the source for all writes, otherwise a split brain situation will be created.


```bash
$ pglogicalmanager reverse-subscription --help
```

This will also overwrite your `.env` configuration and change the source DSN to the destination DSN and vice versa.

#### Manually creating replication slots

Creating replication slots is useful to tell your source (primary) to preserve WAL segments from the point of creation of the slot. The inheritent danger is running out of space on write-heavy systems, since WAL segments won't be cleaned up, and busy servers write a lot of WAL!

```bash
$ pglogicalmanager create-replication-slot test_slot
```

and see all current replication slots with

```bash
$ pglogicalmanager list-replication-slots
```

TODO: Document use cases.
