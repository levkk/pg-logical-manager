# pg-logical-manager
Manage PostgreSQL logical replication

## Local setup
To test this:

0. Have PostgreSQL 10 running or higher.
1. Create databases "src"
2. Create database "dest"
3. Make sure you have local authentication working with no password (see hardcoded connection strings)

Basically, make sure this works:

```python
src = psycopg2.connect('postgres://localhost:5432/src')
dest = psycopg2.connect('postgres://localhost:5432/dest')
```

## Usage

Use the menu:

```bash
$ pip3 install requirements.txt
$ python3 manager.py --help
```