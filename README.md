# TD output plugin for Embulk

TODO: Write short description here

## Overview

* **Plugin type**: output
* **Load all or nothing**: yes
* **Resume supported**: no

## Configuration

- **endpoint**: hostname (string, required)
- **apikey**: apikey (string, required)
- **session**: bulk_import session name (string, optional)
- **database**: database name (string, required)
- **table**: table name (string, required)
- **auto_create_table**: the flag for creating the database and/or the table if they don't exist (boolean, default=true)
- **use_ssl**: the flag (boolean, default=true)

## Example

```yaml
out:
  type: td
  endpoint: api.treasuredata.com
  apikey: <your apikey>
  session: <session name>
  database: <db name>
  table: <table name>
```

## Build

```
$ ./gradlew gem
```
