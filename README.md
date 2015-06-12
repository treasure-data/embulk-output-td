# TD output plugin for Embulk

TODO: Write short description here

## Overview

* **Plugin type**: output
* **Load all or nothing**: yes
* **Resume supported**: no

## Configuration

- **apikey**: apikey (string, required)
- **endpoint**: hostname (string, required)
- **use_ssl**: the flag (boolean, default=true)
- **auto_create_table**: the flag for creating the database and/or the table if they don't exist (boolean, default=true)
- **database**: database name (string, required)
- **table**: table name (string, required)
- **session**: bulk_import session name (string, optional)
- **tmpdir**: temporal directory

## Example

```yaml
out:
  type: td
  apikey: <your apikey>
  endpoint: api.treasuredata.com
  database: <db name>
  table: <table name>
```

## Build

```
$ ./gradlew gem
```
