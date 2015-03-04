# TD output plugin for Embulk

TODO: Write short description here

## Overview

* **Plugin type**: output
* **Load all or nothing**: yes
* **Resume supported**: no

## Configuration

- **api_server_url**: url (string, required)

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
