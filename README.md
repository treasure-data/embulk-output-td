# TD output plugin for Embulk

TODO: Write short description here

## Overview

* **Plugin type**: output
* **Load all or nothing**: yes
* **Resume supported**: no

## Configuration

- **apikey**: apikey (string, required)
- **endpoint**: hostname (string, default='api.treasuredata.com')
- **http_proxy**: http proxy configuration (tuple of host, port and useSsl, default is null)
- **use_ssl**: the flag (boolean, default=true)
- **auto_create_table**: the flag for creating the database and/or the table if they don't exist (boolean, default=true)
- **database**: database name (string, required)
- **table**: table name (string, required)
- **session**: bulk_import session name (string, optional)
- **time_column**: user-defined time column (string, optional)
- **unix_timestamp_unit**: if type of "time" or **time_column** is long, it's considered unix timestamp. This option specify its unit in sec, milli, micro or nano (enum, default: `sec`)
- **tmpdir**: temporal directory
- **upload_concurrency**: upload concurrency (int, default=2). max concurrency is 8.
- **file_split_size**: split size (long, default=16384 (16MB)).

## Example
Here is sample configuration for TD output plugin.
```yaml
out:
  type: td
  apikey: <your apikey>
  endpoint: api.treasuredata.com
  database: my_db
  table: my_table
  time_column: created_at
```

### Http Proxy Configuration
If you want to add your Http Proxy configuration, you can use `http_proxy` parameter:
```yaml
out:
  type: td
  apikey: <your apikey>
  endpoint: api.treasuredata.com
  http_proxy: {host: localhost, port: 8080, use_ssl: false}
  database: my_db
  table: my_table
  time_column: created_at
```



## Build

### Build by Gradle
```
$ git clone https://github.com/treasure-data/embulk-output-td.git
$ cd embulk-output-td
$ ./gradlew gem classpath
```

### Run on Embulk
$ bin/embulk run -I embulk-output-td/lib/ config.yml
