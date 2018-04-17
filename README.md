# TD output plugin for Embulk

[Treasure Data Service](https://www.treasuredata.com/) output plugin for [Embulk](https://github.com/embulk/embulk)

**NOTICE**:
  * embulk-output-td v0.5.0+ requires Java 1.8 or higher. For Java7, use embulk-output-td v0.4.x
  * embulk-output-td v0.4.0+ only supports **Embulk v0.8.22+**.

## Overview

* **Plugin type**: output
* **Load all or nothing**: yes
* **Resume supported**: no

## Configuration

- **apikey**: apikey (string, required)
- **endpoint**: hostname (string, default='api.treasuredata.com')
- **http_proxy**: http proxy configuration (tuple of host, port, useSsl, user, and password. default is null)
- **use_ssl**: the flag (boolean, default=true)
- **auto_create_table**: the flag for creating the database and/or the table if they don't exist (boolean, default=true)
- **mode**: 'append', 'replace' and 'truncate' (string, default='append')
- **database**: database name (string, required)
- **table**: table name (string, required)
- **session**: bulk_import session name (string, optional)
- **pool_name**: bulk_import session pool name (string, optional)
- **time_column**: user-defined time column (string, optional)
- **unix_timestamp_unit**: if type of "time" or **time_column** is long, it's considered unix timestamp. This option specify its unit in sec, milli, micro or nano (enum, default: `sec`)
- **tmpdir**: temporal directory (string, optional) if set to null, plugin will use directory that could get from System.property
- **upload_concurrency**: upload concurrency (int, default=2). max concurrency is 8.
- **file_split_size**: split size (long, default=16384 (16MB)).
- **stop_on_invalid_record**: stop bulk load transaction if a file includes invalid record (such as invalid timestamp) (boolean, default=false).
- **displayed_error_records_count_limit**: limit the count of the shown error records skipped by the perform job (int, default=10).
- **default_timestamp_type_convert_to**: configure output type of timestamp columns. Available options are "sec" (convert timestamp to UNIX timestamp in seconds) and "string" (convert timestamp to string). (string, default: `"string"`)
- **default_timezone**: default timezone (string, default='UTC')
- **default_timestamp_format**: default timestamp format (string, default=`%Y-%m-%d %H:%M:%S.%6N`)
- **column_options**: advanced: a key-value pairs where key is a column name and value is options for the column.
  - **timezone**: If input column type (embulk type) is timestamp, this plugin needs to format the timestamp value into a SQL string. In this cases, this timezone option is used to control the timezone. (string, value of default_timezone option is used by default)
  - **format**: If input column type (embulk type) is timestamp, this plugin needs to format the timestamp value into a string. This timestamp_format option is used to control the format of the timestamp. (string, value of default_timestamp_format option is used by default)
- **retry_limit**: indicates how many retries are allowed (int, default: 20)
- **retry_initial_interval_millis**: the initial intervals (int, default: 1000)
- **retry_max_interval_millis**: the maximum intervals. The interval doubles every retry until retry_max_interval_millis is reached. (int, default: 90000)

## Modes
* **append**:
  - Uploads data to existing table directly.
* **replace**:
  - Creates new temp table and uploads data to the temp table first.
  - After uploading finished, the table specified as 'table' option is replaced with the temp table.
  - Schema in existing table is not migrated to the replaced table.
* **truncate**:
  - Creates new temp table and uploads data to the temp table first.
  - After uploading finished, the table specified as 'table' option is replaced with the temp table.
  - Schema in existing table is added to the replaced table.

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
  auto_create_table: true
  mode: append
```

## Install

```
$ embulk gem install embulk-output-td
```

### Http Proxy Configuration
If you want to add your Http Proxy configuration, you can use `http_proxy` parameter:
```yaml
out:
  type: td
  apikey: <your apikey>
  endpoint: api.treasuredata.com
  http_proxy: {host: localhost, port: 8080, use_ssl: false, user: "proxyuser", password: "PASSWORD"}
  database: my_db
  table: my_table
  time_column: created_at
  auto_create_table: true
  mode: append
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

## Release

### Upload gem to Rubygems.org

```
$ ./gradlew gem     # create .gem file under pkg/ directory
$ ./gradlew gemPush # create and publish .gem file
```

Repo URL: https://rubygems.org/gems/embulk-output-td

### Upload jars to Bintray.com

```
$ ./gradlew bintrayUpload
```

Repo URL: https://bintray.com/embulk-output-td/maven/embulk-output-td
