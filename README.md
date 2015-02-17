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
  api_server_url: https://api.treasuredata.com:80/
```

## Build

```
$ ./gradlew gem
```
