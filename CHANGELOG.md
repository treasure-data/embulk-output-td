## 0.3.15 - 2017-05-30

* [fix bug] Use underscores for sessions names to fix #70 [#71](https://github.com/treasure-data/embulk-output-td/pull/71)

## 0.3.14 - 2017-05-24

* [maintenance] Use UUID as postfix of generated bulk_import session name [#67](https://github.com/treasure-data/embulk-output-td/pull/67)

## 0.3.13 - 2017-03-03

* [maintenance] Not send perform and commit requests if no parts [#63](https://github.com/treasure-data/embulk-output-td/pull/63)

## 0.3.12 - 2016-11-30

* [maintenance] Upgrade td-client v0.7.29 [#61](https://github.com/treasure-data/embulk-output-td/pull/61)
* [maintenance] Change variable type of PluginTask.setTempDir() [#60](https://github.com/treasure-data/embulk-output-td/pull/60)

## 0.3.11 - 2016-11-18

* [maintenance] Refactor TdOutputPlugin, FieldWriterSet and else [#56](https://github.com/treasure-data/embulk-output-td/pull/56)

## 0.3.10 - 2016-11-16

* [maintenance] Setup bintray repo [#55](https://github.com/treasure-data/embulk-output-td/pull/55)

## 0.3.9 - 2016-10-26

* [maintenance] Add retry limit and interval configurations [#51](https://github.com/treasure-data/embulk-output-td/pull/51)

## 0.3.8 - 2016-08-15

* [maintenance] Change retry configuration [#50](https://github.com/treasure-data/embulk-output-td/pull/50)

## 0.3.7 - 2016-07-27

* [maintenance] Upgrade td-client v0.7.24 [#49](https://github.com/treasure-data/embulk-output-td/pull/49)

## 0.3.6 - 2016-07-11

* [new feature] Http proxy config from system properties [#47](https://github.com/treasure-data/embulk-output-td/pull/47)
* [maintenance] Remove redundant logging during run stage [#48](https://github.com/treasure-data/embulk-output-td/pull/48)

## 0.3.5 - 2016-06-29

* [new feature] Enable user/password for `http_proxy` option [#46](https://github.com/treasure-data/embulk-output-td/pull/46)

## 0.3.4 - 2016-06-23

* [maintenance] Upgrade td-client v0.7.22 [#45](https://github.com/treasure-data/embulk-output-td/pull/45)

## 0.3.3 - 2016-05-27

* [maintenance] Upgrade td-client v0.7.19 [#42](https://github.com/treasure-data/embulk-output-td/pull/42)

## 0.3.2 - 2016-05-09

* [maintenance] Get temporary directory path from System.property('java.io.tmpdir') [#39](https://github.com/treasure-data/embulk-output-td/pull/39)

## 0.3.1 - 2016-04-25

* [maintenance] Fix bug TransactionalPageOutput.flush method throws NullPointerException

## 0.3.0 - 2016-03-03

* [maintenance] Upgrade embulk v08 [#37](https://github.com/treasure-data/embulk-output-td/pull/37)

## 0.2.2 - 2016-02-29

* [maintenance] Update a table schema by Embulk's guessed columns if the # of the uploaded record is zero [#36](https://github.com/treasure-data/embulk-output-td/pull/36)
* [maintenance] replace and truncate modes create a new table if the table doesn't exists [#35](https://github.com/treasure-data/embulk-output-td/pull/35)

## 0.2.1 - 2016-01-28

* [new feature] Add truncate mode [#33](https://github.com/treasure-data/embulk-output-td/pull/33)

## 0.2.0 - 2016-01-12

* [new feature] Not use first timestamp column as primary key [#32](https://github.com/treasure-data/embulk-output-td/pull/32)

## 0.1.8 - 2016-01-09

* [new feature] Add mode to time value option  [#31](https://github.com/treasure-data/embulk-output-td/pull/31)
* [maintenance] Remove unnecessary warning messages within showErrorRecord method [#30](https://github.com/treasure-data/embulk-output-td/pull/30)

## 0.1.7 - 2016-01-07

* [new feature] Add time_value option  [#16](https://github.com/treasure-data/embulk-output-td/pull/16)
* [new feature] Merge exact column types into the table schema [#25](https://github.com/treasure-data/embulk-output-td/pull/25)
* [new feature]	Add stop_on_invalid_record option [#26](https://github.com/treasure-data/embulk-output-td/pull/26)
* [new feature] Show skipped records by a perform job [#28](https://github.com/treasure-data/embulk-output-td/pull/28)
* [maintenance] Use rename endpoint with 'overwrite' parameter [#23](https://github.com/treasure-data/embulk-output-td/pull/23)

## 0.1.6 - 2015-12-08

* [maintenance] Upgrade Embulk v0.7.10 [#22](https://github.com/treasure-data/embulk-output-td/pull/22)
* [maintenance] Upgrade Embulk v0.7.5 [#21](https://github.com/treasure-data/embulk-output-td/pull/21)
* [new feature] Add default_timestamp_type_convert_to option [#18](https://github.com/treasure-data/embulk-output-td/pull/18)

## 0.1.5 - 2015-09-29

* [new feature] Add replace mode [#17](https://github.com/treasure-data/embulk-output-td/pull/17)

## 0.1.4 - 2015-08-17

* [maintenance] Upgrade Embulk v0.6.25
* [fixed] Change a field writer for 'time' column [#13](https://github.com/treasure-data/embulk-output-td/pull/13)


## 0.1.3 - 2015-08-05

* [maintenance] Upgrade Embulk v0.6.19
* [new feature] Add column_options [#11](https://github.com/treasure-data/embulk-output-td/pull/11)

## 0.1.2 - 2015-07-14

## 0.1.1 - 2015-07-14

* [maintenance] Make part name unique and idempotent [#9](https://github.com/treasure-data/embulk-output-td/pull/9)
* [maintenance] Delete temp files after uploading [#7](https://github.com/treasure-data/embulk-output-td/pull/7)
* [new feature] Add unix_timestamp_unit option [#6](https://github.com/treasure-data/embulk-output-td/pull/6)

## 0.1.0 - 2015-06-23

The first release!!
