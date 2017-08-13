# Column As Json Key filter plugin for Embulk

An Embulk filter plugin to add a column to another JSON column as a new key.

## Overview

* **Plugin type**: filter

## Configuration

- **column**: column name of JSON (string, required)
- **column_to_be_added**: column name to be added to JSON (required)
- **path**: key path to add column (string, optional, default is `$`)

## Example

input:

```tsv
json_payload	age
{"name": "John"}	23
{"name": "David"}	34
```

yaml:

```yaml
filters:
  - type: column_as_json_key
    column: json_payload
    column_to_be_added: age
```

output:

```json
{"name":"John","age":23},23
{"name":"David","age":34},34
```

See also [example.tsv](./example/example.tsv) and [example.yml](./example/example.yml).

## Build

```
$ ./gradlew gem  # -t to watch change of files and rebuild continuously
```
