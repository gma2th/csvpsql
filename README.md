# csvpsql

`csvpsql` generate Postgres table from csv file.

## Installation

```bash
cargo install csvpsql
```

## Usage

```bash
USAGE:
    csvpsql [FLAGS] [OPTIONS] --table-name <table-name> [file]

FLAGS:
        --drop          To drop the table if exists
    -h, --help          Prints help information
        --no-copy       To remove copy command
        --no-header     Whenever the csv file has no header
    -V, --version       Prints version information

OPTIONS:
    -c, --columns <columns>          Override column name. Separated by comma. Use the csv header or letters by default.
    -d, --delimiter <delimiter>       [default: ,]
    -n, --null-as <null-as>          Empty string are null by default [default: ]
    -t, --table-name <table-name>    File name is used as default

ARGS:
    <file>
```

## Example

```bash
$ csvpsql --drop example.csv
drop table if exists example;

create table example (
   city text not null,
   region text not null,
   country text not null,
   population integer not null
);

\copy example from 'example.csv' with csv delimiter ',' header;
```
