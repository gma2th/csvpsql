# csvpsql

`csvpsql` generate Postgres table from csv file.

## Installation

```bash
cargo install csvpsql
```

## Usage

```bash
USAGE:
csvpsql [FLAGS] [OPTIONS] [file]

FLAGS:
        --help         Prints help information
    -h, --no-header
    -V, --version      Prints version information

OPTIONS:
    -d, --delimiter <delimiter>     [default: ,]
    -n, --null-as <null-as>        Empty string are null by default [default: ]

ARGS:
    <file>
```

## Example

```bash
$ csvpsql example.csv
create table example (
   city text not null,
   region text not null,
   country text not null,
   population integer not null
);
```
