//! `csvpsql` generate Postgres table from csv file.
//!
//! # Installation
//!
//! ```bash
//! cargo install csvpsql
//! ```
//!
//! # Usage
//!
//! ```bash
//! USAGE:
//! csvpsql [FLAGS] [OPTIONS] [file]
//!
//! FLAGS:
//!         --help         Prints help information
//!     -h, --no-header
//!     -V, --version      Prints version information
//!
//! OPTIONS:
//!     -d, --delimiter <delimiter>     [default: ,]
//!     -n, --null-as <null-as>        Empty string are null by default [default: ]
//!
//! ARGS:
//!     <file>
//! ```
//!
//! # Example
//!
//! ```bash
//! $ csvpsql example.csv
//! create table test (
//!    city text not null,
//!    region text not null,
//!    country text not null,
//!    population integer not null
//!);
//! ```

use chrono::NaiveTime;
use itertools::izip;
use std::error::Error;
use std::fmt;
use std::fs;
use std::io;
use std::io::{BufRead, BufReader};
use std::path::PathBuf;
use structopt::StructOpt;

#[derive(Debug, StructOpt)]
#[structopt(name = "csvpsql", about = "Parse csv to sql tables.")]
pub struct Opt {
    #[structopt(short = "h", long)]
    pub no_header: bool,

    #[structopt(short, long, default_value = ",")]
    pub delimiter: char,

    #[structopt(
        short,
        long,
        default_value = "",
        help = "Empty string are null by default"
    )]
    pub null_as: String,

    #[structopt(parse(from_os_str))]
    pub file: Option<PathBuf>,
}

// TODO: Add missing column types
#[derive(Debug, Clone, PartialEq, PartialOrd)]
enum ColumnType {
    Unknown,
    Boolean,
    Integer,
    Double,
    Date,
    Timestamp,
    Text,
}

impl fmt::Display for ColumnType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", format!("{:?}", self).to_lowercase())
    }
}

#[derive(Debug, Clone, PartialEq, PartialOrd)]
enum ColumnConstraint {
    Nullable,
    NotNull,
}

impl fmt::Display for ColumnConstraint {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match *self {
            ColumnConstraint::NotNull => write!(f, "not null"),
            ColumnConstraint::Nullable => write!(f, ""),
        }
    }
}

struct Column {
    name: String,
    ctype: ColumnType,
    constraint: ColumnConstraint,
}

impl fmt::Display for Column {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "    {} {} {}",
            self.name.replace(" ", "_"),
            self.ctype,
            self.constraint,
        )
    }
}

type Columns = Vec<Column>;

struct Table {
    name: String,
    columns: Columns,
}

impl fmt::Display for Table {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "create table {} (", self.name)?;
        for column in &self.columns[0..self.columns.len() - 1] {
            writeln!(f, "    {},", column)?;
        }
        writeln!(f, "    {}", self.columns[self.columns.len() - 1])?;
        writeln!(f, ");")?;
        Ok(())
    }
}

fn try_parse_date(field: &str) -> Result<ColumnType, dtparse::ParseError> {
    let (date, _) = dtparse::parse(field)?;
    if date.time() == NaiveTime::from_hms(0, 0, 0) {
        Ok(ColumnType::Date)
    } else {
        Ok(ColumnType::Timestamp)
    }
}

fn find_type(field: &str) -> ColumnType {
    if [String::from("true"), String::from("false")].contains(&field.to_lowercase()) {
        return ColumnType::Boolean;
    }
    if field.parse::<isize>().is_ok() {
        return ColumnType::Integer;
    }
    if field.parse::<f64>().is_ok() {
        return ColumnType::Double;
    }
    if let Ok(c) = try_parse_date(field) {
        return c;
    }
    ColumnType::Text
}

fn find_constraint(field: &str, null_as: &str) -> ColumnConstraint {
    if field == null_as {
        ColumnConstraint::Nullable
    } else {
        ColumnConstraint::NotNull
    }
}

pub fn run(opt: Opt) -> Result<(), Box<dyn Error>> {
    let reader: Box<dyn BufRead> = match opt.file {
        None => Box::new(BufReader::new(io::stdin())),
        Some(filename) => Box::new(BufReader::new(fs::File::open(filename).unwrap())),
    };
    let mut rdr = csv::ReaderBuilder::new()
        .has_headers(!opt.no_header)
        .delimiter(opt.delimiter as u8)
        .from_reader(reader);

    let mut column_types = vec![ColumnType::Unknown; rdr.headers()?.len()];
    let mut column_constraints = vec![ColumnConstraint::Nullable; rdr.headers()?.len()];
    for result in rdr.records() {
        let record = result?;
        for (i, field) in record.iter().enumerate() {
            let field_type = find_type(field);
            if field_type > column_types[i] {
                column_types[i] = field_type
            }
            let field_constraint = find_constraint(field, &opt.null_as);
            if field_constraint > column_constraints[i] {
                column_constraints[i] = field_constraint
            }
        }
    }

    let headers: Vec<&str> = match opt.no_header {
        false => rdr.headers()?.iter().collect(),
        true => "a,b,c,d,e,f,g,h,i,j,k,l,m,n,o,p,q,r,s,t,u,v,w,x,y,z"
            .split(",")
            .take(rdr.headers()?.len())
            .collect(),
    };

    let columns: Columns = izip!(headers, column_types, column_constraints)
        .map(|(name, ctype, constraint)| Column {
            name: name.to_owned(),
            ctype,
            constraint,
        })
        .collect();

    let table_name = String::from("test");
    let table = Table {
        name: table_name,
        columns,
    };

    println!("{}", table);

    Ok(())
}

mod test {
    #[allow(unused)]
    use super::*;

    #[test]
    fn test_find_type() {
        assert_eq!(find_type("true"), ColumnType::Boolean);
        assert_eq!(find_type("false"), ColumnType::Boolean);
        assert_eq!(find_type("TRUE"), ColumnType::Boolean);
        assert_eq!(find_type("0"), ColumnType::Integer);
        assert_eq!(find_type("0.0"), ColumnType::Double);
    }

    #[test]
    fn test_parse_date() {
        assert_eq!(try_parse_date("2020-01-01"), Ok(ColumnType::Date));
        assert_eq!(
            try_parse_date("2020-01-01 18:30:04 +02:00"),
            Ok(ColumnType::Timestamp)
        );
    }

    #[test]
    fn test_find_constraint() {
        assert_eq!(find_constraint("", ""), ColumnConstraint::Nullable);
        assert_eq!(find_constraint("smth", ""), ColumnConstraint::NotNull);
    }
}