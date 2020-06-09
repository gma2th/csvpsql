use csvpsql::Opt;
use std::process;
use structopt::StructOpt;

fn main() {
    let opt = Opt::from_args();
    if let Err(err) = csvpsql::run(opt) {
        println!("{}", err);
        process::exit(1);
    }
}
