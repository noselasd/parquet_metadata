use parquet::file::reader::FileReader;
use parquet::file::serialized_reader::ReadOptionsBuilder;
use parquet::file::serialized_reader::SerializedFileReader;
use parquet::schema::printer;
use std::env;
use std::fs::File;

fn usage(progname: &str) {
    println!("Usage: {}  parquet-file1 parquet-file2 ...", progname);
    println!("Prints out parquet metadata for each file");

    std::process::exit(1);
}

fn print_metedata(filename: &str) {
    let reader_options = ReadOptionsBuilder::new().with_page_index().build();
    let file = match File::open(filename) {
        Ok(f) => f,
        Err(why) => {
            println!("Can't open {} : {}", filename, why);
            return;
        }
    };
    let reader = SerializedFileReader::new_with_options(file, reader_options).unwrap();
    let metadata = reader.metadata();

    println!("{} Meta Data:", filename);
    printer::print_parquet_metadata(&mut std::io::stdout(), metadata);
}

fn main() {
    let args = env::args().collect::<Vec<String>>();
    if args.len() < 2 {
        usage(args[1].as_str())
    }
    for arg in &args[1..] {
        print_metedata(arg);
    }
}
