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

fn print_metedata(filename: &str) -> Result<(), std::io::Error> {
    let reader_options = ReadOptionsBuilder::new().with_page_index().build();
    let file = File::open(filename)?;

    let reader = SerializedFileReader::new_with_options(file, reader_options).unwrap();
    let metadata = reader.metadata();

    println!("{} Meta Data:", filename);
    printer::print_parquet_metadata(&mut std::io::stdout(), metadata);

    Ok(())
}

fn main() {
    let args = env::args().collect::<Vec<String>>();
    if args.len() < 2 {
        usage(args[1].as_str())
    }
    for arg in &args[1..] {
        if let Err(why) = print_metedata(arg) {
            println!("Can't open {} : {}", arg, why);
        }
    }
}
