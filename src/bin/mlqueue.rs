fn main() {
    if let Err(err) = mlqueue::cli::main() {
        eprintln!("error: {err:#}");
        std::process::exit(1);
    }
}
