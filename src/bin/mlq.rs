fn main() {
    // A CLI piped into `head`/`less` should end quietly on a closed pipe,
    // not panic: restore the default SIGPIPE disposition Rust masks.
    unsafe {
        libc::signal(libc::SIGPIPE, libc::SIG_DFL);
    }
    if let Err(err) = mlqueue::cli::main() {
        eprintln!("error: {err:#}");
        std::process::exit(1);
    }
}
