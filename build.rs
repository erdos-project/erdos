use std::{
    env,
    fs::File,
    path::Path,
    process::{Command, Stdio},
    sync::Mutex,
};

use slog::{Drain, Logger};

static DEFAULT_BUNDLE_MAX_READ_STREAMS: usize = 20;
static DEAFUALT_BUNDLE_MAX_WRITE_STREAMS: usize = 10;

/// Parses an environment variable and falls back to the default if it is not set.
fn parse_env_variable<T: std::str::FromStr + std::fmt::Display>(
    key: &str,
    default: T,
    logger: &Logger,
) -> T
where
    <T as std::str::FromStr>::Err: std::fmt::Debug,
{
    match env::var(key) {
        Ok(s) => s
            .parse()
            .expect(&format!("Error parsing environment variable {}.", key)),
        Err(env::VarError::NotPresent) => {
            slog::info!(logger, "{} not set. Defaulting to {}", key, default,);
            default
        }
        Err(env::VarError::NotUnicode(_)) => panic!("Error decoding {}.", key),
    }
}

fn make_callback_builder(max_read_streams: usize, max_write_streams: usize) -> Result<(), String> {
    let out_dir = env::var("OUT_DIR").unwrap();
    let out_file_path = Path::new(&out_dir).join("callback_builder_generated.rs");

    let mut script = env::current_dir().unwrap();
    script.push("scripts");
    script.push("make_callback_builder.py");

    let file = File::create(out_file_path.to_str().unwrap()).map_err(|e| {
        format!(
            "Error creating file {}: {}",
            out_file_path.to_str().unwrap(),
            e.to_string()
        )
    })?;

    let child = Command::new("python3")
        .arg(script.to_str().unwrap())
        .args(&[max_read_streams.to_string(), max_write_streams.to_string()])
        .stdout(Stdio::from(file))
        .spawn()
        .map_err(|e| {
            format!(
                "Error running {}: {}",
                script.to_str().unwrap(),
                e.to_string()
            )
        })?;

    let output = child.wait_with_output().map_err(|e| {
        format!(
            "Error running {}: {}",
            script.to_str().unwrap(),
            e.to_string()
        )
    })?;

    if !output.status.success() {
        return Err(format!(
            "Error running {}: {}",
            script.to_str().unwrap(),
            String::from_utf8(output.stderr).unwrap_or("".to_string())
        ));
    }

    Ok(())
}

fn make_add_watermark_callback(
    max_read_streams: usize,
    max_write_streams: usize,
) -> Result<(), String> {
    let out_dir = env::var("OUT_DIR").unwrap();
    let out_file_path = Path::new(&out_dir).join("add_watermark_callback_vec_generated.rs");

    let mut script = env::current_dir().unwrap();
    script.push("scripts");
    script.push("make_add_watermark_callback_vec.py");

    let file = File::create(out_file_path.to_str().unwrap()).map_err(|e| {
        format!(
            "Error creating file {}: {}",
            out_file_path.to_str().unwrap(),
            e.to_string()
        )
    })?;

    let child = Command::new("python3")
        .arg(script.to_str().unwrap())
        .args(&[max_read_streams.to_string(), max_write_streams.to_string()])
        .stdout(Stdio::from(file))
        .spawn()
        .map_err(|e| {
            format!(
                "Error running {}: {}",
                script.to_str().unwrap(),
                e.to_string()
            )
        })?;

    let output = child.wait_with_output().map_err(|e| {
        format!(
            "Error running {}: {}",
            script.to_str().unwrap(),
            e.to_string()
        )
    })?;

    if !output.status.success() {
        return Err(format!(
            "Error running {}: {}",
            script.to_str().unwrap(),
            String::from_utf8(output.stderr).unwrap_or("".to_string())
        ));
    }

    Ok(())
}

fn main() -> Result<(), String> {
    let logger = Logger::root(Mutex::new(slog_term::term_full()).fuse(), slog::o!());
    let bundle_max_read_streams: usize = parse_env_variable(
        "ERDOS_BUNDLE_MAX_READ_STREAMS",
        DEFAULT_BUNDLE_MAX_READ_STREAMS,
        &logger,
    );
    let bundle_max_write_streams: usize = parse_env_variable(
        "ERDOS_BUNDLE_MAX_WRITE_STREAMS",
        DEAFUALT_BUNDLE_MAX_WRITE_STREAMS,
        &logger,
    );

    slog::info!(logger, "Generating code for stream bundles.");
    make_callback_builder(bundle_max_read_streams, bundle_max_write_streams)?;
    slog::info!(logger, "Done generating code for stream bundles.");

    slog::info!(
        logger,
        "Generating code for adding callbacks over vectors of streams."
    );
    make_add_watermark_callback(bundle_max_read_streams, bundle_max_write_streams)?;
    slog::info!(
        logger,
        "Done generating code for adding callbacks over vectors of streams."
    );

    // Re-run build.rs if the following files are changed.
    println!("cargo:rerun-if-changed=build.rs");
    println!("cargo:rerun-if-changed=scripts/make_callback_builder.py");
    println!("cargo:rerun-if-changed=scripts/make_add_watermark_callback_vec.py");

    // Re-run build.rs if the following environment variables are changed.
    println!("cargo:rerun-if-env-changed=ERDOS_BUNDLE_MAX_READ_STREAMS");
    println!("cargo:rerun-if-env-changed=ERDOS_BUNDLE_MAX_WRITE_STREAMS");

    Ok(())
}
