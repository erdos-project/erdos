use std::{
    env,
    fs::File,
    path::Path,
    process::{Command, Stdio},
};

fn make_callback_builder() -> Result<(), String> {
    let out_dir = env::var("OUT_DIR").unwrap();
    let callback_builder_path = Path::new(&out_dir).join("callback_builder_generated.rs");

    let mut callback_builder_script = env::current_dir().unwrap();
    callback_builder_script.push("scripts");
    callback_builder_script.push("make_callback_builder.py");

    let callback_builder_file = File::create(callback_builder_path.to_str().unwrap())
        .map_err(|e| format!("make_callback_builder: {}", e.to_string()))?;

    let child = Command::new("python3")
        .arg(callback_builder_script.to_str().unwrap())
        .args(&["build", "8", "8"])
        .stdout(Stdio::from(callback_builder_file))
        .spawn()
        .map_err(|e| format!("make_callback_builder: {}", e.to_string()))?;

    let output = child
        .wait_with_output()
        .map_err(|e| format!("make_callback_builder: {}", e.to_string()))?;

    if !output.status.success() {
        return Err(format!(
            "make_callback_builder: {}",
            String::from_utf8(output.stderr)
                .unwrap_or("failed to run `scripts/make_callback_builder.py`".to_string())
        ));
    }

    Ok(())
}

fn main() -> Result<(), String> {
    make_callback_builder()?;

    Ok(())
}
