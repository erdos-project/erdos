use erdos::Configuration;

/// Returns a unique port for each test to avoid race conditions.
fn get_unique_port() -> usize {
    use std::sync::atomic::{AtomicUsize, Ordering};
    static PORT: AtomicUsize = AtomicUsize::new(9000);
    PORT.fetch_add(1, Ordering::SeqCst)
}

pub fn make_default_config() -> Configuration {
    let data_addresses = vec![format!("127.0.0.1:{}", get_unique_port())
        .parse()
        .expect("Unable to parse socket address")];
    let control_addresses = vec![format!("127.0.0.1:{}", get_unique_port())
        .parse()
        .expect("Unable to parse socket address")];
    Configuration::new(0, data_addresses, control_addresses, 4, None)
}
