use erdos::Configuration;

pub fn make_default_config() -> Configuration {
    let data_addresses = vec!["127.0.0.1:9000"
    .parse()
    .expect("Unable to parse socket address")];
    let control_addresses = vec!["127.0.0.1:9001"
    .parse()
    .expect("Unable to parse socket address")];
    Configuration::new(0, data_addresses, control_addresses, 4)
}