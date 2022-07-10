use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct Resources {
    num_cpus: usize,
    num_gpus: usize,
}

impl Resources {
    pub fn new(num_cpus: usize, num_gpus: usize) -> Self {
        Self { num_cpus, num_gpus }
    }

    pub fn empty() -> Self {
        Resources::new(0, 0)
    }
}
