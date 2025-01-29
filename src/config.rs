use redis::{ConnectionInfo, IntoConnectionInfo};

#[derive(Clone, Default)]
pub struct Config {
    pub initial_nodes: Vec<ConnectionInfo>,
    pub client_id: String,
    pub cluster: Option<bool>,
    pub max_size: u32,
    pub max_delay: Option<u64>, // ms
    pub shards: bool,
    pub bb8: bool,
    pub mock: bool,
}

impl Config {
    pub fn set_nodes<T>(&mut self, nodes: Vec<T>) -> Result<(), redis::RedisError>
    where
        T: IntoConnectionInfo,
    {
        for node in nodes.into_iter() {
            self.initial_nodes.push(node.into_connection_info()?);
        }
        Ok(())
    }

    pub fn set_features(&mut self, features: &[String]) {
        for feature in features.iter() {
            match feature.to_ascii_lowercase().as_str() {
                "shards" => self.shards = true,
                "bb8" => self.bb8 = true,
                "mock" | "inmemory" => self.mock = true,
                _ => continue,
            }
        }
    }
}
