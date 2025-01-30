use redis::{
    cluster::ClusterClient, cluster_async::ClusterConnection, ErrorKind, IntoConnectionInfo,
    RedisError,
};

pub struct ClusterManager {
    pub(crate) client: ClusterClient,
}

impl ClusterManager {
    pub fn new<T>(initial_nodes: Vec<T>) -> Result<Self, RedisError>
    where
        T: IntoConnectionInfo,
    {
        let client = ClusterClient::new(initial_nodes)?;
        Ok(Self { client })
    }
}

impl bb8::ManageConnection for ClusterManager {
    type Connection = ClusterConnection;
    type Error = RedisError;

    async fn connect(&self) -> Result<Self::Connection, Self::Error> {
        self.client.get_async_connection().await
    }

    async fn is_valid(&self, conn: &mut Self::Connection) -> Result<(), Self::Error> {
        let pong: String = redis::cmd("PING").query_async(conn).await?;
        match pong.as_str() {
            "PONG" => Ok(()),
            _ => Err((ErrorKind::ResponseError, "ping request").into()),
        }
    }

    fn has_broken(&self, _: &mut Self::Connection) -> bool {
        false
    }
}
