use apalis_core::backend::factory::BackendFactory;

use crate::{Config, Connection, RedbStorage, error::Error};

/// A shared redb storage that can create multiple [`RedbStorage`] instances.
///
/// All storages created by the factory share one database and wake each
/// other's workers when tasks are pushed.
#[derive(Debug, Clone)]
pub struct RedbStorageFactory {
    conn: Connection,
}

impl RedbStorageFactory {
    /// Creates a new factory sharing the given connection.
    pub async fn new(conn: Connection) -> Result<Self, Error> {
        Ok(RedbStorageFactory { conn })
    }
}

impl<Args> BackendFactory<Args> for RedbStorageFactory {
    type Backend = RedbStorage<Args>;

    type Error = Error;

    fn create(&mut self) -> Result<Self::Backend, Self::Error> {
        let config = Config::default().queue(std::any::type_name::<Args>());
        Self::create_with_config(self, config)
    }

    fn create_with_config(&mut self, config: Config) -> Result<Self::Backend, Self::Error> {
        Ok(RedbStorage::new(self.conn.clone()).with_config(config))
    }
}
