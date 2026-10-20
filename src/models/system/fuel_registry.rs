use anyhow::Context;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use sui_pg_db::Connection;

use diesel_async::RunQueryDsl;

use crate::models::world::StoredFuelConfig;

/// Default fuel efficiency used when no on-chain config has been indexed yet.
const DEFAULT_FUEL_EFFICIENCY: i64 = 10;

pub struct FuelRegistry {
    cache: RwLock<HashMap<i64, Arc<StoredFuelConfig>>>,
}

impl FuelRegistry {
    pub async fn load_from_db(conn: &mut Connection<'_>) -> anyhow::Result<Self> {
        use crate::schema::fuel_config::dsl::*;

        let records = fuel_config
            .load::<StoredFuelConfig>(conn)
            .await
            .context("Failed to fetch records from fuel registry")?;

        let mut cache = HashMap::new();

        for record in records {
            cache.insert(record.type_id, Arc::new(record));
        }

        Ok(Self {
            cache: RwLock::new(cache),
        })
    }

    pub fn add_fuel(&self, record: &StoredFuelConfig) {
        let updated = Arc::new(record.clone());

        if let Some(current) = self.get_record(&updated.type_id) {
            if updated.checkpoint_updated >= current.checkpoint_updated {
                self.cache
                    .write()
                    .unwrap()
                    .insert(updated.type_id.clone(), updated);
            }
        } else {
            self.cache
                .write()
                .unwrap()
                .insert(updated.type_id.clone(), updated);
        };
    }

    pub fn remove_fuel(&self, key: i64) {
        self.cache.write().unwrap().remove(&key);
    }

    pub fn get_record(&self, type_id: &i64) -> Option<Arc<StoredFuelConfig>> {
        let cache = self.cache.read().unwrap();
        cache.get(type_id).cloned()
    }

    pub fn get_value(&self, type_id: &i64) -> i64 {
        let cache = self.cache.read().unwrap();
        match cache.get(type_id).cloned() {
            Some(record) => record.efficiency,
            None => DEFAULT_FUEL_EFFICIENCY,
        }
    }

    pub fn contains(&self, type_id: &i64) -> bool {
        let cache = self.cache.read().unwrap();
        cache.contains_key(type_id)
    }
}
