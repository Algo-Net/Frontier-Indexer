use serde::Serialize;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use sui_pg_db::Connection;

use diesel::prelude::*;
use diesel::query_dsl::methods::FilterDsl;
use diesel::upsert::excluded;
use diesel_async::RunQueryDsl;

use crate::schema::system_table_registry;

#[derive(Queryable, Serialize, Insertable, Selectable, Clone, Debug)]
#[diesel(table_name = system_table_registry)]
pub struct StoredTableRecord {
    pub table_id: String,
    pub parent_id: String,
    pub package_id: String,
    pub module_name: String,
    pub struct_name: String,
    pub key_type: String,
    pub value_type: String,
    pub checkpoint_updated: i64,
}

pub struct TableRegistry {
    cache: RwLock<HashMap<String, Arc<StoredTableRecord>>>,
}

impl TableRegistry {
    pub async fn load_from_db(conn: &mut Connection<'_>) -> Self {
        use crate::schema::system_table_registry::dsl::*;

        let records = system_table_registry
            .load::<StoredTableRecord>(conn)
            .await
            .expect("Failed to fetch records from table registry");

        let mut cache = HashMap::new();

        for record in records {
            cache.insert(record.table_id.clone(), Arc::new(record));
        }

        Self {
            cache: RwLock::new(cache),
        }
    }

    pub async fn add_table(
        &self,
        conn: &mut Connection<'_>,
        record: &StoredTableRecord,
    ) -> QueryResult<()> {
        use crate::schema::system_table_registry::dsl::*;

        diesel::insert_into(system_table_registry)
            .values(record)
            .on_conflict(table_id)
            .do_update()
            .set(checkpoint_updated.eq(excluded(checkpoint_updated)))
            .filter(checkpoint_updated.lt(excluded(checkpoint_updated)))
            .execute(conn)
            .await?;

        let updated = Arc::new(record.clone());

        if let Some(current) = self.get_record(&updated.table_id) {
            if updated.checkpoint_updated >= current.checkpoint_updated {
                self.cache
                    .write()
                    .unwrap()
                    .insert(updated.table_id.clone(), updated);
            }
        } else {
            self.cache
                .write()
                .unwrap()
                .insert(updated.table_id.clone(), updated);
        };

        Ok(())
    }

    pub fn get_record(&self, entry_owner_id: &str) -> Option<Arc<StoredTableRecord>> {
        let cache = self.cache.read().unwrap();
        cache.get(entry_owner_id).cloned()
    }

    pub fn contains(&self, entry_owner_id: &str) -> bool {
        let cache = self.cache.read().unwrap();
        cache.contains_key(entry_owner_id)
    }
}
