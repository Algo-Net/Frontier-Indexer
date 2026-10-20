use anyhow::Context;
use serde::{Deserialize, Serialize};
use std::str::FromStr;

use move_core_types::language_storage::StructTag;

pub mod app;
pub mod system;
pub mod world;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct MoveTypeName {
    pub name: String,
}

impl MoveTypeName {
    pub fn to_components(&self) -> anyhow::Result<(String, String, String)> {
        let base_name = self.name.split('<').next().unwrap_or(&self.name);

        let type_name = if base_name.starts_with("0x") {
            base_name.to_string()
        } else {
            format!("0x{}", base_name)
        };

        let tag =
            StructTag::from_str(&type_name).context("Could not parse TypeName into StructTag")?;

        let package_id = tag.address.to_canonical_string(true);
        let module_name = tag.module.to_string();
        let struct_name = tag.name.to_string();

        Ok((package_id, module_name, struct_name))
    }
}

pub trait Freezable {
    fn id(&self) -> String;

    fn package_id(&self) -> anyhow::Result<String>;

    fn module_name(&self) -> anyhow::Result<String>;

    fn struct_name(&self) -> anyhow::Result<String>;
}
