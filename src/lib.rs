use http::Uri;
use std::collections::HashSet;
use std::str::FromStr;
use std::sync::Arc;
use sui_types::TypeTag;
use url::Url;

use move_core_types::account_address::AccountAddress;

use sui_indexer_alt_framework::types::full_checkpoint_content::ExecutedTransaction;
use sui_indexer_alt_framework::types::full_checkpoint_content::ObjectSet;

use sui_types::event::Event;
use sui_types::object::Object;
use sui_types::transaction::Command;
use sui_types::transaction::TransactionDataAPI;

use crate::models::system::FuelRegistry;
use crate::models::system::TableRegistry;

pub mod config;
pub mod handlers;
pub mod models;
pub mod pipelines;
pub mod sandbox;
pub mod schema;
pub mod transports;

pub const NOT_MAINNET_PACKAGE: &str = "<not on mainnet>";

pub const MAINNET_REMOTE_STORE_URL: &str = "https://checkpoints.mainnet.sui.io";
pub const MAINNET_RPC_URL: &str = "https://fullnode.mainnet.sui.io:443";

pub const TESTNET_REMOTE_STORE_URL: &str = "https://checkpoints.testnet.sui.io";
pub const TESTNET_RPC_URL: &str = "https://fullnode.testnet.sui.io:443";

const MAINNET_PACKAGES: &[&str] = &[];

const MAINNET_WORLD_PACKAGES: &[&str] = &[];

const TESTNET_PACKAGES: &[&str] = &[];

const TESTNET_WORLD_PACKAGES: &[&str] = &[
    "0xac361aa5ceb726bd974f885c9dea9e55dc9bc98fa1f5731c5965a810707bf0b8", // Assets v1
    "0x8b8a46ed766fa1358ce7c5c51f6a164b13d627a63e45343f69ed0ba0446c1aa1", // World v1
    "0x9194d2b9d8b692e418980a7452c23631a55e75e57db67de48fff4952de2d86fc", // World v2
];

pub const APP_MODULES: &[&str] = &[];

pub const WORLD_MODULES: &[&str] = &[
    "assets",
    "access",
    "assembly",
    "EVE",
    "extension_freeze",
    "gate",
    "storage_unit",
    "turret",
    "character",
    "sig_verify",
    "killmail",
    "network_node",
    "energy",
    "fuel",
    "in_game_id",
    "inventory",
    "location",
    "metadata",
    "status",
    "killmail_registry",
    "object_registry",
    "rift",
    "world",
];

pub const SUI_MODULES: &[&str] = &["sui"];

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ModuleType {
    App,
    World,
    Sui,
    Unknown,
}

pub fn is_app_module(module: &str) -> bool {
    APP_MODULES.contains(&module)
}

pub fn is_world_module(module: &str) -> bool {
    WORLD_MODULES.contains(&module)
}

pub fn is_sui_module(module: &str) -> bool {
    SUI_MODULES.contains(&module)
}

pub fn get_module_type(module: &str) -> ModuleType {
    if is_app_module(module) {
        ModuleType::App
    } else if is_world_module(module) {
        ModuleType::World
    } else if is_sui_module(module) {
        ModuleType::Sui
    } else {
        ModuleType::Unknown
    }
}

pub fn get_all_known_modules() -> Vec<&'static str> {
    let mut modules = Vec::new();
    modules.extend_from_slice(APP_MODULES);
    modules.extend_from_slice(WORLD_MODULES);
    modules.extend_from_slice(SUI_MODULES);
    modules
}

pub fn get_app_modules() -> &'static [&'static str] {
    APP_MODULES
}

pub fn get_world_modules() -> &'static [&'static str] {
    WORLD_MODULES
}

pub fn get_sui_modules() -> &'static [&'static str] {
    SUI_MODULES
}

pub fn is_valid_app_package(package: &str) -> bool {
    package != NOT_MAINNET_PACKAGE
}

pub fn is_valid_world_package(package: &str) -> bool {
    package != NOT_MAINNET_PACKAGE
}

pub fn is_valid_app_packages(packages: &[&str]) -> bool {
    packages.iter().any(|&pkg| is_valid_app_package(pkg))
}

pub fn is_valid_world_packages(packages: &[&str]) -> bool {
    packages.iter().any(|&pkg| is_valid_world_package(pkg))
}

pub fn get_app_package_addresses(env: AppEnv) -> &'static [&'static str] {
    if let Some(app) = sandbox::app_packages() {
        return app;
    }

    match env {
        AppEnv::Mainnet => MAINNET_PACKAGES,
        AppEnv::Testnet => TESTNET_PACKAGES,
    }
}

pub fn get_app_package_address(env: AppEnv) -> Result<&'static str, String> {
    if let Some(app) = sandbox::app_packages() {
        return app
            .first()
            .copied()
            .ok_or_else(|| "No app packages configured in sandbox mode".to_string());
    }

    let packages = get_app_package_addresses(env);

    for &package in packages {
        if is_valid_app_package(package) {
            return Ok(package);
        }
    }

    Err(format!(
        "App package is not supported on {:?}. \
        The app package has not been deployed on this network.",
        env
    ))
}

pub fn get_world_package_addresses(env: AppEnv) -> &'static [&'static str] {
    if let Some(world) = sandbox::world_packages() {
        return world;
    }

    match env {
        AppEnv::Mainnet => MAINNET_WORLD_PACKAGES,
        AppEnv::Testnet => TESTNET_WORLD_PACKAGES,
    }
}

pub fn get_world_package_address(env: AppEnv) -> Result<&'static str, String> {
    if let Some(world) = sandbox::world_packages() {
        return world
            .first()
            .copied()
            .ok_or_else(|| "No world packages configured in sandbox mode".to_string());
    }

    let packages = get_world_package_addresses(env);

    for &package in packages {
        if is_valid_world_package(package) {
            return Ok(package);
        }
    }

    Err(format!(
        "World package is not supported on {:?}. \
        The world package has not been deployed on this network.",
        env
    ))
}

#[derive(Debug, Clone, Copy, clap::ValueEnum)]
pub enum AppEnv {
    Mainnet,
    Testnet,
}

impl AppEnv {
    pub fn remote_store_url(&self) -> Url {
        let url = match self {
            AppEnv::Mainnet => MAINNET_REMOTE_STORE_URL,
            AppEnv::Testnet => TESTNET_REMOTE_STORE_URL,
        };
        Url::parse(url).expect("Could not parse remote_store_url")
    }

    pub fn fullnode_url(&self) -> Url {
        let url = match self {
            AppEnv::Mainnet => MAINNET_RPC_URL,
            AppEnv::Testnet => TESTNET_RPC_URL,
        };
        Url::parse(url).expect("Could not parse remote_store_url")
    }

    pub fn streaming_url(&self) -> Uri {
        let url = match self {
            AppEnv::Mainnet => MAINNET_RPC_URL,
            AppEnv::Testnet => TESTNET_RPC_URL,
        };
        url.parse::<Uri>().expect("Could not parse streaming_url")
    }
}

#[derive(Clone)]
pub struct AppContext {
    pub env: AppEnv,
    pub tables: Arc<TableRegistry>,
    pub fuels: Arc<FuelRegistry>,

    pub app_packages: Arc<HashSet<AccountAddress>>,
    pub world_packages: Arc<HashSet<AccountAddress>>,
}

impl AppContext {
    pub fn new(env: AppEnv, tables: TableRegistry, fuels: FuelRegistry) -> Self {
        let app_packages = Self::get_app_package_strings(env)
            .iter()
            .filter_map(|s| AccountAddress::from_str(s).ok())
            .collect();

        let world_packages = Self::get_world_package_strings(env)
            .iter()
            .filter_map(|s| AccountAddress::from_str(s).ok())
            .collect();

        Self {
            env,
            tables: Arc::new(tables),
            fuels: Arc::new(fuels),
            app_packages: Arc::new(app_packages),
            world_packages: Arc::new(world_packages),
        }
    }

    pub fn is_indexed_tx(&self, tx: &ExecutedTransaction, checkpoint_objects: &ObjectSet) -> bool {
        let app_addresses = self.package_addresses();
        let app_packages = self.package_ids();

        // Check input object against all known package versions
        let has_app_input = tx.input_objects(checkpoint_objects).any(|obj| {
            obj.data
                .type_()
                .map(|t| app_addresses.iter().any(|addr| t.address() == *addr))
                .unwrap_or_default()
        });

        if has_app_input {
            return true;
        }

        // Check if any changed object is in our table registry
        let touches_registered_table =
            tx.effects
                .all_changed_objects()
                .iter()
                .any(|(entry, _, _)| {
                    if self.tables.contains(&entry.0.to_canonical_string(true)) {
                        return true;
                    }

                    false
                });

        if touches_registered_table {
            return true;
        }

        // Check if transaction has application events from any version
        if let Some(events) = &tx.events {
            let has_app_event = events.data.iter().any(|event| {
                app_addresses
                    .iter()
                    .any(|addr| event.type_.address == *addr)
            });

            if has_app_event {
                return true;
            }
        }

        // Check if transaction calls a application function from any version
        let txn_kind = tx.transaction.kind();

        let has_app_call = txn_kind.iter_commands().any(|cmd| {
            if let Command::MoveCall(move_call) = cmd {
                app_packages.iter().any(|pkg| *pkg == move_call.package)
            } else {
                false
            }
        });

        has_app_call
    }

    pub fn is_world_event(&self, event: &Event, module_name: &str, event_name: &str) -> bool {
        let tag = &event.type_;

        if tag.module.as_str() != module_name {
            return false;
        }

        if tag.name.as_str() != event_name {
            return false;
        }

        self.world_packages.contains(&tag.address)
    }

    pub fn is_world_object(&self, obj: &Object, module_name: &str, struct_name: &str) -> bool {
        let Some(move_type) = obj.type_() else {
            return false;
        };

        let Some(tag) = move_type.other() else {
            return false;
        };

        if tag.module.as_str() != module_name {
            return false;
        }

        if tag.name.as_str() != struct_name {
            return false;
        }

        self.world_packages.contains(&tag.address)
    }

    pub fn is_world_struct(&self, tag: &TypeTag, module_name: &str, struct_name: &str) -> bool {
        let TypeTag::Struct(s_tag) = tag else {
            return false;
        };

        if s_tag.module.as_str() != module_name {
            return false;
        }

        if s_tag.name.as_str() != struct_name {
            return false;
        }

        self.world_packages.contains(&s_tag.address)
    }

    fn get_app_package_strings(env: AppEnv) -> Vec<&'static str> {
        if let Some(app) = sandbox::app_packages() {
            return app.to_vec();
        }

        let app_packages = match env {
            AppEnv::Mainnet => MAINNET_PACKAGES,
            AppEnv::Testnet => TESTNET_PACKAGES,
        };

        app_packages.to_vec()
    }

    fn get_world_package_strings(env: AppEnv) -> Vec<&'static str> {
        if let Some(world) = sandbox::world_packages() {
            return world.to_vec();
        }

        let world_packages = match env {
            AppEnv::Mainnet => MAINNET_WORLD_PACKAGES,
            AppEnv::Testnet => TESTNET_WORLD_PACKAGES,
        };

        world_packages.to_vec()
    }

    fn get_all_package_strings(env: AppEnv) -> Vec<&'static str> {
        if let (Some(app), Some(world)) = (sandbox::app_packages(), sandbox::world_packages()) {
            let mut all = app.to_vec();
            all.extend_from_slice(world);
            return all;
        }

        let (app_packages, world_packages) = match env {
            AppEnv::Mainnet => (MAINNET_PACKAGES, MAINNET_WORLD_PACKAGES),
            AppEnv::Testnet => (TESTNET_PACKAGES, TESTNET_WORLD_PACKAGES),
        };

        let mut all_packages = app_packages.to_vec();

        for &world_package in world_packages {
            if world_package != NOT_MAINNET_PACKAGE {
                all_packages.push(world_package);
            }
        }

        all_packages
    }

    fn package_ids(&self) -> Vec<sui_types::base_types::ObjectID> {
        use std::str::FromStr;
        use sui_types::base_types::ObjectID;

        Self::get_all_package_strings(self.env)
            .iter()
            .map(|pkg| ObjectID::from_str(pkg).unwrap())
            .collect()
    }

    fn package_addresses(&self) -> Vec<move_core_types::account_address::AccountAddress> {
        use move_core_types::account_address::AccountAddress;
        use std::str::FromStr;

        Self::get_all_package_strings(self.env)
            .iter()
            .map(|pkg| AccountAddress::from_str(pkg).unwrap())
            .collect()
    }
}
