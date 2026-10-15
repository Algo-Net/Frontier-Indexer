// @generated automatically by Diesel CLI.
diesel::table! {
    address_aliases (address, alias) {
        #[max_length = 66]
        address -> Varchar,
        #[max_length = 66]
        alias -> Varchar,
        checkpoint_updated -> Int8,
    }
}

diesel::table! {
    assemblies (id) {
        #[max_length = 66]
        id -> Varchar,
        #[max_length = 20]
        item_id -> Varchar,
        tenant -> Text,
        type_id -> Int8,
        #[max_length = 66]
        owner_cap_id -> Varchar,
        #[max_length = 66]
        location -> Varchar,
        status -> Text,
        #[max_length = 66]
        energy_source_id -> Nullable<Varchar>,
        name -> Nullable<Text>,
        description -> Nullable<Text>,
        url -> Nullable<Text>,
        checkpoint_updated -> Int8,
    }
}

diesel::table! {
    characters (id) {
        #[max_length = 66]
        id -> Varchar,
        #[max_length = 20]
        item_id -> Varchar,
        tenant -> Text,
        #[max_length = 66]
        owner_cap_id -> Varchar,
        #[max_length = 66]
        owner_address -> Varchar,
        #[max_length = 20]
        tribe_id -> Varchar,
        name -> Text,
        description -> Nullable<Text>,
        url -> Nullable<Text>,
        checkpoint_updated -> Int8,
    }
}

diesel::table! {
    energy_config (type_id, table_id) {
        #[max_length = 66]
        table_id -> Varchar,
        type_id -> Int8,
        energy_cost -> Int8,
        #[max_length = 66]
        entry_object_id -> Varchar,
        checkpoint_updated -> Int8,
    }
}

diesel::table! {
    events_assembly_created (event_id, occurred_at) {
        #[max_length = 100]
        event_id -> Varchar,
        occurred_at -> Timestamptz,
        #[max_length = 66]
        id -> Varchar,
        #[max_length = 20]
        item_id -> Varchar,
        tenant -> Text,
        type_id -> Int8,
        #[max_length = 66]
        owner_cap_id -> Varchar,
    }
}

diesel::table! {
    events_character_created (event_id, occurred_at) {
        #[max_length = 100]
        event_id -> Varchar,
        occurred_at -> Timestamptz,
        #[max_length = 66]
        id -> Varchar,
        #[max_length = 20]
        item_id -> Varchar,
        tenant -> Text,
        #[max_length = 66]
        owner_address -> Varchar,
        #[max_length = 20]
        tribe_id -> Varchar,
    }
}

diesel::table! {
    events_energy_production_started (event_id, occurred_at) {
        #[max_length = 100]
        event_id -> Varchar,
        occurred_at -> Timestamptz,
        #[max_length = 66]
        id -> Varchar,
        production_current -> Int8,
    }
}

diesel::table! {
    events_energy_production_stopped (event_id, occurred_at) {
        #[max_length = 100]
        event_id -> Varchar,
        occurred_at -> Timestamptz,
        #[max_length = 66]
        id -> Varchar,
    }
}

diesel::table! {
    events_energy_released (event_id, occurred_at) {
        #[max_length = 100]
        event_id -> Varchar,
        occurred_at -> Timestamptz,
        #[max_length = 66]
        id -> Varchar,
        type_id -> Int8,
        released -> Int8,
        reserved_total -> Int8,
    }
}

diesel::table! {
    events_energy_reserved (event_id, occurred_at) {
        #[max_length = 100]
        event_id -> Varchar,
        occurred_at -> Timestamptz,
        #[max_length = 66]
        id -> Varchar,
        type_id -> Int8,
        reserved -> Int8,
        reserved_total -> Int8,
    }
}

diesel::table! {
    events_extension_frozen (event_id, occurred_at, id) {
        #[max_length = 66]
        event_id -> Varchar,
        occurred_at -> Timestamptz,
        #[max_length = 66]
        id -> Varchar,
    }
}

diesel::table! {
    events_fuel_burning_started (event_id, occurred_at) {
        #[max_length = 100]
        event_id -> Varchar,
        occurred_at -> Timestamptz,
        #[max_length = 66]
        id -> Varchar,
        #[max_length = 20]
        item_id -> Varchar,
        type_id -> Int8,
        quantity -> Int8,
        quantity_old -> Int8,
        burning -> Bool,
    }
}

diesel::table! {
    events_fuel_burning_stopped (event_id, occurred_at) {
        #[max_length = 100]
        event_id -> Varchar,
        occurred_at -> Timestamptz,
        #[max_length = 66]
        id -> Varchar,
        #[max_length = 20]
        item_id -> Varchar,
        type_id -> Int8,
        quantity -> Int8,
        quantity_old -> Int8,
        burning -> Bool,
    }
}

diesel::table! {
    events_fuel_burning_updated (event_id, occurred_at) {
        #[max_length = 100]
        event_id -> Varchar,
        occurred_at -> Timestamptz,
        #[max_length = 66]
        id -> Varchar,
        #[max_length = 20]
        item_id -> Varchar,
        type_id -> Int8,
        quantity -> Int8,
        quantity_old -> Int8,
        burning -> Bool,
    }
}

diesel::table! {
    events_fuel_deleted (event_id, occurred_at) {
        #[max_length = 100]
        event_id -> Varchar,
        occurred_at -> Timestamptz,
        #[max_length = 66]
        id -> Varchar,
        #[max_length = 20]
        item_id -> Varchar,
        type_id -> Int8,
        quantity -> Int8,
        quantity_old -> Int8,
        burning -> Bool,
    }
}

diesel::table! {
    events_fuel_deposited (event_id, occurred_at) {
        #[max_length = 100]
        event_id -> Varchar,
        occurred_at -> Timestamptz,
        #[max_length = 66]
        id -> Varchar,
        #[max_length = 20]
        item_id -> Varchar,
        type_id -> Int8,
        quantity -> Int8,
        quantity_old -> Int8,
        burning -> Bool,
    }
}

diesel::table! {
    events_fuel_efficiency_removed (event_id, occurred_at) {
        #[max_length = 100]
        event_id -> Varchar,
        occurred_at -> Timestamptz,
        type_id -> Int8,
    }
}

diesel::table! {
    events_fuel_efficiency_set (event_id, occurred_at) {
        #[max_length = 100]
        event_id -> Varchar,
        occurred_at -> Timestamptz,
        type_id -> Int8,
        efficiency -> Int8,
    }
}

diesel::table! {
    events_fuel_withdrawn (event_id, occurred_at) {
        #[max_length = 100]
        event_id -> Varchar,
        occurred_at -> Timestamptz,
        #[max_length = 66]
        id -> Varchar,
        #[max_length = 20]
        item_id -> Varchar,
        type_id -> Int8,
        quantity -> Int8,
        quantity_old -> Int8,
        burning -> Bool,
    }
}

diesel::table! {
    events_gate_created (event_id, occurred_at) {
        #[max_length = 100]
        event_id -> Varchar,
        occurred_at -> Timestamptz,
        #[max_length = 66]
        id -> Varchar,
        #[max_length = 20]
        item_id -> Varchar,
        tenant -> Text,
        type_id -> Int8,
        #[max_length = 66]
        owner_cap_id -> Varchar,
    }
}

diesel::table! {
    events_gate_extension_authorized (event_id, occurred_at) {
        #[max_length = 100]
        event_id -> Varchar,
        occurred_at -> Timestamptz,
        #[max_length = 66]
        id -> Varchar,
        #[max_length = 20]
        item_id -> Varchar,
        #[max_length = 66]
        package_id -> Varchar,
        module_name -> Text,
        struct_name -> Text,
        #[max_length = 66]
        package_id_old -> Nullable<Varchar>,
        module_name_old -> Nullable<Text>,
        struct_name_old -> Nullable<Text>,
    }
}

diesel::table! {
    events_gate_extension_revoked (event_id, occurred_at) {
        #[max_length = 100]
        event_id -> Varchar,
        occurred_at -> Timestamptz,
        #[max_length = 66]
        id -> Varchar,
        #[max_length = 20]
        item_id -> Varchar,
        #[max_length = 66]
        package_id -> Varchar,
        module_name -> Text,
        struct_name -> Text,
    }
}

diesel::table! {
    events_gate_jumped (event_id, occurred_at) {
        #[max_length = 100]
        event_id -> Varchar,
        occurred_at -> Timestamptz,
        #[max_length = 66]
        character_id -> Varchar,
        #[max_length = 20]
        character_item_id -> Varchar,
        #[max_length = 66]
        departure_id -> Varchar,
        #[max_length = 20]
        departure_item_id -> Varchar,
        #[max_length = 66]
        destination_id -> Varchar,
        #[max_length = 20]
        destination_item_id -> Varchar,
    }
}

diesel::table! {
    events_gate_linked (event_id, occurred_at) {
        #[max_length = 100]
        event_id -> Varchar,
        occurred_at -> Timestamptz,
        #[max_length = 66]
        departure_id -> Varchar,
        #[max_length = 20]
        departure_item_id -> Varchar,
        #[max_length = 66]
        destination_id -> Varchar,
        #[max_length = 20]
        destination_item_id -> Varchar,
    }
}

diesel::table! {
    events_gate_permit_issued (event_id, occurred_at) {
        #[max_length = 100]
        event_id -> Varchar,
        occurred_at -> Timestamptz,
        #[max_length = 66]
        id -> Varchar,
        #[max_length = 66]
        character_id -> Varchar,
        #[max_length = 20]
        character_item_id -> Varchar,
        #[max_length = 66]
        departure_id -> Varchar,
        #[max_length = 20]
        departure_item_id -> Varchar,
        #[max_length = 66]
        destination_id -> Varchar,
        #[max_length = 20]
        destination_item_id -> Varchar,
        #[max_length = 66]
        link_hash -> Varchar,
        #[max_length = 66]
        package_id -> Varchar,
        module_name -> Text,
        struct_name -> Text,
        expires_at -> Timestamptz,
    }
}

diesel::table! {
    events_gate_unlinked (event_id, occurred_at) {
        #[max_length = 100]
        event_id -> Varchar,
        occurred_at -> Timestamptz,
        #[max_length = 66]
        departure_id -> Varchar,
        #[max_length = 20]
        departure_item_id -> Varchar,
        #[max_length = 66]
        destination_id -> Varchar,
        #[max_length = 20]
        destination_item_id -> Varchar,
    }
}

diesel::table! {
    events_item_burned (event_id, occurred_at) {
        #[max_length = 66]
        event_id -> Varchar,
        occurred_at -> Timestamptz,
        #[max_length = 20]
        item_id -> Varchar,
        type_id -> Int8,
        quantity -> Int8,
        #[max_length = 66]
        assembly_id -> Varchar,
        #[max_length = 20]
        assembly_item_id -> Varchar,
        #[max_length = 66]
        character_id -> Varchar,
        #[max_length = 20]
        character_item_id -> Varchar,
    }
}

diesel::table! {
    events_item_deposited (event_id, occurred_at) {
        #[max_length = 66]
        event_id -> Varchar,
        occurred_at -> Timestamptz,
        #[max_length = 20]
        item_id -> Varchar,
        type_id -> Int8,
        quantity -> Int8,
        #[max_length = 66]
        assembly_id -> Varchar,
        #[max_length = 20]
        assembly_item_id -> Varchar,
        #[max_length = 66]
        character_id -> Varchar,
        #[max_length = 20]
        character_item_id -> Varchar,
        #[max_length = 66]
        inventory_id -> Nullable<Varchar>,
    }
}

diesel::table! {
    events_item_destroyed (event_id, occurred_at) {
        #[max_length = 66]
        event_id -> Varchar,
        occurred_at -> Timestamptz,
        #[max_length = 20]
        item_id -> Varchar,
        type_id -> Int8,
        quantity -> Int8,
        #[max_length = 66]
        assembly_id -> Varchar,
        #[max_length = 20]
        assembly_item_id -> Varchar,
    }
}

diesel::table! {
    events_item_minted (event_id, occurred_at) {
        #[max_length = 66]
        event_id -> Varchar,
        occurred_at -> Timestamptz,
        #[max_length = 20]
        item_id -> Varchar,
        type_id -> Int8,
        quantity -> Int8,
        #[max_length = 66]
        assembly_id -> Varchar,
        #[max_length = 20]
        assembly_item_id -> Varchar,
        #[max_length = 66]
        character_id -> Varchar,
        #[max_length = 20]
        character_item_id -> Varchar,
    }
}

diesel::table! {
    events_item_withdrawn (event_id, occurred_at) {
        #[max_length = 66]
        event_id -> Varchar,
        occurred_at -> Timestamptz,
        #[max_length = 20]
        item_id -> Varchar,
        type_id -> Int8,
        quantity -> Int8,
        #[max_length = 66]
        assembly_id -> Varchar,
        #[max_length = 20]
        assembly_item_id -> Varchar,
        #[max_length = 66]
        character_id -> Varchar,
        #[max_length = 20]
        character_item_id -> Varchar,
        #[max_length = 66]
        inventory_id -> Nullable<Varchar>,
    }
}

diesel::table! {
    events_location_revealed (event_id, occurred_at) {
        #[max_length = 100]
        event_id -> Varchar,
        occurred_at -> Timestamptz,
        #[max_length = 66]
        id -> Varchar,
        #[max_length = 20]
        item_id -> Varchar,
        type_id -> Int8,
        #[max_length = 66]
        owner_cap_id -> Varchar,
        #[max_length = 66]
        location_hash -> Varchar,
        #[max_length = 20]
        solar_system_id -> Varchar,
        x -> Text,
        y -> Text,
        z -> Text,
    }
}

diesel::table! {
    events_network_node_created (event_id, occurred_at) {
        #[max_length = 100]
        event_id -> Varchar,
        occurred_at -> Timestamptz,
        #[max_length = 66]
        id -> Varchar,
        #[max_length = 20]
        item_id -> Varchar,
        tenant -> Text,
        type_id -> Int8,
        #[max_length = 66]
        owner_cap_id -> Varchar,
    }
}

diesel::table! {
    events_owner_cap_created (event_id, occurred_at) {
        #[max_length = 100]
        event_id -> Varchar,
        occurred_at -> Timestamptz,
        #[max_length = 66]
        id -> Varchar,
        #[max_length = 66]
        object_id -> Varchar,
    }
}

diesel::table! {
    events_owner_cap_transferred (event_id, occurred_at) {
        #[max_length = 100]
        event_id -> Varchar,
        occurred_at -> Timestamptz,
        #[max_length = 66]
        id -> Varchar,
        #[max_length = 66]
        object_id -> Varchar,
        #[max_length = 66]
        owner -> Varchar,
        #[max_length = 66]
        owner_old -> Varchar,
    }
}

diesel::table! {
    events_rift_location_broadcasted (event_id, occurred_at) {
        #[max_length = 100]
        event_id -> Varchar,
        occurred_at -> Timestamptz,
        #[max_length = 66]
        id -> Varchar,
        #[max_length = 20]
        item_id -> Varchar,
        #[max_length = 66]
        location_hash -> Varchar,
        #[max_length = 20]
        solar_system_id -> Varchar,
        x -> Text,
        y -> Text,
        z -> Text,
    }
}

diesel::table! {
    events_rift_mining_started (event_id, occurred_at) {
        #[max_length = 100]
        event_id -> Varchar,
        occurred_at -> Timestamptz,
        #[max_length = 20]
        item_id -> Varchar,
        #[max_length = 66]
        character_id -> Varchar,
        #[max_length = 20]
        solar_system_id -> Varchar,
        x -> Text,
        y -> Text,
        z -> Text,
    }
}

diesel::table! {
    events_rift_spawned (event_id, occurred_at) {
        #[max_length = 100]
        event_id -> Varchar,
        occurred_at -> Timestamptz,
        #[max_length = 66]
        id -> Varchar,
        #[max_length = 20]
        item_id -> Varchar,
        tenant -> Text,
        #[max_length = 66]
        location_hash -> Varchar,
    }
}

diesel::table! {
    events_status_changed (event_id, occurred_at) {
        #[max_length = 100]
        event_id -> Varchar,
        occurred_at -> Timestamptz,
        #[max_length = 66]
        id -> Varchar,
        #[max_length = 20]
        item_id -> Varchar,
        status -> Text,
        action -> Text,
    }
}

diesel::table! {
    events_storage_unit_created (event_id, occurred_at) {
        #[max_length = 100]
        event_id -> Varchar,
        occurred_at -> Timestamptz,
        #[max_length = 66]
        id -> Varchar,
        #[max_length = 20]
        item_id -> Varchar,
        tenant -> Text,
        type_id -> Int8,
        #[max_length = 66]
        owner_cap_id -> Varchar,
    }
}

diesel::table! {
    events_storage_unit_extension_authorized (event_id, occurred_at) {
        #[max_length = 100]
        event_id -> Varchar,
        occurred_at -> Timestamptz,
        #[max_length = 66]
        id -> Varchar,
        #[max_length = 20]
        item_id -> Varchar,
        #[max_length = 66]
        package_id -> Varchar,
        module_name -> Text,
        struct_name -> Text,
        #[max_length = 66]
        package_id_old -> Nullable<Varchar>,
        module_name_old -> Nullable<Text>,
        struct_name_old -> Nullable<Text>,
    }
}

diesel::table! {
    events_storage_unit_extension_revoked (event_id, occurred_at) {
        #[max_length = 100]
        event_id -> Varchar,
        occurred_at -> Timestamptz,
        #[max_length = 66]
        id -> Varchar,
        #[max_length = 20]
        item_id -> Varchar,
        #[max_length = 66]
        package_id -> Varchar,
        module_name -> Text,
        struct_name -> Text,
    }
}

diesel::table! {
    events_turret_created (event_id, occurred_at) {
        #[max_length = 100]
        event_id -> Varchar,
        occurred_at -> Timestamptz,
        #[max_length = 66]
        id -> Varchar,
        #[max_length = 20]
        item_id -> Varchar,
        tenant -> Text,
        type_id -> Int8,
        #[max_length = 66]
        owner_cap_id -> Varchar,
    }
}

diesel::table! {
    events_turret_extension_authorized (event_id, occurred_at) {
        #[max_length = 100]
        event_id -> Varchar,
        occurred_at -> Timestamptz,
        #[max_length = 66]
        id -> Varchar,
        #[max_length = 20]
        item_id -> Varchar,
        #[max_length = 66]
        package_id -> Varchar,
        module_name -> Text,
        struct_name -> Text,
        #[max_length = 66]
        package_id_old -> Nullable<Varchar>,
        module_name_old -> Nullable<Text>,
        struct_name_old -> Nullable<Text>,
    }
}

diesel::table! {
    events_turret_extension_revoked (event_id, occurred_at) {
        #[max_length = 100]
        event_id -> Varchar,
        occurred_at -> Timestamptz,
        #[max_length = 66]
        id -> Varchar,
        #[max_length = 20]
        item_id -> Varchar,
        #[max_length = 66]
        package_id -> Varchar,
        module_name -> Text,
        struct_name -> Text,
    }
}

diesel::table! {
    extension_freezes (id) {
        #[max_length = 66]
        id -> Varchar,
        #[max_length = 66]
        owner_id -> Varchar,
        #[max_length = 66]
        package_id -> Varchar,
        module_name -> Text,
        struct_name -> Text,
    }
}

diesel::table! {
    fuel_config (type_id, table_id) {
        #[max_length = 66]
        table_id -> Varchar,
        type_id -> Int8,
        efficiency -> Int8,
        #[max_length = 66]
        entry_object_id -> Varchar,
        checkpoint_updated -> Int8,
    }
}

diesel::table! {
    gate_config (type_id, table_id) {
        #[max_length = 66]
        table_id -> Varchar,
        type_id -> Int8,
        distance -> Int8,
        #[max_length = 66]
        entry_object_id -> Varchar,
        checkpoint_updated -> Int8,
    }
}

diesel::table! {
    gate_permits (id) {
        #[max_length = 66]
        id -> Varchar,
        #[max_length = 66]
        character_id -> Varchar,
        #[max_length = 66]
        link_hash -> Varchar,
        expires_at -> Timestamptz,
    }
}

diesel::table! {
    gates (id) {
        #[max_length = 66]
        id -> Varchar,
        #[max_length = 20]
        item_id -> Varchar,
        tenant -> Text,
        type_id -> Int8,
        #[max_length = 66]
        owner_cap_id -> Varchar,
        #[max_length = 66]
        location -> Varchar,
        status -> Text,
        #[max_length = 66]
        energy_source_id -> Nullable<Varchar>,
        #[max_length = 66]
        linked_id -> Nullable<Varchar>,
        name -> Nullable<Text>,
        description -> Nullable<Text>,
        url -> Nullable<Text>,
        #[max_length = 66]
        package_id -> Nullable<Varchar>,
        module_name -> Nullable<Text>,
        struct_name -> Nullable<Text>,
        checkpoint_updated -> Int8,
    }
}

diesel::table! {
    inventories (id) {
        #[max_length = 66]
        id -> Varchar,
        #[max_length = 66]
        parent_id -> Varchar,
        capacity_used -> Int8,
        capacity_max -> Int8,
        checkpoint_updated -> Int8,
    }
}

diesel::table! {
    inventory_entries (inventory_id, type_id) {
        #[max_length = 66]
        inventory_id -> Varchar,
        type_id -> Int8,
        #[max_length = 20]
        item_id -> Varchar,
        volume -> Int8,
        quantity -> Int8,
        checkpoint_updated -> Int8,
    }
}

diesel::table! {
    items (id) {
        #[max_length = 66]
        id -> Varchar,
        #[max_length = 66]
        parent_id -> Varchar,
        #[max_length = 66]
        location -> Varchar,
        #[max_length = 20]
        item_id -> Varchar,
        type_id -> Int8,
        volume -> Int8,
        quantity -> Int8,
    }
}

diesel::table! {
    killmails (id, occurred_at) {
        #[max_length = 66]
        id -> Varchar,
        #[max_length = 20]
        kill_id -> Varchar,
        tenant -> Text,
        occurred_at -> Timestamptz,
        #[max_length = 20]
        solar_system_id -> Varchar,
        loss_type -> Text,
        #[max_length = 20]
        killer_id -> Varchar,
        #[max_length = 20]
        victim_id -> Varchar,
        #[max_length = 20]
        reporter_id -> Varchar,
    }
}

diesel::table! {
    network_nodes (id) {
        #[max_length = 66]
        id -> Varchar,
        #[max_length = 20]
        item_id -> Varchar,
        tenant -> Text,
        type_id -> Int8,
        #[max_length = 66]
        owner_cap_id -> Varchar,
        #[max_length = 66]
        location -> Varchar,
        status -> Text,
        energy_production -> Int8,
        energy_capacity -> Int8,
        energy_reserved -> Int8,
        connected_ids -> Array<Nullable<Text>>,
        burning -> Bool,
        burn_rate -> Int8,
        burn_start -> Timestamptz,
        burn_updated -> Timestamptz,
        burn_elapsed -> Int8,
        fuel_capacity -> Int8,
        fuel_duration -> Int8,
        fuel_quantity -> Int8,
        fuel_type -> Nullable<Int8>,
        fuel_volume -> Nullable<Int8>,
        name -> Nullable<Text>,
        description -> Nullable<Text>,
        url -> Nullable<Text>,
        checkpoint_updated -> Int8,
    }
}

diesel::table! {
    owner_caps (id) {
        #[max_length = 66]
        id -> Varchar,
        #[max_length = 66]
        object_id -> Varchar,
        #[max_length = 66]
        owner_address -> Varchar,
        #[max_length = 66]
        package_id -> Varchar,
        module_name -> Text,
        struct_name -> Text,
        checkpoint_updated -> Int8,
    }
}

diesel::table! {
    rifts (id) {
        #[max_length = 66]
        id -> Varchar,
        #[max_length = 20]
        item_id -> Varchar,
        tenant -> Text,
        #[max_length = 66]
        location -> Varchar,
        checkpoint_updated -> Int8,
    }
}

diesel::table! {
    storage_units (id) {
        #[max_length = 66]
        id -> Varchar,
        #[max_length = 20]
        item_id -> Varchar,
        tenant -> Text,
        type_id -> Int8,
        #[max_length = 66]
        owner_cap_id -> Varchar,
        #[max_length = 66]
        location -> Varchar,
        status -> Text,
        inventory_ids -> Array<Nullable<Text>>,
        #[max_length = 66]
        energy_source_id -> Nullable<Varchar>,
        name -> Nullable<Text>,
        description -> Nullable<Text>,
        url -> Nullable<Text>,
        #[max_length = 66]
        package_id -> Nullable<Varchar>,
        module_name -> Nullable<Text>,
        struct_name -> Nullable<Text>,
        checkpoint_updated -> Int8,
    }
}

diesel::table! {
    system_table_registry (table_id) {
        #[max_length = 66]
        table_id -> Varchar,
        #[max_length = 66]
        parent_id -> Varchar,
        #[max_length = 66]
        package_id -> Varchar,
        module_name -> Text,
        struct_name -> Text,
        key_type -> Text,
        value_type -> Text,
        checkpoint_updated -> Int8,
    }
}

diesel::table! {
    turrets (id) {
        #[max_length = 66]
        id -> Varchar,
        #[max_length = 20]
        item_id -> Varchar,
        tenant -> Text,
        type_id -> Int8,
        #[max_length = 66]
        owner_cap_id -> Varchar,
        #[max_length = 66]
        location -> Varchar,
        status -> Text,
        #[max_length = 66]
        energy_source_id -> Nullable<Varchar>,
        name -> Nullable<Text>,
        description -> Nullable<Text>,
        url -> Nullable<Text>,
        #[max_length = 66]
        package_id -> Nullable<Varchar>,
        module_name -> Nullable<Text>,
        struct_name -> Nullable<Text>,
        checkpoint_updated -> Int8,
    }
}

diesel::allow_tables_to_appear_in_same_query!(
    address_aliases,
    assemblies,
    characters,
    energy_config,
    events_assembly_created,
    events_character_created,
    events_energy_production_started,
    events_energy_production_stopped,
    events_energy_released,
    events_energy_reserved,
    events_extension_frozen,
    events_fuel_burning_started,
    events_fuel_burning_stopped,
    events_fuel_burning_updated,
    events_fuel_deleted,
    events_fuel_deposited,
    events_fuel_efficiency_removed,
    events_fuel_efficiency_set,
    events_fuel_withdrawn,
    events_gate_created,
    events_gate_extension_authorized,
    events_gate_extension_revoked,
    events_gate_jumped,
    events_gate_linked,
    events_gate_permit_issued,
    events_gate_unlinked,
    events_item_burned,
    events_item_deposited,
    events_item_destroyed,
    events_item_minted,
    events_item_withdrawn,
    events_location_revealed,
    events_network_node_created,
    events_owner_cap_created,
    events_owner_cap_transferred,
    events_rift_location_broadcasted,
    events_rift_mining_started,
    events_rift_spawned,
    events_status_changed,
    events_storage_unit_created,
    events_storage_unit_extension_authorized,
    events_storage_unit_extension_revoked,
    events_turret_created,
    events_turret_extension_authorized,
    events_turret_extension_revoked,
    extension_freezes,
    fuel_config,
    gate_config,
    gate_permits,
    gates,
    inventories,
    inventory_entries,
    items,
    killmails,
    network_nodes,
    owner_caps,
    rifts,
    storage_units,
    system_table_registry,
    turrets,
);
