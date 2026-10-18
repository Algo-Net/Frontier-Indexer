use serde_json::json;
use sui_graphql::Bcs;
use sui_graphql::Client;
use sui_graphql_macros::Response;
use sui_pg_db::Connection;
use sui_sdk_types::Address;

use diesel::prelude::*;
use diesel::query_dsl::methods::FilterDsl;
use diesel::upsert::excluded;
use diesel_async::{AsyncConnection, RunQueryDsl};

use crate::models::system::AddressAliases;
use crate::models::system::MoveAddressAliases;

#[derive(Response)]
#[response(root_type = "Owner")]
enum OwnerData {
    ConsensusAddressOwner(ConsensusOwner),
}

#[derive(Response)]
#[response(root_type = "ConsensusAddressOwner")]
struct ConsensusOwner {
    #[field(path = "address.address")]
    address: String,
}

#[derive(Response)]
struct AliasPage {
    #[field(path = "objects.pageInfo.hasNextPage")]
    has_next_page: bool,

    #[field(path = "objects.pageInfo.endCursor?")]
    end_cursor: Option<String>,

    #[field(path = "objects.nodes[].address")]
    object_ids: Vec<String>,

    #[field(path = "objects.nodes[].owner?")]
    owners: Vec<Option<OwnerData>>,

    #[field(path = "objects.nodes[].asMoveObject?.contents?.bcs?")]
    aliases: Vec<Option<Bcs<MoveAddressAliases>>>,

    #[field(path = "objects.nodes[].previousTransaction?.effects?.checkpoint?.sequenceNumber?")]
    checkpoints: Vec<Option<u64>>,
}

impl AliasPage {
    fn to_models(&self) -> Vec<AddressAliases> {
        let mut result = Vec::with_capacity(self.object_ids.len());
        for i in 0..self.object_ids.len() {
            let (Some(OwnerData::ConsensusAddressOwner(owner)), Some(Bcs(aliases)), Some(cp)) =
                (&self.owners[i], &self.aliases[i], self.checkpoints[i])
            else {
                eprintln!(
                    "skipping {}: missing owner/aliases/checkpoint",
                    self.object_ids[i]
                );
                continue;
            };

            let Ok(owner_addr) = owner.address.parse::<Address>() else {
                eprintln!("skipping {}: bad owner address", self.object_ids[i]);
                continue;
            };

            result.push(AddressAliases::from_parts(&owner_addr, aliases, cp as i64));
        }
        result
    }
}

async fn commit_page(
    entries: &[AddressAliases],
    conn: &mut Connection<'_>,
) -> anyhow::Result<usize> {
    conn.transaction::<_, anyhow::Error, _>(async |conn| write_entries(entries, conn).await)
        .await
}

async fn write_entries(
    entries: &[AddressAliases],
    conn: &mut Connection<'_>,
) -> anyhow::Result<usize> {
    use crate::schema::address_aliases::dsl::*;

    for entry in entries {
        let current = entry.aliases.clone();
        let values = entry.clone().to_aliases();

        diesel::insert_into(address_aliases)
            .values(values)
            .on_conflict((address, alias))
            .do_update()
            .set(checkpoint_updated.eq(excluded(checkpoint_updated)))
            .filter(checkpoint_updated.lt(excluded(checkpoint_updated)))
            .execute(conn)
            .await?;

        diesel::delete(address_aliases)
            .filter(address.eq(entry.id.clone()))
            .filter(alias.ne_all(current))
            .filter(checkpoint_updated.lt(entry.checkpoint_updated))
            .execute(conn)
            .await?;
    }
    Ok(entries.len())
}

pub async fn upload_address_aliases(conn: &mut Connection<'_>) -> anyhow::Result<()> {
    let client = Client::new(Client::TESTNET).expect("Could not initialize graphql client");

    let query = "query GetAddressAliases($type:String, $token:String) {
      objects(filter: {type: $type}, after: $token) {
        pageInfo { hasNextPage, endCursor }
        nodes {
          address
          previousTransaction {
            effects { checkpoint { sequenceNumber }}
          }
          owner {
            __typename
            ... on ConsensusAddressOwner {
              address { address }
            }
          }
          asMoveObject {
            contents { bcs }
          }
        }
      }
    }";

    let obj_type: String = "0x2::address_alias::AddressAliases".to_string();
    let mut token: Option<String> = None;

    loop {
        let response = client
            .query::<AliasPage>(query, json!({ "type": obj_type, "token": &token }))
            .await?;

        for err in response.errors() {
            eprintln!("GraphQL error: {}", err.message());
        }

        let Some(page) = response.data() else { break };
        commit_page(&page.to_models(), conn).await?;

        if !page.has_next_page {
            break;
        }

        token = page.end_cursor.clone();
    }

    Ok(())
}
