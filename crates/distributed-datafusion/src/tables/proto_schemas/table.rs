// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::catalog::TableProvider;
use datafusion::logical_expr::Expr;
use datafusion::physical_plan::SendableRecordBatchStream;
use datafusion::physical_plan::stream::{RecordBatchReceiverStream, RecordBatchStreamAdapter};
use tokio::sync::mpsc::Sender;

use restate_storage_query_api::tables::proto_schemas::{ProtoSchemas, ProtoSchemasColumn};
use restate_storage_query_datafusion::proto_schema::{
    CompiledMessageSchema, DynamicProtoSchemaRegistry, ServiceProtoSchemas,
};
use restate_types::schema::service::ServiceMetadataResolver;

use super::row::append_proto_schema_row;
use super::schema::{SysProtoSchemasBuilder, SysProtoSchemasTable};
use crate::source_registry::SourceRegistry;
use crate::statistics::{SERVICE_ROW_ESTIMATE, TableStatisticsBuilder};
use crate::table_providers::{GenericTableProvider, Scan};

impl SysProtoSchemasTable {
    pub(crate) fn create_provider(sources: SourceRegistry) -> Arc<dyn TableProvider> {
        let statistics = TableStatisticsBuilder::<ProtoSchemas>::new()
            .with_num_rows_estimate(SERVICE_ROW_ESTIMATE)
            .with_foreign_key(ProtoSchemasColumn::ServiceName, SERVICE_ROW_ESTIMATE);
        Arc::new(
            GenericTableProvider::declared::<Self>(None, Some(sources))
                .with_statistics(statistics.build()),
        )
    }

    pub(crate) fn create_local_scanner<SMR>(registry: DynamicProtoSchemaRegistry<SMR>) -> impl Scan
    where
        SMR: ServiceMetadataResolver + Send + Sync + Clone + 'static,
    {
        ProtoSchemasScanner(registry)
    }
}

#[derive(Clone, derive_more::Debug)]
#[debug("ProtoSchemasScanner")]
struct ProtoSchemasScanner<SMR>(DynamicProtoSchemaRegistry<SMR>);

impl<SMR: ServiceMetadataResolver + Sync + Send + Clone + 'static> Scan
    for ProtoSchemasScanner<SMR>
{
    fn scan(
        &self,
        projection: SchemaRef,
        _filters: &[Expr],
        batch_size: usize,
        limit: Option<usize>,
    ) -> SendableRecordBatchStream {
        if limit == Some(0) {
            return Box::pin(RecordBatchStreamAdapter::new(
                projection,
                futures::stream::empty(),
            ));
        }

        let schema = projection.clone();
        let mut stream_builder = RecordBatchReceiverStream::builder(projection, 16);
        let tx = stream_builder.tx();
        let registry = self.0.clone();

        stream_builder.spawn(async move {
            let services = registry.list_all_service_schemas();
            send_proto_schema_batches(schema, tx, services, batch_size, limit).await;
            Ok(())
        });
        stream_builder.build()
    }
}

async fn send_proto_schema_batches(
    schema: SchemaRef,
    tx: Sender<datafusion::common::Result<RecordBatch>>,
    mut services: Vec<Arc<ServiceProtoSchemas>>,
    batch_size: usize,
    limit: Option<usize>,
) {
    services.sort_by(|a, b| a.service_name.cmp(&b.service_name));
    let mut builder = SysProtoSchemasBuilder::new(schema);
    let mut remaining = limit.unwrap_or(usize::MAX);

    for svc in services {
        let mut keys: Vec<(&str, &Arc<CompiledMessageSchema>)> = svc
            .state_key_messages
            .iter()
            .map(|(k, v)| (k.as_str(), v))
            .collect();
        keys.sort_by_key(|(k, _)| *k);
        if let Some(wild) = &svc.wildcard_message {
            keys.push(("*", wild));
        }
        for (state_key, msg) in keys {
            if remaining == 0 {
                break;
            }
            remaining -= 1;
            append_proto_schema_row(
                &mut builder,
                &svc.service_name,
                svc.revision,
                state_key,
                msg,
            );
            if builder.num_rows() >= batch_size && tx.send(builder.finish()).await.is_err() {
                return;
            }
        }
        if remaining == 0 {
            break;
        }
    }

    if builder.num_rows() > 0 {
        let _ = tx.send(builder.into_batch()).await;
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use datafusion::arrow::array::RecordBatch;
    use datafusion::arrow::util::display::array_value_to_string;
    use futures::TryStreamExt;
    use prost::Message;

    use restate_storage_api::Transaction;
    use restate_storage_api::state_table::WriteStateTable;
    use restate_storage_query_datafusion::proto_schema::{
        PROTO_DESCRIPTOR_SET_KEY, PROTO_STATE_KEY_PREFIX,
    };
    use restate_types::identifiers::ServiceId;
    use restate_types::schema::service::ServiceMetadata;

    use crate::mocks::{MockQueryEngine, MockSchemas};

    #[derive(Clone, PartialEq, prost::Message)]
    struct FieldDesc {
        #[prost(string, optional, tag = "1")]
        name: Option<String>,
        #[prost(int32, optional, tag = "3")]
        number: Option<i32>,
        #[prost(int32, optional, tag = "4")]
        label: Option<i32>,
        #[prost(int32, optional, tag = "5")]
        ty: Option<i32>,
    }

    #[derive(Clone, PartialEq, prost::Message)]
    struct MsgDesc {
        #[prost(string, optional, tag = "1")]
        name: Option<String>,
        #[prost(message, repeated, tag = "2")]
        field: Vec<FieldDesc>,
    }

    #[derive(Clone, PartialEq, prost::Message)]
    struct FileDesc {
        #[prost(string, optional, tag = "1")]
        name: Option<String>,
        #[prost(string, optional, tag = "2")]
        package: Option<String>,
        #[prost(message, repeated, tag = "4")]
        message_type: Vec<MsgDesc>,
        #[prost(string, optional, tag = "12")]
        syntax: Option<String>,
    }

    #[derive(Clone, PartialEq, prost::Message)]
    struct Fds {
        #[prost(message, repeated, tag = "1")]
        file: Vec<FileDesc>,
    }

    #[derive(Clone, PartialEq, prost::Message)]
    struct ProfileProto {
        #[prost(string, tag = "1")]
        name: String,
    }

    fn encode_base64(input: &[u8]) -> String {
        const TABLE: &[u8; 64] =
            b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
        let mut out = String::with_capacity(input.len().div_ceil(3) * 4);
        for chunk in input.chunks(3) {
            let triple = (u32::from(chunk[0]) << 16)
                | (u32::from(chunk.get(1).copied().unwrap_or(0)) << 8)
                | u32::from(chunk.get(2).copied().unwrap_or(0));
            out.push(TABLE[((triple >> 18) & 0x3f) as usize] as char);
            out.push(TABLE[((triple >> 12) & 0x3f) as usize] as char);
            out.push(if chunk.len() > 1 {
                TABLE[((triple >> 6) & 0x3f) as usize] as char
            } else {
                '='
            });
            out.push(if chunk.len() > 2 {
                TABLE[(triple & 0x3f) as usize] as char
            } else {
                '='
            });
        }
        out
    }

    async fn query_rows(engine: &MockQueryEngine, sql: &str) -> Vec<Vec<String>> {
        let batches: Vec<RecordBatch> = engine
            .execute(sql)
            .await
            .unwrap()
            .stream
            .try_collect()
            .await
            .unwrap();
        batches
            .iter()
            .flat_map(|batch| {
                (0..batch.num_rows()).map(|row| {
                    batch
                        .columns()
                        .iter()
                        .map(|column| array_value_to_string(column, row).unwrap())
                        .collect()
                })
            })
            .collect()
    }

    #[restate_core::test(flavor = "multi_thread", worker_threads = 2)]
    async fn sys_proto_schemas_and_proto_udfs_end_to_end() {
        let fds = Fds {
            file: vec![FileDesc {
                name: Some("profile.proto".to_string()),
                package: Some("example".to_string()),
                message_type: vec![MsgDesc {
                    name: Some("Profile".to_string()),
                    field: vec![FieldDesc {
                        name: Some("name".to_string()),
                        number: Some(1),
                        label: Some(1),
                        ty: Some(9),
                    }],
                }],
                syntax: Some("proto3".to_string()),
            }],
        };

        let mut svc = ServiceMetadata::mock_virtual_object("UserProfile", ["get"]);
        svc.revision = 3;
        svc.metadata.insert(
            PROTO_DESCRIPTOR_SET_KEY.to_string(),
            encode_base64(&fds.encode_to_vec()),
        );
        svc.metadata.insert(
            format!("{PROTO_STATE_KEY_PREFIX}profile"),
            "example.Profile".to_string(),
        );

        let mut schemas = MockSchemas::default();
        schemas.services.add(svc);
        let mut engine = MockQueryEngine::create_with(schemas).await;

        {
            let mut tx = engine.partition_store().transaction();
            tx.put_user_state(
                &ServiceId::new(None, "UserProfile", "user-1"),
                &Bytes::from_static(b"profile"),
                ProfileProto {
                    name: "alice".to_string(),
                }
                .encode_to_vec(),
            )
            .unwrap();
            tx.put_user_state(
                &ServiceId::new(None, "UserProfile", "user-2"),
                &Bytes::from_static(b"profile"),
                ProfileProto {
                    name: "bob".to_string(),
                }
                .encode_to_vec(),
            )
            .unwrap();
            tx.commit().await.unwrap();
        }

        assert_eq!(
            query_rows(
                &engine,
                "SELECT service_name, revision, state_key, message_name, fields_json FROM sys_proto_schemas",
            )
            .await,
            vec![vec![
                "UserProfile".to_string(),
                "3".to_string(),
                "profile".to_string(),
                "example.Profile".to_string(),
                r#"[{"name":"name","number":1,"type":"string","repeated":false,"map":false}]"#
                    .to_string(),
            ]],
        );

        assert!(
            query_rows(
                &engine,
                "SELECT service_name FROM sys_proto_schemas LIMIT 0"
            )
            .await
            .is_empty()
        );

        assert_eq!(
            query_rows(
                &engine,
                "SELECT proto_get_str(service_name, key, value, 'name'), proto_to_json(service_name, key, value) FROM state WHERE proto_get_str(service_name, key, value, 'name') = 'alice'",
            )
            .await,
            vec![vec![
                "alice".to_string(),
                r#"{"name":"alice"}"#.to_string(),
            ]],
        );
    }
}
