// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use std::time::Instant;

use base64::Engine;
use bytes::Bytes;
use datafusion::arrow::array::{
    Array, ArrayRef, BooleanArray, Float64Array, Int64Array, LargeBinaryArray, LargeStringArray,
    UInt64Array,
};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::{DFSchema, ScalarValue};
use datafusion::config::ConfigOptions;
use datafusion::execution::FunctionRegistry;
use datafusion::logical_expr::{ColumnarValue, ScalarFunctionArgs};
use datafusion::prelude::{col, lit};
use futures::StreamExt;
use parking_lot::RwLock;
use prost::Message;
use prost_types::field_descriptor_proto::{Label, Type};
use prost_types::{
    DescriptorProto, EnumDescriptorProto, EnumValueDescriptorProto, FieldDescriptorProto,
    FileDescriptorProto, FileDescriptorSet, MessageOptions,
};

use restate_storage_api::Transaction;
use restate_storage_api::state_table::WriteStateTable;
use restate_types::Scope;
use restate_types::deployment::{DeploymentAddress, Headers};
use restate_types::identifiers::{DeploymentId, ServiceId, ServiceRevision};
use restate_types::live::Live;
use restate_types::net::address::{AdvertisedAddress, HttpIngressPort};
use restate_types::schema::deployment::test_util::MockDeploymentMetadataRegistry;
use restate_types::schema::deployment::{Deployment, DeploymentResolver};
use restate_types::schema::service::{ServiceMetadata, ServiceMetadataResolver};

use super::udf::create_udfs;
use super::wire::{
    extract_bool, extract_f64, extract_i64, extract_str, transcode_to_json,
    validate_descriptor_set_wire_nesting,
};
use super::{
    CompiledDescriptorPool, DynamicProtoSchemaRegistry, MAX_CACHED_PATHS_PER_MESSAGE,
    MAX_DESCRIPTOR_NESTING, MAX_IDENTIFIER_LEN, MAX_POOL_MESSAGES, PROTO_DESCRIPTOR_SET_KEY,
    PROTO_FDS_SHORT_KEY, PROTO_STATE_KEY_PREFIX, PROTO_STATE_WILDCARD_KEY, ServiceProtoSchemas,
};
use crate::DataFusionEnv;
use crate::mocks::{MockQueryEngine, MockSchemas, MockStatusHandle};
#[derive(Clone, PartialEq, prost::Message)]
struct BusyMemberProto {
    #[prost(string, tag = "1")]
    member_id: String,
    #[prost(string, tag = "2")]
    email: String,
    #[prost(string, tag = "3")]
    tenant_id: String,
    #[prost(string, tag = "4")]
    phone: String,
    #[prost(string, tag = "5")]
    external_id: String,
    #[prost(string, tag = "6")]
    status: String,
    #[prost(string, tag = "7")]
    tier: String,
    #[prost(int64, tag = "8")]
    balance_cents: i64,
    #[prost(uint64, tag = "9")]
    churn_count: u64,
    #[prost(uint64, tag = "10")]
    last_churn_at: u64,
    #[prost(bool, tag = "11")]
    armed: bool,
    #[prost(uint64, tag = "12")]
    generation: u64,
    #[prost(btree_map = "string, string", tag = "15")]
    metadata: BTreeMap<String, String>,
}

fn build_busy_member_descriptor_set() -> Vec<u8> {
    let field = |name: &str, num: i32, ty: Type, label: Label, type_name: Option<&str>| {
        FieldDescriptorProto {
            name: Some(name.to_string()),
            number: Some(num),
            label: Some(label as i32),
            r#type: Some(ty as i32),
            type_name: type_name.map(ToString::to_string),
            ..Default::default()
        }
    };

    let map_entry = DescriptorProto {
        name: Some("MetadataEntry".to_string()),
        field: vec![
            field("key", 1, Type::String, Label::Optional, None),
            field("value", 2, Type::String, Label::Optional, None),
        ],
        options: Some(MessageOptions {
            map_entry: Some(true),
            ..Default::default()
        }),
        ..Default::default()
    };

    let busy_member_msg = DescriptorProto {
        name: Some("BusyMemberProfile".to_string()),
        field: vec![
            field("member_id", 1, Type::String, Label::Optional, None),
            field("email", 2, Type::String, Label::Optional, None),
            field("tenant_id", 3, Type::String, Label::Optional, None),
            field("phone", 4, Type::String, Label::Optional, None),
            field("external_id", 5, Type::String, Label::Optional, None),
            field("status", 6, Type::String, Label::Optional, None),
            field("tier", 7, Type::String, Label::Optional, None),
            field("balance_cents", 8, Type::Int64, Label::Optional, None),
            field("churn_count", 9, Type::Uint64, Label::Optional, None),
            field("last_churn_at", 10, Type::Uint64, Label::Optional, None),
            field("armed", 11, Type::Bool, Label::Optional, None),
            field("generation", 12, Type::Uint64, Label::Optional, None),
            field(
                "metadata",
                15,
                Type::Message,
                Label::Repeated,
                Some(".previ.bench.BusyMemberProfile.MetadataEntry"),
            ),
        ],
        nested_type: vec![map_entry],
        ..Default::default()
    };

    let file = FileDescriptorProto {
        name: Some("busy_member.proto".to_string()),
        package: Some("previ.bench".to_string()),
        message_type: vec![busy_member_msg],
        syntax: Some("proto3".to_string()),
        ..Default::default()
    };

    let fds = FileDescriptorSet { file: vec![file] };
    fds.encode_to_vec()
}

fn make_service_metadata_with_proto(name: &str) -> ServiceMetadata {
    let fds_bytes = build_busy_member_descriptor_set();
    let fds_b64 = base64::engine::general_purpose::STANDARD.encode(&fds_bytes);
    let mut metadata = HashMap::new();
    metadata.insert(PROTO_DESCRIPTOR_SET_KEY.to_string(), fds_b64);
    metadata.insert(
        format!("{PROTO_STATE_KEY_PREFIX}profile"),
        "previ.bench.BusyMemberProfile".to_string(),
    );

    let mut svc = ServiceMetadata::mock_virtual_object(name, ["seed", "tick", "get_state"]);
    svc.metadata = metadata;
    svc
}

fn make_alpha_service_metadata_with_proto(name: &str, revision: u32) -> ServiceMetadata {
    let field = |fname: &str, num: i32, ty: Type, label: Label, type_name: Option<&str>| {
        FieldDescriptorProto {
            name: Some(fname.to_string()),
            number: Some(num),
            label: Some(label as i32),
            r#type: Some(ty as i32),
            type_name: type_name.map(ToString::to_string),
            ..Default::default()
        }
    };
    let counts_entry = DescriptorProto {
        name: Some("CountsEntry".to_string()),
        field: vec![
            field("key", 1, Type::String, Label::Optional, None),
            field("value", 2, Type::Int64, Label::Optional, None),
        ],
        options: Some(MessageOptions {
            map_entry: Some(true),
            ..Default::default()
        }),
        ..Default::default()
    };
    let item_list = DescriptorProto {
        name: Some("ItemList".to_string()),
        field: vec![
            field("id", 1, Type::Int64, Label::Optional, None),
            field("tags", 2, Type::String, Label::Repeated, None),
            field(
                "counts",
                3,
                Type::Message,
                Label::Repeated,
                Some(".previ.alpha.ItemList.CountsEntry"),
            ),
        ],
        nested_type: vec![counts_entry],
        ..Default::default()
    };
    let fds = FileDescriptorSet {
        file: vec![FileDescriptorProto {
            name: Some("alpha.proto".to_string()),
            package: Some("previ.alpha".to_string()),
            message_type: vec![item_list],
            syntax: Some("proto3".to_string()),
            ..Default::default()
        }],
    };
    let fds_b64 = base64::engine::general_purpose::STANDARD.encode(fds.encode_to_vec());
    let mut svc = ServiceMetadata::mock_virtual_object(name, ["get"]);
    svc.revision = revision;
    svc.metadata
        .insert(PROTO_DESCRIPTOR_SET_KEY.to_string(), fds_b64);
    svc.metadata.insert(
        format!("{PROTO_STATE_KEY_PREFIX}items"),
        "previ.alpha.ItemList".to_string(),
    );
    svc
}

#[restate_core::test(flavor = "multi_thread", worker_threads = 2)]
async fn dynamic_proto_schema_registry_and_sql_udfs() {
    let mut mock_schemas = MockSchemas::default();
    let mut busy_svc = make_service_metadata_with_proto("BusyMember");
    busy_svc.revision = 5;
    mock_schemas.0.add(busy_svc);
    mock_schemas
        .0
        .add(make_alpha_service_metadata_with_proto("AlphaSvc", 3));

    let mut engine = MockQueryEngine::create_with(MockStatusHandle::default(), mock_schemas).await;

    let mut meta = BTreeMap::new();
    meta.insert("region".to_string(), "us-west".to_string());
    meta.insert("department".to_string(), "Engineering".to_string());
    meta.insert("carrier_code".to_string(), "BCBS".to_string());

    let proto_record = BusyMemberProto {
        member_id: "mbr_000543210".to_string(),
        email: "user_543210@tenant_010.com".to_string(),
        tenant_id: "tenant_010".to_string(),
        phone: "+15550543210".to_string(),
        external_id: "EXT-543210".to_string(),
        status: "active".to_string(),
        tier: "enterprise".to_string(),
        balance_cents: 14250,
        churn_count: 87,
        last_churn_at: 1_730_000_000_000,
        armed: true,
        generation: 3,
        metadata: meta,
    };

    let encoded_bytes = proto_record.encode_to_vec();
    {
        let mut tx = engine.partition_store().transaction();
        tx.put_user_state(
            &ServiceId::new(None, "BusyMember", "mbr_000543210"),
            &Bytes::from_static(b"profile"),
            &encoded_bytes,
        )
        .unwrap();
        tx.commit().await.unwrap();
    }

    // 1. Verify full sys_proto_schemas introspection table (sort order, revisions, fields_json, projections, COUNT, LIMIT)
    let schema_batches: Vec<RecordBatch> = engine
        .execute(
            "SELECT service_name, revision, state_key, message_name, fields_json
             FROM sys_proto_schemas",
        )
        .await
        .unwrap()
        .stream
        .map(Result::unwrap)
        .collect()
        .await;
    assert_eq!(schema_batches.len(), 1);
    let sb = &schema_batches[0];
    assert_eq!(sb.num_rows(), 2);
    let svc_col = sb
        .column_by_name("service_name")
        .unwrap()
        .as_any()
        .downcast_ref::<LargeStringArray>()
        .unwrap();
    let rev_col = sb
        .column_by_name("revision")
        .unwrap()
        .as_any()
        .downcast_ref::<UInt64Array>()
        .unwrap();
    let state_key_col = sb
        .column_by_name("state_key")
        .unwrap()
        .as_any()
        .downcast_ref::<LargeStringArray>()
        .unwrap();
    let msg_col = sb
        .column_by_name("message_name")
        .unwrap()
        .as_any()
        .downcast_ref::<LargeStringArray>()
        .unwrap();
    let fields_col = sb
        .column_by_name("fields_json")
        .unwrap()
        .as_any()
        .downcast_ref::<LargeStringArray>()
        .unwrap();

    assert_eq!(svc_col.value(0), "AlphaSvc");
    assert_eq!(svc_col.value(1), "BusyMember");
    assert_eq!(rev_col.value(0), 3);
    assert_eq!(rev_col.value(1), 5);
    assert_eq!(state_key_col.value(0), "items");
    assert_eq!(state_key_col.value(1), "profile");
    assert_eq!(msg_col.value(0), "previ.alpha.ItemList");
    assert_eq!(msg_col.value(1), "previ.bench.BusyMemberProfile");

    for row_idx in 0..2 {
        let parsed: Vec<serde_json::Value> =
            serde_json::from_str(fields_col.value(row_idx)).unwrap();
        let numbers: Vec<u64> = parsed
            .iter()
            .map(|entry| entry["number"].as_u64().unwrap())
            .collect();
        assert!(numbers.windows(2).all(|w| w[0] < w[1]));
        if row_idx == 0 {
            let tags = parsed.iter().find(|e| e["name"] == "tags").unwrap();
            assert_eq!(tags["repeated"], true);
            assert_eq!(tags["map"], false);
            let counts = parsed.iter().find(|e| e["name"] == "counts").unwrap();
            assert_eq!(counts["repeated"], true);
            assert_eq!(counts["map"], true);
        } else {
            let metadata = parsed.iter().find(|e| e["name"] == "metadata").unwrap();
            assert_eq!(metadata["repeated"], true);
            assert_eq!(metadata["map"], true);
        }
    }

    let proj_batches: Vec<RecordBatch> = engine
        .execute("SELECT service_name, revision, state_key, message_name FROM sys_proto_schemas")
        .await
        .unwrap()
        .stream
        .map(Result::unwrap)
        .collect()
        .await;
    assert_eq!(proj_batches[0].num_rows(), 2);
    assert_eq!(proj_batches[0].num_columns(), 4);
    assert!(proj_batches[0].column_by_name("fields_json").is_none());

    let count_batches: Vec<RecordBatch> = engine
        .execute("SELECT COUNT(*) AS cnt FROM sys_proto_schemas")
        .await
        .unwrap()
        .stream
        .map(Result::unwrap)
        .collect()
        .await;
    assert_eq!(
        count_batches[0]
            .column_by_name("cnt")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0),
        2
    );

    let limit0_batches: Vec<RecordBatch> = engine
        .execute("SELECT service_name FROM sys_proto_schemas LIMIT 0")
        .await
        .unwrap()
        .stream
        .map(Result::unwrap)
        .collect()
        .await;
    let limit0_rows: usize = limit0_batches.iter().map(RecordBatch::num_rows).sum();
    assert_eq!(limit0_rows, 0);

    let limit1_batches: Vec<RecordBatch> = engine
        .execute("SELECT service_name FROM sys_proto_schemas LIMIT 1")
        .await
        .unwrap()
        .stream
        .map(Result::unwrap)
        .collect()
        .await;
    let limit1_rows: usize = limit1_batches.iter().map(RecordBatch::num_rows).sum();
    assert_eq!(limit1_rows, 1);
    assert_eq!(
        limit1_batches[0]
            .column_by_name("service_name")
            .unwrap()
            .as_any()
            .downcast_ref::<LargeStringArray>()
            .unwrap()
            .value(0),
        "AlphaSvc"
    );

    // 2. Verify proto_get_str, proto_get_int, proto_get_bool, map path, and proto_to_json
    let query = "SELECT
            service_key,
            proto_get_str(service_name, key, value, 'phone') AS phone,
            proto_get_str(service_name, key, value, 'tenant_id') AS tenant_id,
            proto_get_str(service_name, key, value, 'metadata.department') AS department,
            proto_get_int(service_name, key, value, 'balance_cents') AS balance_cents,
            proto_get_bool(service_name, key, value, 'armed') AS armed,
            proto_to_json(service_name, key, value) AS profile_json
        FROM state
        WHERE scope IS NULL AND service_name = 'BusyMember' AND service_key = 'mbr_000543210'";

    let batches: Vec<RecordBatch> = engine
        .execute(query)
        .await
        .unwrap()
        .stream
        .map(Result::unwrap)
        .collect()
        .await;
    assert_eq!(batches.len(), 1);
    let b = &batches[0];
    assert_eq!(b.num_rows(), 1);

    let phone = b
        .column_by_name("phone")
        .unwrap()
        .as_any()
        .downcast_ref::<LargeStringArray>()
        .unwrap()
        .value(0);
    let tenant = b
        .column_by_name("tenant_id")
        .unwrap()
        .as_any()
        .downcast_ref::<LargeStringArray>()
        .unwrap()
        .value(0);
    let dept = b
        .column_by_name("department")
        .unwrap()
        .as_any()
        .downcast_ref::<LargeStringArray>()
        .unwrap()
        .value(0);
    let balance = b
        .column_by_name("balance_cents")
        .unwrap()
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .value(0);
    let armed = b
        .column_by_name("armed")
        .unwrap()
        .as_any()
        .downcast_ref::<BooleanArray>()
        .unwrap()
        .value(0);
    let json_str = b
        .column_by_name("profile_json")
        .unwrap()
        .as_any()
        .downcast_ref::<LargeStringArray>()
        .unwrap()
        .value(0);

    assert_eq!(phone, "+15550543210");
    assert_eq!(tenant, "tenant_010");
    assert_eq!(dept, "Engineering");
    assert_eq!(balance, 14250);
    assert!(armed);

    let parsed_json: serde_json::Value = serde_json::from_str(json_str).unwrap();
    assert_eq!(parsed_json["member_id"], "mbr_000543210");
    assert_eq!(parsed_json["balance_cents"], 14250);
    assert_eq!(parsed_json["metadata"]["carrier_code"], "BCBS");
}

#[restate_core::test(flavor = "multi_thread", worker_threads = 2)]
async fn multi_batch_sql_queries_and_worker_decode_expr() {
    // 1. Multi-batch SQL query crossing the 128-row batch boundary with JOIN, GROUP BY, aggregates, and scoped state
    let mut mock_schemas = MockSchemas::default();
    let mut busy_svc = make_service_metadata_with_proto("BusyMember");
    busy_svc.revision = 4;
    mock_schemas.0.add(busy_svc);

    let mut engine = MockQueryEngine::create_with(MockStatusHandle::default(), mock_schemas).await;
    let state_key = Bytes::from_static(b"profile");
    let scope_a = Scope::try_non_interned("tenant-a").unwrap();

    // Compute expected group aggregates across 200 rows (120 unscoped + 80 scoped):
    // (scope_is_some, armed) -> (count, sum_balance_cents)
    let mut expected: BTreeMap<(bool, bool), (i64, i64)> = BTreeMap::new();
    {
        let mut tx = engine.partition_store().transaction();
        for i in 0..200usize {
            let scoped = i >= 120;
            let tenant_id = if i % 4 != 3 { "tenant_0" } else { "tenant_1" };
            let armed = i % 2 == 0;
            let balance_cents = 1_000 + (i as i64) * 10;
            if tenant_id == "tenant_0" {
                let entry = expected.entry((scoped, armed)).or_insert((0, 0));
                entry.0 += 1;
                entry.1 += balance_cents;
            }

            let mut meta = BTreeMap::new();
            meta.insert("tier_tag".to_string(), "gold".to_string());
            let rec = BusyMemberProto {
                member_id: format!("mbr_{i:04}"),
                email: format!("u_{i}@{tenant_id}.com"),
                tenant_id: tenant_id.to_string(),
                phone: format!("+1555{i:04}"),
                external_id: format!("EXT-{i:04}"),
                status: "active".to_string(),
                tier: "enterprise".to_string(),
                balance_cents,
                churn_count: 1,
                last_churn_at: 1_730_000_000_000,
                armed,
                generation: 1,
                metadata: meta,
            };
            let service_id = ServiceId::new(
                scoped.then(|| scope_a.clone()),
                "BusyMember",
                format!("mbr_{i:04}"),
            );
            tx.put_user_state(&service_id, &state_key, rec.encode_to_vec())
                .unwrap();
        }
        tx.commit().await.unwrap();
    }

    let agg_batches: Vec<RecordBatch> = engine
        .execute(
            "SELECT
                s.scope,
                p.message_name,
                proto_get_bool(s.service_name, s.key, s.value, 'armed') AS is_armed,
                COUNT(*) AS cnt,
                SUM(proto_get_int(s.service_name, s.key, s.value, 'balance_cents')) AS total_bal,
                AVG(proto_get_float(s.service_name, s.key, s.value, 'balance_cents')) AS avg_bal
            FROM state s
            JOIN sys_proto_schemas p
                ON s.service_name = p.service_name AND s.key = p.state_key
            WHERE s.service_name = 'BusyMember'
              AND proto_get_str(s.service_name, s.key, s.value, 'tenant_id') = 'tenant_0'
            GROUP BY s.scope, p.message_name, proto_get_bool(s.service_name, s.key, s.value, 'armed')
            ORDER BY s.scope NULLS FIRST, is_armed",
        )
        .await
        .unwrap()
        .stream
        .map(Result::unwrap)
        .collect()
        .await;

    let mut total_matched_rows = 0i64;
    let mut actual_groups = BTreeMap::new();
    for batch in &agg_batches {
        let scopes = batch
            .column_by_name("scope")
            .unwrap()
            .as_any()
            .downcast_ref::<LargeStringArray>()
            .unwrap();
        let msgs = batch
            .column_by_name("message_name")
            .unwrap()
            .as_any()
            .downcast_ref::<LargeStringArray>()
            .unwrap();
        let armed_col = batch
            .column_by_name("is_armed")
            .unwrap()
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap();
        let cnt_col = batch
            .column_by_name("cnt")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let sum_col = batch
            .column_by_name("total_bal")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let avg_col = batch
            .column_by_name("avg_bal")
            .unwrap()
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();

        for row in 0..batch.num_rows() {
            assert_eq!(msgs.value(row), "previ.bench.BusyMemberProfile");
            let scope_opt = (!scopes.is_null(row)).then(|| scopes.value(row));
            if let Some(s) = scope_opt {
                assert_eq!(s, "tenant-a");
            }
            let cnt = cnt_col.value(row);
            let total_bal = sum_col.value(row);
            let avg_bal = avg_col.value(row);
            let expected_avg = total_bal as f64 / cnt as f64;
            assert!((avg_bal - expected_avg).abs() < 1e-9);
            total_matched_rows += cnt;
            actual_groups.insert(
                (scope_opt.is_some(), armed_col.value(row)),
                (cnt, total_bal),
            );
        }
    }
    assert_eq!(total_matched_rows, 150);
    assert_eq!(actual_groups, expected);

    let scan_batches: Vec<RecordBatch> = engine
        .execute(
            "SELECT service_key, proto_get_str(service_name, key, value, 'tenant_id') AS tid
             FROM state
             WHERE service_name = 'BusyMember'",
        )
        .await
        .unwrap()
        .stream
        .map(Result::unwrap)
        .collect()
        .await;
    let scanned_rows: usize = scan_batches.iter().map(RecordBatch::num_rows).sum();
    assert_eq!(scanned_rows, 200);

    // 2. Worker-only DataFusionEnv with ONLY create_udfs registered (no MetadataTables/Admin tables)
    let mut worker_schemas = MockSchemas::default();
    worker_schemas
        .0
        .add(make_service_metadata_with_proto("BusyMember"));
    let registry = DynamicProtoSchemaRegistry::new(Live::from_value(worker_schemas));
    let worker_env = DataFusionEnv::new(10 * 1024 * 1024, None, Some(1), &HashMap::new()).unwrap();
    assert!(worker_env.table_provider("sys_proto_schemas").is_none());
    assert_eq!(worker_env.udf_generation(), 0);

    let udfs = create_udfs(registry.clone());
    assert_eq!(udfs.len(), 5);
    for (idx, udf) in udfs.into_iter().enumerate() {
        worker_env.register_udf(udf).unwrap();
        assert_eq!(worker_env.udf_generation(), (idx + 1) as u64);
    }
    let dup_udf = create_udfs(registry).into_iter().next().unwrap();
    assert!(worker_env.register_udf(dup_udf).is_err());
    assert_eq!(worker_env.udf_generation(), 5);

    let input_schema = Arc::new(Schema::new(vec![
        Field::new("service_name", DataType::LargeUtf8, false),
        Field::new("key", DataType::LargeUtf8, false),
        Field::new("value", DataType::LargeBinary, false),
    ]));
    let df_schema = DFSchema::try_from(Arc::clone(&input_schema)).unwrap();
    let planner_state = worker_env.build_session_state().unwrap();
    let proto_get_str_udf = planner_state.udf("proto_get_str").unwrap();
    let logical_pred = proto_get_str_udf
        .call(vec![
            col("service_name"),
            col("key"),
            col("value"),
            lit("tenant_id"),
        ])
        .eq(lit("tenant_1"));
    let physical_pred = planner_state
        .create_physical_expr(logical_pred, &df_schema)
        .unwrap();
    let encoded = crate::encode_expr(&physical_pred).unwrap();
    let decoded = crate::decode_expr(
        &worker_env.build_session_state().unwrap().task_ctx(),
        &input_schema,
        &encoded,
    )
    .unwrap();

    let make_row = |tid: &str| {
        BusyMemberProto {
            member_id: "m".to_string(),
            email: "e".to_string(),
            tenant_id: tid.to_string(),
            phone: "p".to_string(),
            external_id: "x".to_string(),
            status: "s".to_string(),
            tier: "t".to_string(),
            balance_cents: 1,
            churn_count: 1,
            last_churn_at: 1,
            armed: false,
            generation: 1,
            metadata: BTreeMap::new(),
        }
        .encode_to_vec()
    };
    let batch = RecordBatch::try_new(
        input_schema,
        vec![
            Arc::new(LargeStringArray::from(vec!["BusyMember", "BusyMember"])) as ArrayRef,
            Arc::new(LargeStringArray::from(vec!["profile", "profile"])) as ArrayRef,
            Arc::new(LargeBinaryArray::from(vec![
                make_row("tenant_1").as_slice(),
                make_row("tenant_0").as_slice(),
            ])) as ArrayRef,
        ],
    )
    .unwrap();
    let eval_arr = decoded
        .evaluate(&batch)
        .unwrap()
        .into_array(batch.num_rows())
        .unwrap();
    let mask = eval_arr.as_any().downcast_ref::<BooleanArray>().unwrap();
    assert!(mask.value(0));
    assert!(!mask.value(1));
}

#[restate_core::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "manual benchmark"]
async fn benchmark_proto_vs_json_state_scan_in_rocksdb() {
    let mut mock_schemas = MockSchemas::default();
    mock_schemas
        .0
        .add(make_service_metadata_with_proto("BusyMemberProtoSvc"));

    let mut engine = MockQueryEngine::create_with(MockStatusHandle::default(), mock_schemas).await;

    let num_records: usize = 50_000;
    let mut total_proto_bytes: usize = 0;
    let mut total_json_bytes: usize = 0;
    let state_key = Bytes::from_static(b"profile");

    {
        let mut tx = engine.partition_store().transaction();
        for i in 0..num_records {
            let tid = format!("tenant_{:03}", i % 100);
            let mbr_id = format!("mbr_{i:09}");
            let phone = format!("+1555{i:07}");
            let bal = 10_000 + ((i * 37) % 15_000) as i64;

            let mut meta = BTreeMap::new();
            meta.insert("region".to_string(), "us-west".to_string());
            meta.insert("department".to_string(), "Engineering".to_string());
            meta.insert("carrier_code".to_string(), "BCBS".to_string());
            meta.insert("plan_code".to_string(), "PLN-BCBS-US-WEST-2026".to_string());
            meta.insert(
                "notes".to_string(),
                "Previ B2B employer-sponsored benefit line eligible for automated carrier reconciliation, monthly payroll deduction ledger sync, and split-phase Restate durable state transitions.".to_string(),
            );

            let proto_rec = BusyMemberProto {
                member_id: mbr_id.clone(),
                email: format!("user_{i}@{tid}.com"),
                tenant_id: tid,
                phone,
                external_id: format!("EXT-{i:06}"),
                status: if i % 10 == 0 {
                    "waived".to_string()
                } else {
                    "active".to_string()
                },
                tier: "enterprise".to_string(),
                balance_cents: bal,
                churn_count: 1,
                last_churn_at: 1_730_000_000_000,
                armed: false,
                generation: 1,
                metadata: meta,
            };
            let p_bytes = proto_rec.encode_to_vec();
            total_proto_bytes += p_bytes.len();
            tx.put_user_state(
                &ServiceId::new(None, "BusyMemberProtoSvc", mbr_id.as_str()),
                &state_key,
                &p_bytes,
            )
            .unwrap();
        }
        tx.commit().await.unwrap();
    }

    let proto_q3 = "SELECT count(*) AS total, sum(proto_get_int(service_name, key, value, 'balance_cents')) AS balance
        FROM state
        WHERE service_name = 'BusyMemberProtoSvc'
          AND proto_get_str(service_name, key, value, 'tenant_id') = 'tenant_012'";

    // Warm up Protobuf scan
    let _ = engine
        .execute(proto_q3)
        .await
        .unwrap()
        .stream
        .collect::<Vec<_>>()
        .await;

    let t0 = Instant::now();
    let proto_res: Vec<RecordBatch> = engine
        .execute(proto_q3)
        .await
        .unwrap()
        .stream
        .map(Result::unwrap)
        .collect()
        .await;
    let proto_ms = t0.elapsed().as_secs_f64() * 1000.0;

    // Now overwrite the exact same 50,000 keys with JSON payloads so RocksDB holds 50k JSON rows
    {
        let mut tx = engine.partition_store().transaction();
        for i in 0..num_records {
            let tid = format!("tenant_{:03}", i % 100);
            let mbr_id = format!("mbr_{i:09}");
            let phone = format!("+1555{i:07}");
            let bal = 10_000 + ((i * 37) % 15_000) as i64;
            let mut meta = BTreeMap::new();
            meta.insert("region".to_string(), "us-west".to_string());
            meta.insert("department".to_string(), "Engineering".to_string());
            meta.insert("carrier_code".to_string(), "BCBS".to_string());
            meta.insert("plan_code".to_string(), "PLN-BCBS-US-WEST-2026".to_string());
            meta.insert(
                "notes".to_string(),
                "Previ B2B employer-sponsored benefit line eligible for automated carrier reconciliation, monthly payroll deduction ledger sync, and split-phase Restate durable state transitions.".to_string(),
            );
            let j_bytes = serde_json::to_vec(&serde_json::json!({
                "member_id": mbr_id,
                "email": format!("user_{i}@{tid}.com"),
                "tenant_id": tid,
                "phone": phone,
                "external_id": format!("EXT-{i:06}"),
                "status": if i % 10 == 0 { "waived" } else { "active" },
                "tier": "enterprise",
                "balance_cents": bal,
                "churn_count": 1,
                "last_churn_at": 1_730_000_000_000u64,
                "armed": false,
                "generation": 1,
                "metadata": meta,
            }))
            .unwrap();
            total_json_bytes += j_bytes.len();

            tx.put_user_state(
                &ServiceId::new(None, "BusyMemberProtoSvc", mbr_id.as_str()),
                &state_key,
                &j_bytes,
            )
            .unwrap();
        }
        tx.commit().await.unwrap();
    }

    let json_q3 =
        "SELECT count(*) AS total, sum(json_get_int(value_utf8, 'balance_cents')) AS balance
        FROM state
        WHERE service_name = 'BusyMemberProtoSvc'
          AND json_get_str(value_utf8, 'tenant_id') = 'tenant_012'";

    let _ = engine
        .execute(json_q3)
        .await
        .unwrap()
        .stream
        .collect::<Vec<_>>()
        .await;

    let t1 = Instant::now();
    let json_res: Vec<RecordBatch> = engine
        .execute(json_q3)
        .await
        .unwrap()
        .stream
        .map(Result::unwrap)
        .collect()
        .await;
    let json_ms = t1.elapsed().as_secs_f64() * 1000.0;

    let p_count = proto_res[0]
        .column_by_name("total")
        .unwrap()
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .value(0);
    let p_bal = proto_res[0]
        .column_by_name("balance")
        .unwrap()
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .value(0);
    let j_count = json_res[0]
        .column_by_name("total")
        .unwrap()
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .value(0);
    let j_bal = json_res[0]
        .column_by_name("balance")
        .unwrap()
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .value(0);

    assert_eq!(p_count, 500);
    assert_eq!(p_count, j_count);
    assert_eq!(p_bal, j_bal);

    eprintln!(
        "\n=== ROCKSDB STATE SCAN BENCHMARK ({num_records} records per format) ===\n\
         Protobuf total size: {:.2} MB ({:.1} B/row) vs JSON total size: {:.2} MB ({:.1} B/row)\n\
         Protobuf Q3 Scan:    {:.2} ms\n\
         JSON Q3 Scan:        {:.2} ms\n\
         Speedup:             {:.2}x faster, {:.2}x smaller in RocksDB\n",
        total_proto_bytes as f64 / (1024.0 * 1024.0),
        total_proto_bytes as f64 / num_records as f64,
        total_json_bytes as f64 / (1024.0 * 1024.0),
        total_json_bytes as f64 / num_records as f64,
        proto_ms,
        json_ms,
        json_ms / proto_ms,
        total_json_bytes as f64 / total_proto_bytes as f64,
    );
}

#[test]
fn wire_safety_packed_repeated_maps_last_wins_and_numeric_coercion() {
    let field = |name: &str, num: i32, ty: Type, label: Label, type_name: Option<&str>| {
        FieldDescriptorProto {
            name: Some(name.to_string()),
            number: Some(num),
            label: Some(label as i32),
            r#type: Some(ty as i32),
            type_name: type_name.map(ToString::to_string),
            ..Default::default()
        }
    };

    let int_map_entry = DescriptorProto {
        name: Some("IntLabelsEntry".to_string()),
        field: vec![
            field("key", 1, Type::Int32, Label::Optional, None),
            field("value", 2, Type::String, Label::Optional, None),
        ],
        options: Some(MessageOptions {
            map_entry: Some(true),
            ..Default::default()
        }),
        ..Default::default()
    };
    let counters_map_entry = DescriptorProto {
        name: Some("CountersEntry".to_string()),
        field: vec![
            field("key", 1, Type::String, Label::Optional, None),
            field("value", 2, Type::Sint64, Label::Optional, None),
        ],
        options: Some(MessageOptions {
            map_entry: Some(true),
            ..Default::default()
        }),
        ..Default::default()
    };
    let flags_map_entry = DescriptorProto {
        name: Some("FlagsEntry".to_string()),
        field: vec![
            field("key", 1, Type::String, Label::Optional, None),
            field("value", 2, Type::Bool, Label::Optional, None),
        ],
        options: Some(MessageOptions {
            map_entry: Some(true),
            ..Default::default()
        }),
        ..Default::default()
    };
    let weights_map_entry = DescriptorProto {
        name: Some("WeightsEntry".to_string()),
        field: vec![
            field("key", 1, Type::String, Label::Optional, None),
            field("value", 2, Type::Double, Label::Optional, None),
        ],
        options: Some(MessageOptions {
            map_entry: Some(true),
            ..Default::default()
        }),
        ..Default::default()
    };
    let node_map_entry = DescriptorProto {
        name: Some("NodeMapEntry".to_string()),
        field: vec![
            field("key", 1, Type::String, Label::Optional, None),
            field(
                "value",
                2,
                Type::Message,
                Label::Optional,
                Some(".previ.test.SubNode"),
            ),
        ],
        options: Some(MessageOptions {
            map_entry: Some(true),
            ..Default::default()
        }),
        ..Default::default()
    };
    let sub_msg = DescriptorProto {
        name: Some("SubNode".to_string()),
        field: vec![
            field("left", 1, Type::Int64, Label::Optional, None),
            field("right", 2, Type::Int64, Label::Optional, None),
            field(
                "child",
                3,
                Type::Message,
                Label::Optional,
                Some(".previ.test.SubNode"),
            ),
        ],
        ..Default::default()
    };
    let status_enum = EnumDescriptorProto {
        name: Some("Mode".to_string()),
        value: vec![
            EnumValueDescriptorProto {
                name: Some("MODE_UNSPECIFIED".to_string()),
                number: Some(0),
                ..Default::default()
            },
            EnumValueDescriptorProto {
                name: Some("MODE_FAST".to_string()),
                number: Some(1),
                ..Default::default()
            },
            EnumValueDescriptorProto {
                name: Some("MODE_SAFE".to_string()),
                number: Some(2),
                ..Default::default()
            },
        ],
        ..Default::default()
    };
    let mut opt_int_field = field("opt_quota", 12, Type::Int64, Label::Optional, None);
    opt_int_field.proto3_optional = Some(true);
    opt_int_field.oneof_index = Some(1);
    let mut oneof_a = field("choice_a", 13, Type::Int64, Label::Optional, None);
    oneof_a.oneof_index = Some(0);
    let mut oneof_b = field("choice_b", 14, Type::String, Label::Optional, None);
    oneof_b.oneof_index = Some(0);

    let root_msg = DescriptorProto {
        name: Some("TestEnvelope".to_string()),
        field: vec![
            field("label", 1, Type::String, Label::Optional, None),
            field("seq", 2, Type::Int64, Label::Optional, None),
            field("delta", 3, Type::Sint64, Label::Optional, None),
            field("big_u64", 4, Type::Uint64, Label::Optional, None),
            field("scores", 5, Type::Int32, Label::Repeated, None),
            field(
                "modes",
                6,
                Type::Enum,
                Label::Repeated,
                Some(".previ.test.Mode"),
            ),
            field(
                "int_labels",
                7,
                Type::Message,
                Label::Repeated,
                Some(".previ.test.TestEnvelope.IntLabelsEntry"),
            ),
            field(
                "counters",
                8,
                Type::Message,
                Label::Repeated,
                Some(".previ.test.TestEnvelope.CountersEntry"),
            ),
            field(
                "flags",
                9,
                Type::Message,
                Label::Repeated,
                Some(".previ.test.TestEnvelope.FlagsEntry"),
            ),
            field(
                "weights",
                10,
                Type::Message,
                Label::Repeated,
                Some(".previ.test.TestEnvelope.WeightsEntry"),
            ),
            field(
                "sub",
                11,
                Type::Message,
                Label::Optional,
                Some(".previ.test.SubNode"),
            ),
            opt_int_field,
            oneof_a,
            oneof_b,
            field(
                "node_map",
                15,
                Type::Message,
                Label::Repeated,
                Some(".previ.test.TestEnvelope.NodeMapEntry"),
            ),
        ],
        nested_type: vec![
            int_map_entry,
            counters_map_entry,
            flags_map_entry,
            weights_map_entry,
            node_map_entry,
        ],
        ..Default::default()
    };

    let fds = FileDescriptorSet {
        file: vec![FileDescriptorProto {
            name: Some("test_envelope.proto".to_string()),
            package: Some("previ.test".to_string()),
            message_type: vec![root_msg, sub_msg],
            enum_type: vec![status_enum],
            syntax: Some("proto3".to_string()),
            ..Default::default()
        }],
    };
    let pool = CompiledDescriptorPool::from_file_descriptor_set(&fds).unwrap();
    let msg = pool
        .resolve_message("previ.test.TestEnvelope")
        .unwrap()
        .clone();
    let sub_schema = pool.resolve_message("previ.test.SubNode").unwrap().clone();

    // 1. Overflow-safe length bounds check (pos = 11 + (u64::MAX - 10) wraps to 0 if unchecked):
    //    Tag 99 (unmatched, wire_type = 2 -> key = (99 << 3) | 2 = 794 -> varint [0x9a, 0x06])
    //    Wait: 1-byte tag (e.g. field 15, wire_type 2 -> 0x7a) + 10-byte varint for u64::MAX - 10 (0xfffffffffffffff5):
    let mut overflow_skip_buf = vec![0x7a];
    let mut v = u64::MAX - 10;
    while v >= 0x80 {
        overflow_skip_buf.push(((v as u8) & 0x7f) | 0x80);
        v >>= 7;
    }
    overflow_skip_buf.push(v as u8);
    let label_steps = msg.resolve_path("label", &pool).unwrap();
    let seq_steps = msg.resolve_path("seq", &pool).unwrap();
    assert_eq!(extract_str(&overflow_skip_buf, &label_steps, &pool), None);
    assert_eq!(extract_i64(&overflow_skip_buf, &seq_steps, &pool), None);
    assert_eq!(transcode_to_json(&overflow_skip_buf, &msg, &pool), None);

    // Same wrapping varint on a matched string field (tag 1, wire_type 2 -> 0x0a):
    let mut overflow_str_buf = overflow_skip_buf.clone();
    overflow_str_buf[0] = 0x0a;
    assert_eq!(extract_str(&overflow_str_buf, &label_steps, &pool), None);

    // 2. Bounded recursion depth in transcode_to_json for self-referential SubNode (field 3 -> 0x1a):
    let mut deep_buf = vec![0x08, 0x2a]; // left = 42 at leaf
    for _ in 0..80 {
        let mut wrapped = vec![0x1a];
        prost::encoding::encode_varint(deep_buf.len() as u64, &mut wrapped);
        wrapped.extend_from_slice(&deep_buf);
        deep_buf = wrapped;
    }
    assert_eq!(transcode_to_json(&deep_buf, &sub_schema, &pool), None);

    // Shallow nesting (3 levels) succeeds:
    let mut shallow_buf = vec![0x08, 0x2a];
    for _ in 0..3 {
        let mut wrapped = vec![0x1a, shallow_buf.len() as u8];
        wrapped.extend_from_slice(&shallow_buf);
        shallow_buf = wrapped;
    }
    let shallow_json = transcode_to_json(&shallow_buf, &sub_schema, &pool).unwrap();
    assert_eq!(shallow_json["child"]["child"]["child"]["left"], 42);

    // 3. Packed repeated scalars, non-string map keys, typed map getters, last-wins, and float coercion:
    #[derive(Clone, PartialEq, prost::Message)]
    struct SubNodeProto {
        #[prost(int64, tag = "1")]
        left: i64,
        #[prost(int64, tag = "2")]
        right: i64,
        #[prost(message, optional, boxed, tag = "3")]
        child: Option<Box<SubNodeProto>>,
    }

    #[derive(Clone, PartialEq, prost::Message)]
    struct EnvelopeProto {
        #[prost(string, tag = "1")]
        label: String,
        #[prost(int64, tag = "2")]
        seq: i64,
        #[prost(sint64, tag = "3")]
        delta: i64,
        #[prost(uint64, tag = "4")]
        big_u64: u64,
        #[prost(int32, repeated, packed = "true", tag = "5")]
        scores: Vec<i32>,
        #[prost(int32, repeated, packed = "true", tag = "6")]
        modes: Vec<i32>,
        #[prost(btree_map = "int32, string", tag = "7")]
        int_labels: BTreeMap<i32, String>,
        #[prost(btree_map = "string, sint64", tag = "8")]
        counters: BTreeMap<String, i64>,
        #[prost(btree_map = "string, bool", tag = "9")]
        flags: BTreeMap<String, bool>,
        #[prost(btree_map = "string, double", tag = "10")]
        weights: BTreeMap<String, f64>,
        #[prost(message, optional, tag = "11")]
        sub: Option<SubNodeProto>,
        #[prost(int64, optional, tag = "12")]
        opt_quota: Option<i64>,
        #[prost(int64, optional, tag = "13")]
        choice_a: Option<i64>,
        #[prost(string, optional, tag = "14")]
        choice_b: Option<String>,
        #[prost(btree_map = "string, message", tag = "15")]
        node_map: BTreeMap<String, SubNodeProto>,
    }

    // Reject non-map repeated fields and leaf message/map paths at compile time:
    assert!(msg.resolve_path("scores", &pool).is_none());
    assert!(msg.resolve_path("sub", &pool).is_none());
    assert!(msg.resolve_path("counters", &pool).is_none());
    assert!(msg.resolve_path("node_map.primary", &pool).is_none());

    // Reject illegal wire tag 0 (`[0x00, 0x00]`):
    assert_eq!(extract_i64(&[0x00, 0x00], &seq_steps, &pool), None);
    assert_eq!(transcode_to_json(&[0x00, 0x00], &msg, &pool), None);

    let mut int_labels = BTreeMap::new();
    int_labels.insert(1, "one".to_string());
    int_labels.insert(2, "two".to_string());

    let mut counters = BTreeMap::new();
    counters.insert("requests".to_string(), 42);
    counters.insert("negative".to_string(), -9);
    counters.insert("zero_val".to_string(), 0);

    let mut flags = BTreeMap::new();
    flags.insert("enabled".to_string(), true);
    flags.insert("disabled".to_string(), false);

    let mut weights = BTreeMap::new();
    weights.insert("alpha".to_string(), 3.25);
    weights.insert("nan_val".to_string(), f64::NAN);

    let mut node_map_1 = BTreeMap::new();
    node_map_1.insert(
        "primary".to_string(),
        SubNodeProto {
            left: 10,
            right: 20,
            child: None,
        },
    );

    let big_u64_val = (i64::MAX as u64) + 1024;
    let first_part = EnvelopeProto {
        label: "initial".to_string(),
        seq: 1,
        delta: -1,
        big_u64: big_u64_val,
        scores: vec![10, 20, 30],
        modes: vec![1, 2],
        int_labels,
        counters,
        flags,
        weights,
        sub: Some(SubNodeProto {
            left: 100,
            right: 200,
            child: Some(Box::new(SubNodeProto {
                left: 555,
                right: 0,
                child: None,
            })),
        }),
        opt_quota: None,
        choice_a: Some(77),
        choice_b: None,
        node_map: node_map_1,
    };

    // Append a second occurrence overwriting `label`, `seq`, `counters["requests"]`, `sub.right`
    // (while omitting `sub.child`!), clearing `choice_a` via `choice_b`, and replacing `node_map["primary"]`:
    let mut override_counters = BTreeMap::new();
    override_counters.insert("requests".to_string(), 99);
    let mut node_map_2 = BTreeMap::new();
    node_map_2.insert(
        "primary".to_string(),
        SubNodeProto {
            left: 0,
            right: 99,
            child: None,
        },
    );
    let second_part = EnvelopeProto {
        label: "updated".to_string(),
        seq: 2,
        delta: -1,
        big_u64: big_u64_val,
        scores: vec![],
        modes: vec![],
        int_labels: BTreeMap::new(),
        counters: override_counters,
        flags: BTreeMap::new(),
        weights: BTreeMap::new(),
        sub: Some(SubNodeProto {
            left: 0,
            right: 777,
            child: None,
        }),
        opt_quota: None,
        choice_a: None,
        choice_b: Some("winner".to_string()),
        node_map: node_map_2,
    };

    let mut merged_bytes = first_part.encode_to_vec();
    merged_bytes.extend_from_slice(&second_part.encode_to_vec());

    // Last-wins for singular scalars, multi-level submessages (sub.child.left survives!), oneof clearing, and optional presence:
    assert_eq!(
        extract_str(&merged_bytes, &label_steps, &pool).as_deref(),
        Some("updated")
    );
    assert_eq!(extract_i64(&merged_bytes, &seq_steps, &pool), Some(2));
    let sub_left_steps = msg.resolve_path("sub.left", &pool).unwrap();
    let sub_right_steps = msg.resolve_path("sub.right", &pool).unwrap();
    let sub_child_left_steps = msg.resolve_path("sub.child.left", &pool).unwrap();
    let opt_quota_steps = msg.resolve_path("opt_quota", &pool).unwrap();
    let choice_a_steps = msg.resolve_path("choice_a", &pool).unwrap();
    let choice_b_steps = msg.resolve_path("choice_b", &pool).unwrap();
    let map_sub_left_steps = msg.resolve_path("node_map.primary.left", &pool).unwrap();
    let map_sub_right_steps = msg.resolve_path("node_map.primary.right", &pool).unwrap();

    assert_eq!(
        extract_i64(&merged_bytes, &sub_left_steps, &pool),
        Some(100)
    );
    assert_eq!(
        extract_i64(&merged_bytes, &sub_right_steps, &pool),
        Some(777)
    );
    assert_eq!(
        extract_i64(&merged_bytes, &sub_child_left_steps, &pool),
        Some(555)
    );
    assert_eq!(extract_i64(&merged_bytes, &opt_quota_steps, &pool), None);
    assert_eq!(extract_i64(&merged_bytes, &choice_a_steps, &pool), None);
    assert_eq!(
        extract_str(&merged_bytes, &choice_b_steps, &pool).as_deref(),
        Some("winner")
    );
    // Duplicate map<string, SubNode> entry replaces rather than merging (`left` becomes 0, not 10!):
    assert_eq!(
        extract_i64(&merged_bytes, &map_sub_left_steps, &pool),
        Some(0)
    );
    assert_eq!(
        extract_i64(&merged_bytes, &map_sub_right_steps, &pool),
        Some(99)
    );

    // Typed map lookups (including int/float/bool, zero-valued entries, last-wins, and missing keys):
    let req_steps = msg.resolve_path("counters.requests", &pool).unwrap();
    let neg_steps = msg.resolve_path("counters.negative", &pool).unwrap();
    let zero_steps = msg.resolve_path("counters.zero_val", &pool).unwrap();
    let missing_steps = msg.resolve_path("counters.missing", &pool).unwrap();
    let flag_t_steps = msg.resolve_path("flags.enabled", &pool).unwrap();
    let flag_f_steps = msg.resolve_path("flags.disabled", &pool).unwrap();
    let flag_missing_steps = msg.resolve_path("flags.unknown", &pool).unwrap();
    let weight_steps = msg.resolve_path("weights.alpha", &pool).unwrap();
    let int_map_1_steps = msg.resolve_path("int_labels.1", &pool).unwrap();
    let int_map_2_steps = msg.resolve_path("int_labels.2", &pool).unwrap();

    assert_eq!(extract_i64(&merged_bytes, &req_steps, &pool), Some(99));
    assert_eq!(extract_i64(&merged_bytes, &neg_steps, &pool), Some(-9));
    assert_eq!(extract_f64(&merged_bytes, &neg_steps, &pool), Some(-9.0));
    assert_eq!(extract_i64(&merged_bytes, &zero_steps, &pool), Some(0));
    assert_eq!(extract_i64(&merged_bytes, &missing_steps, &pool), None);
    assert_eq!(
        extract_bool(&merged_bytes, &flag_t_steps, &pool),
        Some(true)
    );
    assert_eq!(
        extract_bool(&merged_bytes, &flag_f_steps, &pool),
        Some(false)
    );
    assert_eq!(
        extract_bool(&merged_bytes, &flag_missing_steps, &pool),
        None
    );
    assert_eq!(extract_f64(&merged_bytes, &weight_steps, &pool), Some(3.25));
    assert_eq!(
        extract_str(&merged_bytes, &int_map_1_steps, &pool).as_deref(),
        Some("one")
    );
    assert_eq!(
        extract_str(&merged_bytes, &int_map_2_steps, &pool).as_deref(),
        Some("two")
    );

    // Respect integer kinds when converting varints to ints/floats (uint64 > i64::MAX -> None in i64, positive in f64):
    let delta_steps = msg.resolve_path("delta", &pool).unwrap();
    let big_u64_steps = msg.resolve_path("big_u64", &pool).unwrap();
    assert_eq!(extract_i64(&merged_bytes, &big_u64_steps, &pool), None);
    assert_eq!(extract_f64(&merged_bytes, &delta_steps, &pool), Some(-1.0));
    assert_eq!(
        extract_f64(&merged_bytes, &big_u64_steps, &pool),
        Some(big_u64_val as f64)
    );

    // JSON transcoding of packed repeated scalars, non-string map keys, NaN floats, oneof clearing, and map<K,SubMsg> replacement:
    let transcoded = transcode_to_json(&merged_bytes, &msg, &pool).unwrap();
    assert_eq!(transcoded["label"], "updated");
    assert_eq!(transcoded["seq"], 2);
    assert_eq!(transcoded["scores"], serde_json::json!([10, 20, 30]));
    assert_eq!(
        transcoded["modes"],
        serde_json::json!(["MODE_FAST", "MODE_SAFE"])
    );
    assert_eq!(
        transcoded["int_labels"],
        serde_json::json!({"1": "one", "2": "two"})
    );
    assert_eq!(transcoded["counters"]["requests"], 99);
    assert_eq!(transcoded["counters"]["negative"], -9);
    assert_eq!(transcoded["counters"]["zero_val"], 0);
    assert_eq!(transcoded["weights"]["nan_val"], "NaN");
    assert_eq!(transcoded["sub"]["left"], 100);
    assert_eq!(transcoded["sub"]["right"], 777);
    assert_eq!(transcoded["sub"]["child"]["left"], 555);
    assert!(transcoded.get("choice_a").is_none());
    assert_eq!(transcoded["choice_b"], "winner");
    assert_eq!(
        transcoded["node_map"]["primary"],
        serde_json::json!({"right": 99})
    );

    // 4. Bounded path_cache, no negative caching, and >512B path bypass:
    let before_neg = msg.cached_path_count();
    for i in 0..500 {
        assert!(
            msg.resolve_path(&format!("nonexistent_{i}"), &pool)
                .is_none()
        );
    }
    assert_eq!(msg.cached_path_count(), before_neg);

    let long_valid_path = format!("counters.{}", "k".repeat(520));
    assert!(msg.resolve_path(&long_valid_path, &pool).is_some());
    assert_eq!(msg.cached_path_count(), before_neg);

    for i in 0..400 {
        assert!(
            msg.resolve_path(&format!("counters.dyn_{i}"), &pool)
                .is_some()
        );
    }
    assert!(msg.cached_path_count() <= MAX_CACHED_PATHS_PER_MESSAGE);
    assert!(msg.resolve_path("counters.after_cap", &pool).is_some());
}

fn build_nested_descriptor_wire(levels: usize) -> Vec<u8> {
    let mut msg = vec![0x0a, 0x01, b'X']; // DescriptorProto { name: "X" }
    for _ in 1..levels {
        let mut outer = vec![0x1a]; // nested_type (field 3, wire_type 2)
        prost::encoding::encode_varint(msg.len() as u64, &mut outer);
        outer.extend_from_slice(&msg);
        msg = outer;
    }
    let mut file = vec![0x22]; // FileDescriptorProto.message_type (field 4, wire_type 2)
    prost::encoding::encode_varint(msg.len() as u64, &mut file);
    file.extend_from_slice(&msg);
    let mut fds = vec![0x0a]; // FileDescriptorSet.file (field 1, wire_type 2)
    prost::encoding::encode_varint(file.len() as u64, &mut fds);
    fds.extend_from_slice(&file);
    fds
}

#[test]
fn wire_compliance_dos_presence_all_kinds_and_cache_bounds() {
    // 1. Stack-overflow DoS regression & wire group rejection inside FileDescriptorSet
    for levels in [40usize, 5_000usize] {
        let deep_fds_wire = build_nested_descriptor_wire(levels);
        assert!(!validate_descriptor_set_wire_nesting(
            &deep_fds_wire,
            MAX_DESCRIPTOR_NESTING
        ));
        let mut svc = ServiceMetadata::mock_virtual_object("DeepSvc", ["get"]);
        svc.metadata.insert(
            PROTO_DESCRIPTOR_SET_KEY.to_string(),
            base64::engine::general_purpose::STANDARD.encode(&deep_fds_wire),
        );
        svc.metadata
            .insert(format!("{PROTO_STATE_KEY_PREFIX}state"), "X".to_string());
        assert!(ServiceProtoSchemas::from_service_metadata(&svc).is_none());
    }

    // Wire SGROUP(3)/EGROUP(4) inside FileDescriptorProto and DescriptorProto rejected:
    let group_in_file = vec![0x0a, 0x04, 0x9b, 0x06, 0x9c, 0x06];
    assert!(!validate_descriptor_set_wire_nesting(
        &group_in_file,
        MAX_DESCRIPTOR_NESTING
    ));
    let group_in_msg = vec![0x0a, 0x06, 0x22, 0x04, 0x9b, 0x06, 0x9c, 0x06];
    assert!(!validate_descriptor_set_wire_nesting(
        &group_in_msg,
        MAX_DESCRIPTOR_NESTING
    ));
    let mut group_svc = ServiceMetadata::mock_virtual_object("GroupSvc", ["get"]);
    group_svc.metadata.insert(
        PROTO_DESCRIPTOR_SET_KEY.to_string(),
        base64::engine::general_purpose::STANDARD.encode(&group_in_msg),
    );
    group_svc
        .metadata
        .insert(format!("{PROTO_STATE_KEY_PREFIX}state"), "X".to_string());
    assert!(ServiceProtoSchemas::from_service_metadata(&group_svc).is_none());

    // 2. PoolBuilder fail-closed on MAX_POOL_MESSAGES and MAX_IDENTIFIER_LEN
    let too_many_msgs = FileDescriptorSet {
        file: vec![FileDescriptorProto {
            name: Some("many.proto".to_string()),
            package: Some("previ.cap".to_string()),
            message_type: (0..=MAX_POOL_MESSAGES)
                .map(|i| DescriptorProto {
                    name: Some(format!("M{i}")),
                    ..Default::default()
                })
                .collect(),
            syntax: Some("proto3".to_string()),
            ..Default::default()
        }],
    };
    assert!(CompiledDescriptorPool::from_file_descriptor_set(&too_many_msgs).is_none());

    let too_long_ident = FileDescriptorSet {
        file: vec![FileDescriptorProto {
            name: Some("long.proto".to_string()),
            package: Some("previ.cap".to_string()),
            message_type: vec![DescriptorProto {
                name: Some("A".repeat(MAX_IDENTIFIER_LEN + 1)),
                ..Default::default()
            }],
            syntax: Some("proto3".to_string()),
            ..Default::default()
        }],
    };
    assert!(CompiledDescriptorPool::from_file_descriptor_set(&too_long_ident).is_none());

    let field = |name: &str, num: i32, ty: Type, label: Label, type_name: Option<&str>| {
        FieldDescriptorProto {
            name: Some(name.to_string()),
            number: Some(num),
            label: Some(label as i32),
            r#type: Some(ty as i32),
            type_name: type_name.map(ToString::to_string),
            ..Default::default()
        }
    };

    // 3. Oneof submessage clearing and re-setting + proto3 defaults + proto2 presence
    let sub_node = DescriptorProto {
        name: Some("SubNode".to_string()),
        field: vec![
            field("left", 1, Type::Int64, Label::Optional, None),
            field("right", 2, Type::Int64, Label::Optional, None),
            field("child", 3, Type::Message, Label::Optional, Some("SubNode")),
        ],
        ..Default::default()
    };
    let mode_enum = EnumDescriptorProto {
        name: Some("Mode".to_string()),
        value: vec![
            EnumValueDescriptorProto {
                name: Some("MODE_UNSPECIFIED".to_string()),
                number: Some(0),
                ..Default::default()
            },
            EnumValueDescriptorProto {
                name: Some("MODE_FAST".to_string()),
                number: Some(1),
                ..Default::default()
            },
        ],
        ..Default::default()
    };
    let mut oneof_sub = field("sub", 11, Type::Message, Label::Optional, Some("SubNode"));
    oneof_sub.oneof_index = Some(0);
    let mut oneof_choice_b = field("choice_b", 14, Type::String, Label::Optional, None);
    oneof_choice_b.oneof_index = Some(0);
    let mut opt_quota = field("opt_quota", 12, Type::Int64, Label::Optional, None);
    opt_quota.proto3_optional = Some(true);
    opt_quota.oneof_index = Some(1);

    let meta_entry = DescriptorProto {
        name: Some("MetadataEntry".to_string()),
        field: vec![
            field("key", 1, Type::String, Label::Optional, None),
            field("value", 2, Type::String, Label::Optional, None),
        ],
        options: Some(MessageOptions {
            map_entry: Some(true),
            ..Default::default()
        }),
        ..Default::default()
    };
    let node_map_entry = DescriptorProto {
        name: Some("NodeMapEntry".to_string()),
        field: vec![
            field("key", 1, Type::String, Label::Optional, None),
            field("value", 2, Type::Message, Label::Optional, Some("SubNode")),
        ],
        options: Some(MessageOptions {
            map_entry: Some(true),
            ..Default::default()
        }),
        ..Default::default()
    };

    // Comprehensive proto3 message covering all remaining FieldKinds, relative type_name,
    // Type::Group skipping, oneof submessage, and maps.
    let all_kinds_msg = DescriptorProto {
        name: Some("AllKinds".to_string()),
        field: vec![
            field("raw_bytes", 1, Type::Bytes, Label::Optional, None),
            field("f32", 2, Type::Float, Label::Optional, None),
            field("fix64", 3, Type::Fixed64, Label::Optional, None),
            field("sfix64", 4, Type::Sfixed64, Label::Optional, None),
            field("fix32", 5, Type::Fixed32, Label::Optional, None),
            field("sfix32", 6, Type::Sfixed32, Label::Optional, None),
            field("u32", 7, Type::Uint32, Label::Optional, None),
            field("s32", 8, Type::Sint32, Label::Optional, None),
            field("i32", 9, Type::Int32, Label::Optional, None),
            field("mode", 10, Type::Enum, Label::Optional, Some("Mode")),
            oneof_sub,
            opt_quota,
            field("dbl", 13, Type::Double, Label::Optional, None),
            oneof_choice_b,
            field(
                "node_map",
                15,
                Type::Message,
                Label::Repeated,
                Some("AllKinds.NodeMapEntry"),
            ),
            field(
                "metadata",
                16,
                Type::Message,
                Label::Repeated,
                Some("AllKinds.MetadataEntry"),
            ),
            field("label", 17, Type::String, Label::Optional, None),
            field("armed", 18, Type::Bool, Label::Optional, None),
            field("seq", 19, Type::Int64, Label::Optional, None),
            field("scores", 20, Type::Int32, Label::Repeated, None),
            field(
                "legacy_group",
                21,
                Type::Group,
                Label::Optional,
                Some("SubNode"),
            ),
        ],
        nested_type: vec![meta_entry, node_map_entry],
        ..Default::default()
    };

    let proto2_msg = DescriptorProto {
        name: Some("Proto2Msg".to_string()),
        field: vec![
            field("label", 1, Type::String, Label::Optional, None),
            field("seq", 2, Type::Int64, Label::Optional, None),
            field("armed", 3, Type::Bool, Label::Optional, None),
            field(
                "mode",
                4,
                Type::Enum,
                Label::Optional,
                Some(".previ.rel.Mode"),
            ),
        ],
        ..Default::default()
    };

    let fds = FileDescriptorSet {
        file: vec![
            FileDescriptorProto {
                name: Some("rel.proto".to_string()),
                package: Some("previ.rel".to_string()),
                message_type: vec![all_kinds_msg, sub_node],
                enum_type: vec![mode_enum],
                syntax: Some("proto3".to_string()),
                ..Default::default()
            },
            FileDescriptorProto {
                name: Some("p2.proto".to_string()),
                package: Some("previ.p2".to_string()),
                message_type: vec![proto2_msg],
                syntax: Some("proto2".to_string()),
                ..Default::default()
            },
        ],
    };
    let pool = CompiledDescriptorPool::from_file_descriptor_set(&fds).unwrap();
    let msg = pool.resolve_message("previ.rel.AllKinds").unwrap();
    let p2_msg = pool.resolve_message("previ.p2.Proto2Msg").unwrap();

    // Type::Group omitted and synthetic MapEntry excluded from resolve_message
    assert!(!msg.fields_by_name.contains_key("legacy_group"));
    assert!(
        pool.resolve_message("previ.rel.AllKinds.MetadataEntry")
            .is_none()
    );
    assert!(pool.resolve_message("MetadataEntry").is_none());

    // Oneof submessage clearing and re-setting
    #[derive(Clone, PartialEq, prost::Message)]
    struct SubWire {
        #[prost(int64, tag = "1")]
        left: i64,
        #[prost(int64, tag = "2")]
        right: i64,
    }
    #[derive(Clone, PartialEq, prost::Message)]
    struct OneofWire {
        #[prost(message, optional, tag = "11")]
        sub: Option<SubWire>,
        #[prost(string, optional, tag = "14")]
        choice_b: Option<String>,
    }
    let part_sub_left = OneofWire {
        sub: Some(SubWire {
            left: 100,
            right: 0,
        }),
        choice_b: None,
    }
    .encode_to_vec();
    let part_choice_b = OneofWire {
        sub: None,
        choice_b: Some("hi".to_string()),
    }
    .encode_to_vec();
    let part_sub_right = OneofWire {
        sub: Some(SubWire {
            left: 0,
            right: 200,
        }),
        choice_b: None,
    }
    .encode_to_vec();

    let mut cleared_bytes = part_sub_left;
    cleared_bytes.extend_from_slice(&part_choice_b);
    let sub_left = msg.resolve_path("sub.left", &pool).unwrap();
    let sub_right = msg.resolve_path("sub.right", &pool).unwrap();
    let choice_b = msg.resolve_path("choice_b", &pool).unwrap();
    assert_eq!(extract_i64(&cleared_bytes, &sub_left, &pool), None);
    assert_eq!(
        extract_str(&cleared_bytes, &choice_b, &pool).as_deref(),
        Some("hi")
    );
    let cleared_json = transcode_to_json(&cleared_bytes, msg, &pool).unwrap();
    assert!(cleared_json.get("sub").is_none());
    assert_eq!(cleared_json["choice_b"], "hi");

    let mut reset_bytes = cleared_bytes;
    reset_bytes.extend_from_slice(&part_sub_right);
    assert_eq!(extract_i64(&reset_bytes, &sub_left, &pool), Some(0));
    assert_eq!(extract_i64(&reset_bytes, &sub_right, &pool), Some(200));
    assert_eq!(extract_str(&reset_bytes, &choice_b, &pool), None);
    let reset_json = transcode_to_json(&reset_bytes, msg, &pool).unwrap();
    assert!(reset_json.get("choice_b").is_none());
    assert_eq!(reset_json["sub"], serde_json::json!({"right": 200}));

    // Duplicate tag 2 inside a single map<K, Message> entry merges:
    // tag 15 (0x7a), len 13: tag 1 "dup" [0x0a, 0x03, b'd', b'u', b'p'],
    // tag 2 { left: 10 } [0x12, 0x02, 0x08, 0x0a], tag 2 { right: 20 } [0x12, 0x02, 0x10, 0x14]
    let dup_map_entry_wire = vec![
        0x7a, 13, 0x0a, 3, b'd', b'u', b'p', 0x12, 2, 0x08, 10, 0x12, 2, 0x10, 20,
    ];
    let dup_left = msg.resolve_path("node_map.dup.left", &pool).unwrap();
    let dup_right = msg.resolve_path("node_map.dup.right", &pool).unwrap();
    assert_eq!(extract_i64(&dup_map_entry_wire, &dup_left, &pool), Some(10));
    assert_eq!(
        extract_i64(&dup_map_entry_wire, &dup_right, &pool),
        Some(20)
    );
    let dup_json = transcode_to_json(&dup_map_entry_wire, msg, &pool).unwrap();
    assert_eq!(
        dup_json["node_map"]["dup"],
        serde_json::json!({"left": 10, "right": 20})
    );

    // Proto3 empty payload defaults vs explicit-presence None, and Proto2 optional presence:
    let label_steps = msg.resolve_path("label", &pool).unwrap();
    let mode_steps = msg.resolve_path("mode", &pool).unwrap();
    let seq_steps = msg.resolve_path("seq", &pool).unwrap();
    let armed_steps = msg.resolve_path("armed", &pool).unwrap();
    let opt_quota_steps = msg.resolve_path("opt_quota", &pool).unwrap();
    let missing_map_steps = msg.resolve_path("metadata.missing", &pool).unwrap();

    assert_eq!(extract_str(&[], &label_steps, &pool).as_deref(), Some(""));
    assert_eq!(
        extract_str(&[], &mode_steps, &pool).as_deref(),
        Some("MODE_UNSPECIFIED")
    );
    assert_eq!(extract_str(&[], &seq_steps, &pool).as_deref(), Some("0"));
    assert_eq!(
        extract_str(&[], &armed_steps, &pool).as_deref(),
        Some("false")
    );
    assert_eq!(extract_str(&[], &opt_quota_steps, &pool), None);
    assert_eq!(extract_str(&[], &choice_b, &pool), None);
    assert_eq!(extract_str(&[], &missing_map_steps, &pool), None);
    assert_eq!(extract_str(&[], &sub_left, &pool), None);

    let p2_label = p2_msg.resolve_path("label", &pool).unwrap();
    let p2_seq = p2_msg.resolve_path("seq", &pool).unwrap();
    let p2_armed = p2_msg.resolve_path("armed", &pool).unwrap();
    let p2_mode = p2_msg.resolve_path("mode", &pool).unwrap();
    assert_eq!(extract_str(&[], &p2_label, &pool), None);
    assert_eq!(extract_i64(&[], &p2_seq, &pool), None);
    assert_eq!(extract_bool(&[], &p2_armed, &pool), None);
    assert_eq!(extract_str(&[], &p2_mode, &pool), None);
    let p2_explicit = vec![0x0a, 0x00, 0x10, 0x00, 0x18, 0x00, 0x20, 0x00];
    assert_eq!(
        extract_str(&p2_explicit, &p2_label, &pool).as_deref(),
        Some("")
    );
    assert_eq!(extract_i64(&p2_explicit, &p2_seq, &pool), Some(0));
    assert_eq!(extract_bool(&p2_explicit, &p2_armed, &pool), Some(false));
    assert_eq!(
        extract_str(&p2_explicit, &p2_mode, &pool).as_deref(),
        Some("MODE_UNSPECIFIED")
    );

    // 4. All remaining FieldKinds (Bytes, Float, Fixed64, Sfixed64, Fixed32, Sfixed32, Uint32, Sint32,
    //    negative Int32 10-byte varint, unknown enum 999) + escaped/empty map keys + invalid paths
    #[derive(Clone, PartialEq, prost::Message)]
    struct AllKindsWire {
        #[prost(bytes = "vec", tag = "1")]
        raw_bytes: Vec<u8>,
        #[prost(float, tag = "2")]
        f32: f32,
        #[prost(fixed64, tag = "3")]
        fix64: u64,
        #[prost(sfixed64, tag = "4")]
        sfix64: i64,
        #[prost(fixed32, tag = "5")]
        fix32: u32,
        #[prost(sfixed32, tag = "6")]
        sfix32: i32,
        #[prost(uint32, tag = "7")]
        u32: u32,
        #[prost(sint32, tag = "8")]
        s32: i32,
        #[prost(int32, tag = "9")]
        i32: i32,
        #[prost(int32, tag = "10")]
        mode: i32,
        #[prost(btree_map = "string, message", tag = "15")]
        node_map: BTreeMap<String, SubWire>,
        #[prost(btree_map = "string, string", tag = "16")]
        metadata: BTreeMap<String, String>,
    }
    let mut node_map = BTreeMap::new();
    node_map.insert(
        String::new(),
        SubWire {
            left: 314,
            right: 0,
        },
    );
    let mut metadata = BTreeMap::new();
    metadata.insert("app.k8s.io/name".to_string(), "restate".to_string());
    metadata.insert(r"a\b".to_string(), "slash".to_string());
    metadata.insert(String::new(), "empty-key-val".to_string());

    let kinds_bytes = AllKindsWire {
        raw_bytes: vec![1, 2, 3],
        f32: 1.5,
        fix64: 123_456_789_012,
        sfix64: -987_654_321,
        fix32: 42_000,
        sfix32: -1_234,
        u32: 3_000_000_000,
        s32: -555,
        i32: -42,
        mode: 999,
        node_map,
        metadata,
    }
    .encode_to_vec();

    let bytes_steps = msg.resolve_path("raw_bytes", &pool).unwrap();
    let f32_steps = msg.resolve_path("f32", &pool).unwrap();
    let fix64_steps = msg.resolve_path("fix64", &pool).unwrap();
    let sfix64_steps = msg.resolve_path("sfix64", &pool).unwrap();
    let fix32_steps = msg.resolve_path("fix32", &pool).unwrap();
    let sfix32_steps = msg.resolve_path("sfix32", &pool).unwrap();
    let u32_steps = msg.resolve_path("u32", &pool).unwrap();
    let s32_steps = msg.resolve_path("s32", &pool).unwrap();
    let i32_steps = msg.resolve_path("i32", &pool).unwrap();

    assert_eq!(
        extract_str(&kinds_bytes, &bytes_steps, &pool).as_deref(),
        Some("AQID")
    );
    assert_eq!(extract_i64(&kinds_bytes, &bytes_steps, &pool), None);
    assert_eq!(extract_f64(&kinds_bytes, &f32_steps, &pool), Some(1.5));
    assert_eq!(
        extract_str(&kinds_bytes, &f32_steps, &pool).as_deref(),
        Some("1.5")
    );
    assert_eq!(
        extract_i64(&kinds_bytes, &fix64_steps, &pool),
        Some(123_456_789_012)
    );
    assert_eq!(
        extract_i64(&kinds_bytes, &sfix64_steps, &pool),
        Some(-987_654_321)
    );
    assert_eq!(extract_i64(&kinds_bytes, &fix32_steps, &pool), Some(42_000));
    assert_eq!(
        extract_i64(&kinds_bytes, &sfix32_steps, &pool),
        Some(-1_234)
    );
    assert_eq!(
        extract_i64(&kinds_bytes, &u32_steps, &pool),
        Some(3_000_000_000)
    );
    assert_eq!(extract_i64(&kinds_bytes, &s32_steps, &pool), Some(-555));
    assert_eq!(extract_i64(&kinds_bytes, &i32_steps, &pool), Some(-42));
    assert_eq!(extract_i64(&kinds_bytes, &mode_steps, &pool), Some(999));
    assert_eq!(extract_f64(&kinds_bytes, &mode_steps, &pool), Some(999.0));
    assert_eq!(
        extract_str(&kinds_bytes, &mode_steps, &pool).as_deref(),
        Some("999")
    );

    let k8s_steps = msg
        .resolve_path(r"metadata.app\.k8s\.io/name", &pool)
        .unwrap();
    let slash_steps = msg.resolve_path(r"metadata.a\\b", &pool).unwrap();
    let empty_meta_steps = msg.resolve_path("metadata.", &pool).unwrap();
    let empty_submap_steps = msg.resolve_path("node_map..left", &pool).unwrap();
    assert_eq!(
        extract_str(&kinds_bytes, &k8s_steps, &pool).as_deref(),
        Some("restate")
    );
    assert_eq!(
        extract_str(&kinds_bytes, &slash_steps, &pool).as_deref(),
        Some("slash")
    );
    assert_eq!(
        extract_str(&kinds_bytes, &empty_meta_steps, &pool).as_deref(),
        Some("empty-key-val")
    );
    assert_eq!(
        extract_i64(&kinds_bytes, &empty_submap_steps, &pool),
        Some(314)
    );

    let kinds_json = transcode_to_json(&kinds_bytes, msg, &pool).unwrap();
    assert_eq!(kinds_json["raw_bytes"], "AQID");
    assert_eq!(kinds_json["f32"], 1.5);
    assert_eq!(kinds_json["i32"], -42);
    assert_eq!(kinds_json["mode"], 999);
    assert_eq!(kinds_json["metadata"]["app.k8s.io/name"], "restate");

    // Invalid paths & 65-segment cyclic path rejected
    for bad_path in [
        "",
        ".label",
        "label.",
        "sub..left",
        r"metadata.trailing\",
        r"metadata.bad\x",
    ] {
        assert!(msg.resolve_path(bad_path, &pool).is_none(), "{bad_path}");
    }
    let cyclic_65 = format!("sub.{}left", "child.".repeat(63));
    assert!(msg.resolve_path(&cyclic_65, &pool).is_none());

    // 5. Malformed wire payloads, unknown SGROUP..EGROUP skipping, non-finite floats, JSON limits
    let malformed_cases: &[&[u8]] = &[
        // 10th-byte varint overflow on tag 19 (`seq`: (19 << 3) | 0 = 0x98, 0x01)
        &[
            0x98, 0x01, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x02,
        ],
        // Truncated varint on tag 19
        &[0x98, 0x01, 0x80],
        // Truncated fixed32 on tag 5 (`fix32`: (5 << 3) | 5 = 0x2d) and unknown tag 99 (`0x9d, 0x06`)
        &[0x2d, 0x01, 0x02, 0x03],
        &[0x9d, 0x06, 0x01, 0x02, 0x03],
        // Truncated fixed64 on tag 3 (`fix64`: (3 << 3) | 1 = 0x19) and unknown tag 99 (`0x99, 0x06`)
        &[0x19, 1, 2, 3, 4, 5, 6, 7],
        &[0x99, 0x06, 1, 2, 3, 4, 5, 6, 7],
        // Truncated len-delimited on tag 17 (`label`: (17 << 3) | 2 = 0x8a, 0x01)
        &[0x8a, 0x01, 0x05, b'a', b'b'],
        // Invalid wire types 6 and 7 (matched and unmatched)
        &[0x8e, 0x01],
        &[0x8f, 0x01],
        &[0x9e, 0x06],
        &[0x9f, 0x06],
        // Stray EGROUP (wire type 4)
        &[0x8c, 0x01],
    ];
    for &bad_buf in malformed_cases {
        assert_eq!(extract_str(bad_buf, &label_steps, &pool), None);
        assert_eq!(extract_i64(bad_buf, &seq_steps, &pool), None);
        assert_eq!(transcode_to_json(bad_buf, msg, &pool), None);
    }
    // Invalid UTF-8 on string field `label` (tag 17) fails when extracting `label` or transcoding
    // the message, while wire-skipping unrelated fields (`seq`) remains unaffected:
    let invalid_utf8_label = &[0x8a, 0x01, 0x02, 0xff, 0xfe];
    assert_eq!(extract_str(invalid_utf8_label, &label_steps, &pool), None);
    assert_eq!(transcode_to_json(invalid_utf8_label, msg, &pool), None);
    assert_eq!(extract_i64(invalid_utf8_label, &seq_steps, &pool), Some(0));

    // Valid unknown SGROUP(3)..EGROUP(4) skipped cleanly before `label = "ok"`:
    // tag 99 SGROUP [0x9b, 0x06], inner field 1 varint 42 [0x08, 0x2a], tag 99 EGROUP [0x9c, 0x06],
    // tag 17 (`label`) "ok" [0x8a, 0x01, 0x02, b'o', b'k']
    let sgroup_skip_buf = [
        0x9b, 0x06, 0x08, 0x2a, 0x9c, 0x06, 0x8a, 0x01, 0x02, b'o', b'k',
    ];
    assert_eq!(
        extract_str(&sgroup_skip_buf, &label_steps, &pool).as_deref(),
        Some("ok")
    );
    assert_eq!(
        transcode_to_json(&sgroup_skip_buf, msg, &pool).unwrap(),
        serde_json::json!({"label": "ok"})
    );

    // Infinity / -Infinity / NaN JSON formatting (tag 13 `dbl`: (13 << 3) | 1 = 0x69; tag 2 `f32`: 0x15)
    let mut inf_buf = vec![0x69];
    inf_buf.extend_from_slice(&f64::INFINITY.to_le_bytes());
    inf_buf.push(0x15);
    inf_buf.extend_from_slice(&f32::NEG_INFINITY.to_le_bytes());
    let inf_json = transcode_to_json(&inf_buf, msg, &pool).unwrap();
    assert_eq!(inf_json["dbl"], "Infinity");
    assert_eq!(inf_json["f32"], "-Infinity");

    let mut neg_inf_buf = vec![0x69];
    neg_inf_buf.extend_from_slice(&f64::NEG_INFINITY.to_le_bytes());
    neg_inf_buf.push(0x15);
    neg_inf_buf.extend_from_slice(&f32::NAN.to_le_bytes());
    let neg_inf_json = transcode_to_json(&neg_inf_buf, msg, &pool).unwrap();
    assert_eq!(neg_inf_json["dbl"], "-Infinity");
    assert_eq!(neg_inf_json["f32"], "NaN");

    // MAX_JSON_VALUES (>65,536 packed elements -> None): tag 20 (`scores`: (20 << 3) | 2 = 0xa2, 0x01)
    let mut too_many_vals = vec![0xa2, 0x01];
    prost::encoding::encode_varint(65_537, &mut too_many_vals);
    too_many_vals.resize(too_many_vals.len() + 65_537, 0x01);
    assert_eq!(transcode_to_json(&too_many_vals, msg, &pool), None);

    // MAX_JSON_BYTES (base64 of 13 MiB > 16 MiB -> None): tag 1 (`raw_bytes`: 0x0a)
    let mut too_many_bytes = vec![0x0a];
    let big_len = 13 * 1024 * 1024;
    prost::encoding::encode_varint(big_len as u64, &mut too_many_bytes);
    too_many_bytes.resize(too_many_bytes.len() + big_len, 0xab);
    assert_eq!(transcode_to_json(&too_many_bytes, msg, &pool), None);
}

#[derive(Clone, Default, Debug)]
struct SharedMockSchemas(Arc<RwLock<SharedMockSchemasInner>>);

#[derive(Default, Debug)]
struct SharedMockSchemasInner {
    services: HashMap<String, ServiceMetadata>,
    deployments: MockDeploymentMetadataRegistry,
}

impl SharedMockSchemas {
    fn add(&self, svc: ServiceMetadata) {
        self.0.write().services.insert(svc.name.clone(), svc);
    }

    fn remove(&self, service_name: &str) {
        self.0.write().services.remove(service_name);
    }
}

impl ServiceMetadataResolver for SharedMockSchemas {
    fn resolve_latest_service(&self, service_name: impl AsRef<str>) -> Option<ServiceMetadata> {
        self.0.read().services.get(service_name.as_ref()).cloned()
    }

    fn resolve_latest_service_openapi(
        &self,
        _svc: impl AsRef<str>,
        _addr: AdvertisedAddress<HttpIngressPort>,
    ) -> Option<serde_json::Value> {
        Some(serde_json::Value::Null)
    }

    fn list_services(&self) -> Vec<ServiceMetadata> {
        self.0.read().services.values().cloned().collect()
    }

    fn list_service_names(&self) -> Vec<String> {
        self.0.read().services.keys().cloned().collect()
    }
}

impl DeploymentResolver for SharedMockSchemas {
    fn resolve_latest_deployment_for_service(
        &self,
        service_name: impl AsRef<str>,
    ) -> Option<Deployment> {
        self.0
            .read()
            .deployments
            .resolve_latest_deployment_for_service(service_name)
    }

    fn find_deployment(
        &self,
        deployment_address: &DeploymentAddress,
        additional_headers: &Headers,
    ) -> Option<(Deployment, Vec<ServiceMetadata>)> {
        self.0
            .read()
            .deployments
            .find_deployment(deployment_address, additional_headers)
    }

    fn get_deployment(&self, deployment_id: &DeploymentId) -> Option<Deployment> {
        self.0.read().deployments.get_deployment(deployment_id)
    }

    fn get_deployment_and_services(
        &self,
        deployment_id: &DeploymentId,
    ) -> Option<(Deployment, Vec<ServiceMetadata>)> {
        self.0
            .read()
            .deployments
            .get_deployment_and_services(deployment_id)
    }

    fn get_deployments(&self) -> Vec<(Deployment, Vec<(String, ServiceRevision)>)> {
        self.0.read().deployments.get_deployments()
    }
}

#[restate_core::test(flavor = "multi_thread", worker_threads = 2)]
async fn schema_cache_invalidation_and_mixed_state_key_batches() {
    // 1. Verify DynamicProtoSchemaRegistry invalidates on live metadata/revision updates,
    //    supports proto.fds + wrapped/unpadded base64 + proto.state.* wildcard, and skips ServiceType::Service:
    let shared_schemas = SharedMockSchemas::default();
    let registry = DynamicProtoSchemaRegistry::new(Live::from_value(shared_schemas.clone()));

    assert!(registry.resolve_service_schemas("DynamicSvc").is_none());
    shared_schemas.add(ServiceMetadata::mock_virtual_object("DynamicSvc", ["run"]));
    assert!(registry.resolve_service_schemas("DynamicSvc").is_none());

    let mut svc_v1 = make_service_metadata_with_proto("DynamicSvc");
    svc_v1.revision = 1;
    shared_schemas.add(svc_v1);
    let rev1 = registry
        .resolve_service_schemas("DynamicSvc")
        .expect("v1 schema should resolve");
    assert_eq!(rev1.revision, 1);
    assert!(rev1.resolve_state_key("profile").is_some());
    assert!(rev1.resolve_state_key("settings").is_none());

    let mut svc_v2 = make_service_metadata_with_proto("DynamicSvc");
    svc_v2.revision = 2;
    svc_v2.metadata.insert(
        format!("{PROTO_STATE_KEY_PREFIX}settings"),
        "previ.bench.BusyMemberProfile".to_string(),
    );
    shared_schemas.add(svc_v2);
    let rev2 = registry
        .resolve_service_schemas("DynamicSvc")
        .expect("v2 schema should resolve");
    assert_eq!(rev2.revision, 2);
    assert!(rev2.resolve_state_key("settings").is_some());

    // Build multi.proto descriptor (ProfileMsg on tags 1..3, SettingsMsg on tags 8..10)
    let field = |name: &str, num: i32, ty: Type| FieldDescriptorProto {
        name: Some(name.to_string()),
        number: Some(num),
        label: Some(Label::Optional as i32),
        r#type: Some(ty as i32),
        ..Default::default()
    };
    let profile_msg = DescriptorProto {
        name: Some("ProfileMsg".to_string()),
        field: vec![
            field("name", 1, Type::String),
            field("val", 2, Type::Int64),
            field("active", 3, Type::Bool),
        ],
        ..Default::default()
    };
    let settings_msg = DescriptorProto {
        name: Some("SettingsMsg".to_string()),
        field: vec![
            field("name", 8, Type::String),
            field("val", 9, Type::Int64),
            field("active", 10, Type::Bool),
        ],
        ..Default::default()
    };
    let fds = FileDescriptorSet {
        file: vec![FileDescriptorProto {
            name: Some("multi.proto".to_string()),
            package: Some("previ.multi".to_string()),
            message_type: vec![profile_msg, settings_msg],
            syntax: Some("proto3".to_string()),
            ..Default::default()
        }],
    };
    let fds_raw = fds.encode_to_vec();
    let fds_b64 = base64::engine::general_purpose::STANDARD.encode(&fds_raw);
    let unpadded = base64::engine::general_purpose::STANDARD_NO_PAD.encode(&fds_raw);
    let wrapped_unpadded = unpadded
        .as_bytes()
        .chunks(32)
        .map(|c| std::str::from_utf8(c).unwrap())
        .collect::<Vec<_>>()
        .join("\n  ");

    // Stateless ServiceType::Service with proto metadata must be ignored
    let mut stateless = ServiceMetadata::mock_service("StatelessSvc", ["call"]);
    stateless
        .metadata
        .insert(PROTO_DESCRIPTOR_SET_KEY.to_string(), fds_b64.clone());
    stateless.metadata.insert(
        format!("{PROTO_STATE_KEY_PREFIX}profile"),
        "previ.multi.ProfileMsg".to_string(),
    );
    shared_schemas.add(stateless);
    assert!(registry.resolve_service_schemas("StatelessSvc").is_none());

    // Wildcard + explicit override + proto.fds short key + line-wrapped unpadded base64
    let mut svc_a = ServiceMetadata::mock_virtual_object("MixedKeySvc", ["get"]);
    svc_a.revision = 1;
    svc_a
        .metadata
        .insert(PROTO_FDS_SHORT_KEY.to_string(), wrapped_unpadded);
    svc_a.metadata.insert(
        format!("{PROTO_STATE_KEY_PREFIX}profile"),
        "previ.multi.ProfileMsg".to_string(),
    );
    svc_a.metadata.insert(
        PROTO_STATE_WILDCARD_KEY.to_string(),
        "previ.multi.SettingsMsg".to_string(),
    );
    shared_schemas.add(svc_a);
    let svc_a_compiled = registry.resolve_service_schemas("MixedKeySvc").unwrap();
    assert_eq!(
        svc_a_compiled
            .resolve_state_key("profile")
            .unwrap()
            .full_name,
        "previ.multi.ProfileMsg"
    );
    assert_eq!(
        svc_a_compiled
            .resolve_state_key("settings")
            .unwrap()
            .full_name,
        "previ.multi.SettingsMsg"
    );

    // Second proto service (uses SettingsMsg for `profile`) and a non-proto JSON service
    let mut svc_b = ServiceMetadata::mock_virtual_object("SecondProtoSvc", ["get"]);
    svc_b.revision = 1;
    svc_b
        .metadata
        .insert(PROTO_DESCRIPTOR_SET_KEY.to_string(), fds_b64.clone());
    svc_b.metadata.insert(
        format!("{PROTO_STATE_KEY_PREFIX}profile"),
        "previ.multi.SettingsMsg".to_string(),
    );
    shared_schemas.add(svc_b);
    shared_schemas.add(ServiceMetadata::mock_virtual_object("JsonSvc", ["get"]));

    // Lifecycle service starts at revision 1 with `profile` -> ProfileMsg
    let mut lifecycle_svc = ServiceMetadata::mock_virtual_object("LifecycleSvc", ["get"]);
    lifecycle_svc.revision = 1;
    lifecycle_svc
        .metadata
        .insert(PROTO_DESCRIPTOR_SET_KEY.to_string(), fds_b64.clone());
    lifecycle_svc.metadata.insert(
        format!("{PROTO_STATE_KEY_PREFIX}profile"),
        "previ.multi.ProfileMsg".to_string(),
    );
    shared_schemas.add(lifecycle_svc);

    #[derive(Clone, PartialEq, prost::Message)]
    struct ProfileWire {
        #[prost(string, tag = "1")]
        name: String,
        #[prost(int64, tag = "2")]
        val: i64,
        #[prost(bool, tag = "3")]
        active: bool,
    }
    #[derive(Clone, PartialEq, prost::Message)]
    struct SettingsWire {
        #[prost(string, tag = "8")]
        name: String,
        #[prost(int64, tag = "9")]
        val: i64,
        #[prost(bool, tag = "10")]
        active: bool,
    }

    let mut engine =
        MockQueryEngine::create_with(MockStatusHandle::default(), shared_schemas.clone()).await;
    {
        let mut tx = engine.partition_store().transaction();
        tx.put_user_state(
            &ServiceId::new(None, "MixedKeySvc", "obj_1"),
            &Bytes::from_static(b"profile"),
            ProfileWire {
                name: "alice".to_string(),
                val: 11,
                active: true,
            }
            .encode_to_vec(),
        )
        .unwrap();
        tx.put_user_state(
            &ServiceId::new(None, "MixedKeySvc", "obj_1"),
            &Bytes::from_static(b"settings"),
            SettingsWire {
                name: "dark-mode".to_string(),
                val: 22,
                active: false,
            }
            .encode_to_vec(),
        )
        .unwrap();
        tx.put_user_state(
            &ServiceId::new(None, "MixedKeySvc", "obj_2"),
            &Bytes::from_static(b"profile"),
            ProfileWire {
                name: "bob".to_string(),
                val: 33,
                active: true,
            }
            .encode_to_vec(),
        )
        .unwrap();
        // Non-UTF8 state key -> `key` column is NULL
        tx.put_user_state(
            &ServiceId::new(None, "MixedKeySvc", "obj_3"),
            &Bytes::from_static(&[0xff, 0xfe]),
            ProfileWire {
                name: "nonutf8".to_string(),
                val: 99,
                active: true,
            }
            .encode_to_vec(),
        )
        .unwrap();
        // Corrupt protobuf bytes -> SQL NULL while sibling rows succeed
        tx.put_user_state(
            &ServiceId::new(None, "MixedKeySvc", "obj_4"),
            &Bytes::from_static(b"profile"),
            [0x00, 0x00],
        )
        .unwrap();
        tx.put_user_state(
            &ServiceId::new(None, "SecondProtoSvc", "sec_1"),
            &Bytes::from_static(b"profile"),
            SettingsWire {
                name: "second".to_string(),
                val: 44,
                active: true,
            }
            .encode_to_vec(),
        )
        .unwrap();
        tx.put_user_state(
            &ServiceId::new(None, "JsonSvc", "json_1"),
            &Bytes::from_static(b"profile"),
            br#"{"name":"ignored"}"#,
        )
        .unwrap();
        // LifecycleSvc rows
        tx.put_user_state(
            &ServiceId::new(None, "LifecycleSvc", "lc_1"),
            &Bytes::from_static(b"profile"),
            ProfileWire {
                name: "init".to_string(),
                val: 7,
                active: true,
            }
            .encode_to_vec(),
        )
        .unwrap();
        tx.put_user_state(
            &ServiceId::new(None, "LifecycleSvc", "lc_1"),
            &Bytes::from_static(b"settings"),
            SettingsWire {
                name: "cfg".to_string(),
                val: 8,
                active: false,
            }
            .encode_to_vec(),
        )
        .unwrap();
        tx.put_user_state(
            &ServiceId::new(None, "LifecycleSvc", "lc_1"),
            &Bytes::from_static(b"extra"),
            ProfileWire {
                name: "extra-val".to_string(),
                val: 9,
                active: true,
            }
            .encode_to_vec(),
        )
        .unwrap();
        tx.commit().await.unwrap();
    }

    // 2. Multi-service + mixed-key batch covering proto_get_bool & proto_to_json slow paths,
    //    non-proto service, non-UTF8 state key, and corrupt protobuf payload:
    let batches: Vec<RecordBatch> = engine
        .execute(
            "SELECT
                service_name,
                service_key,
                key,
                proto_get_str(service_name, key, value, 'name') AS name,
                proto_get_int(service_name, key, value, 'val') AS val_int,
                proto_get_float(service_name, key, value, 'val') AS val_float,
                proto_get_bool(service_name, key, value, 'active') AS active,
                proto_to_json(service_name, key, value) AS row_json
            FROM state
            WHERE scope IS NULL AND service_name IN ('MixedKeySvc', 'SecondProtoSvc', 'JsonSvc')
            ORDER BY service_name, service_key, key_length",
        )
        .await
        .unwrap()
        .stream
        .map(Result::unwrap)
        .collect()
        .await;
    assert_eq!(batches.len(), 1);
    let b = &batches[0];
    // Order: JsonSvc/json_1, MixedKeySvc/obj_1/profile (len 7), MixedKeySvc/obj_1/settings (len 8),
    //        MixedKeySvc/obj_2, MixedKeySvc/obj_3 (non-UTF8 key), MixedKeySvc/obj_4 (corrupt), SecondProtoSvc/sec_1
    assert_eq!(b.num_rows(), 7);

    let names_col = b
        .column_by_name("name")
        .unwrap()
        .as_any()
        .downcast_ref::<LargeStringArray>()
        .unwrap();
    let ints_col = b
        .column_by_name("val_int")
        .unwrap()
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let floats_col = b
        .column_by_name("val_float")
        .unwrap()
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();
    let bools_col = b
        .column_by_name("active")
        .unwrap()
        .as_any()
        .downcast_ref::<BooleanArray>()
        .unwrap();
    let jsons_col = b
        .column_by_name("row_json")
        .unwrap()
        .as_any()
        .downcast_ref::<LargeStringArray>()
        .unwrap();

    // Row 0: JsonSvc -> all NULL
    assert!(names_col.is_null(0));
    assert!(bools_col.is_null(0));
    assert!(jsons_col.is_null(0));
    // Row 1: MixedKeySvc obj_1 profile
    assert_eq!(names_col.value(1), "alice");
    assert_eq!(ints_col.value(1), 11);
    assert_eq!(floats_col.value(1), 11.0);
    assert!(bools_col.value(1));
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(jsons_col.value(1)).unwrap(),
        serde_json::json!({"name": "alice", "val": 11, "active": true})
    );
    // Row 2: MixedKeySvc obj_1 settings
    assert_eq!(names_col.value(2), "dark-mode");
    assert_eq!(ints_col.value(2), 22);
    assert!(!bools_col.value(2));
    // Row 3: MixedKeySvc obj_2 profile
    assert_eq!(names_col.value(3), "bob");
    assert_eq!(ints_col.value(3), 33);
    assert!(bools_col.value(3));
    // Row 4: MixedKeySvc obj_3 non-UTF8 key -> all NULL
    assert!(names_col.is_null(4));
    assert!(bools_col.is_null(4));
    assert!(jsons_col.is_null(4));
    // Row 5: MixedKeySvc obj_4 corrupt protobuf -> all NULL
    assert!(names_col.is_null(5));
    assert!(ints_col.is_null(5));
    assert!(bools_col.is_null(5));
    assert!(jsons_col.is_null(5));
    // Row 6: SecondProtoSvc sec_1 profile
    assert_eq!(names_col.value(6), "second");
    assert_eq!(ints_col.value(6), 44);
    assert!(bools_col.value(6));
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(jsons_col.value(6)).unwrap(),
        serde_json::json!({"name": "second", "val": 44, "active": true})
    );

    // 3. SQL Binary literal X'...' without arrow_cast, invalid arg type planning error,
    //    all-scalar proto_to_json, Scalar(NULL), empty batch, and direct invoke_with_args checks:
    let scalar_batches: Vec<RecordBatch> = engine
        .execute(
            "SELECT
                proto_get_str('MixedKeySvc', 'profile', X'0a056361726f6c102a1801', 'name') AS const_name,
                proto_get_int('MixedKeySvc', 'profile', X'0a056361726f6c102a1801', 'val') AS const_val,
                proto_get_bool('MixedKeySvc', 'profile', X'0a056361726f6c102a1801', 'active') AS const_active,
                proto_to_json('MixedKeySvc', 'profile', X'0a056361726f6c102a1801') AS const_json,
                proto_get_str('MixedKeySvc', 'profile', NULL, 'name') AS null_val,
                proto_get_str('MixedKeySvc', 'profile', X'0a056361726f6c102a1801', NULL) AS null_path,
                proto_to_json('MixedKeySvc', NULL, X'0a056361726f6c102a1801') AS null_key_json,
                proto_to_json('MixedKeySvc', 'profile', NULL) AS null_val_json",
        )
        .await
        .unwrap()
        .stream
        .map(Result::unwrap)
        .collect()
        .await;
    let sb = &scalar_batches[0];
    assert_eq!(sb.num_rows(), 1);
    assert_eq!(
        sb.column_by_name("const_name")
            .unwrap()
            .as_any()
            .downcast_ref::<LargeStringArray>()
            .unwrap()
            .value(0),
        "carol"
    );
    assert_eq!(
        sb.column_by_name("const_val")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0),
        42
    );
    assert!(
        sb.column_by_name("const_active")
            .unwrap()
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap()
            .value(0)
    );
    let const_json = sb
        .column_by_name("const_json")
        .unwrap()
        .as_any()
        .downcast_ref::<LargeStringArray>()
        .unwrap()
        .value(0);
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(const_json).unwrap(),
        serde_json::json!({"name": "carol", "val": 42, "active": true})
    );
    for col_name in ["null_val", "null_path", "null_key_json", "null_val_json"] {
        assert!(sb.column_by_name(col_name).unwrap().is_null(0));
    }

    // Non-string arg fails planning
    assert!(
        engine
            .execute("SELECT proto_get_str('MixedKeySvc', 'profile', X'00', 1)")
            .await
            .is_err()
    );

    // Empty batch (0 rows) via SQL
    let empty_sql_batches: Vec<RecordBatch> = engine
        .execute(
            "SELECT
                proto_get_str(service_name, key, value, 'name') AS n,
                proto_to_json(service_name, key, value) AS j
             FROM state
             WHERE service_name = 'NoSuchService'",
        )
        .await
        .unwrap()
        .stream
        .map(Result::unwrap)
        .collect()
        .await;
    let empty_rows: usize = empty_sql_batches.iter().map(RecordBatch::num_rows).sum();
    assert_eq!(empty_rows, 0);

    // Direct invoke_with_args checks: 0-row arrays, Scalar(NULL) + array, mismatched lengths
    let udfs = create_udfs(registry);
    let get_str_udf = udfs.iter().find(|u| u.name() == "proto_get_str").unwrap();
    let to_json_udf = udfs.iter().find(|u| u.name() == "proto_to_json").unwrap();
    let str_ret_field = Arc::new(Field::new("out", DataType::LargeUtf8, true));
    let cfg = Arc::new(ConfigOptions::default());

    let empty_str_arr: ArrayRef = Arc::new(LargeStringArray::from(Vec::<&str>::new()));
    let empty_bin_arr: ArrayRef = Arc::new(LargeBinaryArray::from(Vec::<&[u8]>::new()));
    let empty_out = get_str_udf
        .invoke_with_args(ScalarFunctionArgs {
            args: vec![
                ColumnarValue::Array(Arc::clone(&empty_str_arr)),
                ColumnarValue::Array(Arc::clone(&empty_str_arr)),
                ColumnarValue::Array(Arc::clone(&empty_bin_arr)),
                ColumnarValue::Array(Arc::clone(&empty_str_arr)),
            ],
            arg_fields: vec![],
            number_rows: 0,
            return_field: Arc::clone(&str_ret_field),
            config_options: Arc::clone(&cfg),
        })
        .unwrap();
    assert_eq!(empty_out.into_array(0).unwrap().len(), 0);
    let empty_json_out = to_json_udf
        .invoke_with_args(ScalarFunctionArgs {
            args: vec![
                ColumnarValue::Array(Arc::clone(&empty_str_arr)),
                ColumnarValue::Array(empty_str_arr),
                ColumnarValue::Array(empty_bin_arr),
            ],
            arg_fields: vec![],
            number_rows: 0,
            return_field: Arc::clone(&str_ret_field),
            config_options: Arc::clone(&cfg),
        })
        .unwrap();
    assert_eq!(empty_json_out.into_array(0).unwrap().len(), 0);

    let two_str_arr: ArrayRef =
        Arc::new(LargeStringArray::from(vec!["MixedKeySvc", "MixedKeySvc"]));
    let three_str_arr: ArrayRef = Arc::new(LargeStringArray::from(vec![
        "profile", "profile", "profile",
    ]));
    let two_bin_arr: ArrayRef = Arc::new(LargeBinaryArray::from(vec![
        [0x08u8, 0x01].as_slice(),
        [0x08u8, 0x02].as_slice(),
    ]));
    let null_scalar_out = to_json_udf
        .invoke_with_args(ScalarFunctionArgs {
            args: vec![
                ColumnarValue::Array(Arc::clone(&two_str_arr)),
                ColumnarValue::Scalar(ScalarValue::LargeUtf8(None)),
                ColumnarValue::Array(Arc::clone(&two_bin_arr)),
            ],
            arg_fields: vec![],
            number_rows: 2,
            return_field: Arc::clone(&str_ret_field),
            config_options: Arc::clone(&cfg),
        })
        .unwrap()
        .into_array(2)
        .unwrap();
    assert_eq!(null_scalar_out.len(), 2);
    assert_eq!(null_scalar_out.null_count(), 2);

    let mismatch_str_err = get_str_udf
        .invoke_with_args(ScalarFunctionArgs {
            args: vec![
                ColumnarValue::Array(Arc::clone(&two_str_arr)),
                ColumnarValue::Array(Arc::clone(&three_str_arr)),
                ColumnarValue::Array(Arc::clone(&two_bin_arr)),
                ColumnarValue::Scalar(ScalarValue::LargeUtf8(Some("name".to_string()))),
            ],
            arg_fields: vec![],
            number_rows: 2,
            return_field: Arc::clone(&str_ret_field),
            config_options: Arc::clone(&cfg),
        })
        .unwrap_err();
    assert!(
        mismatch_str_err
            .to_string()
            .contains("mismatched array lengths")
    );

    let mismatch_json_err = to_json_udf
        .invoke_with_args(ScalarFunctionArgs {
            args: vec![
                ColumnarValue::Array(two_str_arr),
                ColumnarValue::Array(three_str_arr),
                ColumnarValue::Array(two_bin_arr),
            ],
            arg_fields: vec![],
            number_rows: 2,
            return_field: str_ret_field,
            config_options: cfg,
        })
        .unwrap_err();
    assert!(
        mismatch_json_err
            .to_string()
            .contains("mismatched array lengths")
    );

    // 4. End-to-end SQL schema lifecycle: revision bump, same-revision fingerprint update,
    //    proto metadata removal, service deletion, and StatelessSvc exclusion from sys_proto_schemas
    let query_lifecycle = || async {
        let schemas_rows: Vec<RecordBatch> = engine
            .execute(
                "SELECT service_name, revision, state_key, message_name
                 FROM sys_proto_schemas
                 WHERE service_name IN ('LifecycleSvc', 'StatelessSvc')
                 ORDER BY state_key",
            )
            .await
            .unwrap()
            .stream
            .map(Result::unwrap)
            .collect()
            .await;
        let state_rows: Vec<RecordBatch> = engine
            .execute(
                "SELECT key, proto_get_str(service_name, key, value, 'name') AS name
                 FROM state
                 WHERE service_name = 'LifecycleSvc'
                 ORDER BY key",
            )
            .await
            .unwrap()
            .stream
            .map(Result::unwrap)
            .collect()
            .await;
        (schemas_rows, state_rows)
    };

    let extract_kv = |batches: &[RecordBatch]| -> Vec<(String, Option<String>)> {
        let mut out = Vec::new();
        for batch in batches {
            let keys = batch
                .column_by_name("key")
                .unwrap()
                .as_any()
                .downcast_ref::<LargeStringArray>()
                .unwrap();
            let vals = batch
                .column_by_name("name")
                .unwrap()
                .as_any()
                .downcast_ref::<LargeStringArray>()
                .unwrap();
            for i in 0..batch.num_rows() {
                out.push((
                    keys.value(i).to_string(),
                    (!vals.is_null(i)).then(|| vals.value(i).to_string()),
                ));
            }
        }
        out
    };

    // Initial rev 1: only `profile` mapped, `StatelessSvc` absent
    let (s1, d1) = query_lifecycle().await;
    assert_eq!(s1.iter().map(RecordBatch::num_rows).sum::<usize>(), 1);
    assert_eq!(
        extract_kv(&d1),
        vec![
            ("extra".to_string(), None),
            ("profile".to_string(), Some("init".to_string())),
            ("settings".to_string(), None),
        ]
    );

    // Revision bump -> rev 2 adds `settings`
    let mut lc_rev2 = ServiceMetadata::mock_virtual_object("LifecycleSvc", ["get"]);
    lc_rev2.revision = 2;
    lc_rev2
        .metadata
        .insert(PROTO_DESCRIPTOR_SET_KEY.to_string(), fds_b64);
    lc_rev2.metadata.insert(
        format!("{PROTO_STATE_KEY_PREFIX}profile"),
        "previ.multi.ProfileMsg".to_string(),
    );
    lc_rev2.metadata.insert(
        format!("{PROTO_STATE_KEY_PREFIX}settings"),
        "previ.multi.SettingsMsg".to_string(),
    );
    shared_schemas.add(lc_rev2.clone());
    let (s2, d2) = query_lifecycle().await;
    assert_eq!(s2.iter().map(RecordBatch::num_rows).sum::<usize>(), 2);
    assert_eq!(
        s2[0]
            .column_by_name("revision")
            .unwrap()
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap()
            .value(0),
        2
    );
    assert_eq!(
        extract_kv(&d2),
        vec![
            ("extra".to_string(), None),
            ("profile".to_string(), Some("init".to_string())),
            ("settings".to_string(), Some("cfg".to_string())),
        ]
    );

    // Same-revision metadata fingerprint update (still rev 2, adds `proto.state.*`)
    let mut lc_same_rev = lc_rev2;
    lc_same_rev.metadata.insert(
        PROTO_STATE_WILDCARD_KEY.to_string(),
        "previ.multi.ProfileMsg".to_string(),
    );
    shared_schemas.add(lc_same_rev.clone());
    let (s3, d3) = query_lifecycle().await;
    assert_eq!(s3.iter().map(RecordBatch::num_rows).sum::<usize>(), 3);
    assert_eq!(
        extract_kv(&d3),
        vec![
            ("extra".to_string(), Some("extra-val".to_string())),
            ("profile".to_string(), Some("init".to_string())),
            ("settings".to_string(), Some("cfg".to_string())),
        ]
    );

    // Remove proto metadata from LifecycleSvc
    let mut lc_no_proto = ServiceMetadata::mock_virtual_object("LifecycleSvc", ["get"]);
    lc_no_proto.revision = 2;
    shared_schemas.add(lc_no_proto);
    let (s4, d4) = query_lifecycle().await;
    assert_eq!(s4.iter().map(RecordBatch::num_rows).sum::<usize>(), 0);
    assert_eq!(
        extract_kv(&d4),
        vec![
            ("extra".to_string(), None),
            ("profile".to_string(), None),
            ("settings".to_string(), None),
        ]
    );

    // Restore proto metadata, confirm visible, then delete LifecycleSvc completely
    shared_schemas.add(lc_same_rev);
    let (s5, _) = query_lifecycle().await;
    assert_eq!(s5.iter().map(RecordBatch::num_rows).sum::<usize>(), 3);

    shared_schemas.remove("LifecycleSvc");
    let (s6, d6) = query_lifecycle().await;
    assert_eq!(s6.iter().map(RecordBatch::num_rows).sum::<usize>(), 0);
    assert_eq!(
        extract_kv(&d6),
        vec![
            ("extra".to_string(), None),
            ("profile".to_string(), None),
            ("settings".to_string(), None),
        ]
    );
}
