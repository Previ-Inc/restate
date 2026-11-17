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
use std::time::Instant;

use base64::Engine;
use bytes::Bytes;
use datafusion::arrow::array::{Array, BooleanArray, Float64Array, Int64Array, LargeStringArray};
use datafusion::arrow::record_batch::RecordBatch;
use futures::StreamExt;
use prost::Message;
use prost_types::field_descriptor_proto::{Label, Type};
use prost_types::{
    DescriptorProto, EnumDescriptorProto, EnumValueDescriptorProto, FieldDescriptorProto,
    FileDescriptorProto, FileDescriptorSet, MessageOptions,
};
use restate_storage_api::Transaction;
use restate_storage_api::state_table::WriteStateTable;
use restate_types::deployment::{DeploymentAddress, Headers};
use restate_types::identifiers::{DeploymentId, ServiceId, ServiceRevision};
use restate_types::live::Live;
use restate_types::net::address::{AdvertisedAddress, HttpIngressPort};
use restate_types::schema::deployment::{Deployment, DeploymentResolver};
use restate_types::schema::service::{ServiceMetadata, ServiceMetadataResolver};

use super::wire::{extract_bool, extract_f64, extract_i64, extract_str, transcode_to_json};
use super::{
    CompiledDescriptorPool, DynamicProtoSchemaRegistry, PROTO_DESCRIPTOR_SET_KEY,
    PROTO_STATE_KEY_PREFIX,
};
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

#[restate_core::test(flavor = "multi_thread", worker_threads = 2)]
async fn dynamic_proto_schema_registry_and_sql_udfs() {
    let mut mock_schemas = MockSchemas::default();
    mock_schemas
        .0
        .add(make_service_metadata_with_proto("BusyMember"));

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

    // 1. Verify sys_proto_schemas introspection table
    let schema_batches: Vec<RecordBatch> = engine
        .execute("SELECT service_name, state_key, message_name FROM sys_proto_schemas")
        .await
        .unwrap()
        .stream
        .map(Result::unwrap)
        .collect()
        .await;
    assert_eq!(schema_batches.len(), 1);
    let sb = &schema_batches[0];
    let svc_col = sb
        .column_by_name("service_name")
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
    assert_eq!(svc_col.value(0), "BusyMember");
    assert_eq!(msg_col.value(0), "previ.bench.BusyMemberProfile");

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

#[restate_core::test(flavor = "multi_thread", worker_threads = 4)]
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
                tenant_id: tid.clone(),
                phone: phone.clone(),
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
                metadata: meta.clone(),
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
    let pool = CompiledDescriptorPool::from_file_descriptor_set(&fds);
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

    // 4. Bounded path_cache & no negative caching:
    for i in 0..500 {
        assert!(
            msg.resolve_path(&format!("nonexistent_{i}"), &pool)
                .is_none()
        );
    }
    for i in 0..400 {
        assert!(
            msg.resolve_path(&format!("counters.dyn_{i}"), &pool)
                .is_some()
        );
    }
    assert!(msg.cached_path_count() <= super::MAX_CACHED_PATHS_PER_MESSAGE);
}

#[derive(Clone, Default, Debug)]
struct SharedMockSchemas(std::sync::Arc<parking_lot::RwLock<MockSchemas>>);

impl ServiceMetadataResolver for SharedMockSchemas {
    fn resolve_latest_service(&self, service_name: impl AsRef<str>) -> Option<ServiceMetadata> {
        self.0.read().resolve_latest_service(service_name)
    }

    fn resolve_latest_service_openapi(
        &self,
        svc: impl AsRef<str>,
        addr: AdvertisedAddress<HttpIngressPort>,
    ) -> Option<serde_json::Value> {
        self.0.read().resolve_latest_service_openapi(svc, addr)
    }

    fn list_services(&self) -> Vec<ServiceMetadata> {
        self.0.read().list_services()
    }

    fn list_service_names(&self) -> Vec<String> {
        self.0.read().list_service_names()
    }
}

impl DeploymentResolver for SharedMockSchemas {
    fn resolve_latest_deployment_for_service(
        &self,
        service_name: impl AsRef<str>,
    ) -> Option<Deployment> {
        self.0
            .read()
            .resolve_latest_deployment_for_service(service_name)
    }

    fn find_deployment(
        &self,
        deployment_address: &DeploymentAddress,
        additional_headers: &Headers,
    ) -> Option<(Deployment, Vec<ServiceMetadata>)> {
        self.0
            .read()
            .find_deployment(deployment_address, additional_headers)
    }

    fn get_deployment(&self, deployment_id: &DeploymentId) -> Option<Deployment> {
        self.0.read().get_deployment(deployment_id)
    }

    fn get_deployment_and_services(
        &self,
        deployment_id: &DeploymentId,
    ) -> Option<(Deployment, Vec<ServiceMetadata>)> {
        self.0.read().get_deployment_and_services(deployment_id)
    }

    fn get_deployments(&self) -> Vec<(Deployment, Vec<(String, ServiceRevision)>)> {
        self.0.read().get_deployments()
    }
}

#[restate_core::test(flavor = "multi_thread", worker_threads = 2)]
async fn schema_cache_invalidation_and_mixed_state_key_batches() {
    // 1. Verify DynamicProtoSchemaRegistry invalidates on live metadata/revision updates
    //    and recovers from initial negative (None) lookups:
    let shared_schemas = SharedMockSchemas::default();
    let registry = DynamicProtoSchemaRegistry::new(Live::from_value(shared_schemas.clone()));

    // Initial lookup before service exists returns None without poisoning future lookups:
    assert!(registry.resolve_service_schemas("DynamicSvc").is_none());

    // Register service without proto metadata -> still None:
    shared_schemas
        .0
        .write()
        .0
        .add(ServiceMetadata::mock_virtual_object("DynamicSvc", ["run"]));
    assert!(registry.resolve_service_schemas("DynamicSvc").is_none());

    // Publish v1 proto metadata -> registry immediately resolves revision 1:
    let mut svc_v1 = make_service_metadata_with_proto("DynamicSvc");
    svc_v1.revision = 1;
    shared_schemas.0.write().0.add(svc_v1);
    let rev1 = registry
        .resolve_service_schemas("DynamicSvc")
        .expect("v1 schema should resolve");
    assert_eq!(rev1.revision, 1);
    assert!(rev1.resolve_state_key("profile").is_some());
    assert!(rev1.resolve_state_key("settings").is_none());

    // Publish v2 proto metadata adding a `settings` state key mapping -> registry resolves revision 2:
    let mut svc_v2 = make_service_metadata_with_proto("DynamicSvc");
    svc_v2.revision = 2;
    svc_v2.metadata.insert(
        format!("{PROTO_STATE_KEY_PREFIX}settings"),
        "previ.bench.BusyMemberProfile".to_string(),
    );
    shared_schemas.0.write().0.add(svc_v2);
    let rev2 = registry
        .resolve_service_schemas("DynamicSvc")
        .expect("v2 schema should resolve");
    assert_eq!(rev2.revision, 2);
    assert!(rev2.resolve_state_key("settings").is_some());

    // 2. Verify mixed state-key batch `[profile, settings, profile]` across two Virtual Objects
    //    does not misapply `profile` schema to the middle `settings` row via `uniform_batch_str`:
    let field = |name: &str, num: i32, ty: Type| FieldDescriptorProto {
        name: Some(name.to_string()),
        number: Some(num),
        label: Some(Label::Optional as i32),
        r#type: Some(ty as i32),
        ..Default::default()
    };
    let profile_msg = DescriptorProto {
        name: Some("ProfileMsg".to_string()),
        field: vec![field("name", 1, Type::String), field("val", 2, Type::Int64)],
        ..Default::default()
    };
    // SettingsMsg puts `val` on tag 9 instead of tag 2, and `name` on tag 8 instead of tag 1!
    let settings_msg = DescriptorProto {
        name: Some("SettingsMsg".to_string()),
        field: vec![field("name", 8, Type::String), field("val", 9, Type::Int64)],
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
    let fds_b64 = base64::engine::general_purpose::STANDARD.encode(fds.encode_to_vec());
    let mut svc = ServiceMetadata::mock_virtual_object("MixedKeySvc", ["get"]);
    svc.metadata
        .insert(PROTO_DESCRIPTOR_SET_KEY.to_string(), fds_b64);
    svc.metadata.insert(
        format!("{PROTO_STATE_KEY_PREFIX}profile"),
        "previ.multi.ProfileMsg".to_string(),
    );
    svc.metadata.insert(
        format!("{PROTO_STATE_KEY_PREFIX}settings"),
        "previ.multi.SettingsMsg".to_string(),
    );
    shared_schemas.0.write().0.add(svc);

    #[derive(Clone, PartialEq, prost::Message)]
    struct ProfileWire {
        #[prost(string, tag = "1")]
        name: String,
        #[prost(int64, tag = "2")]
        val: i64,
    }

    #[derive(Clone, PartialEq, prost::Message)]
    struct SettingsWire {
        #[prost(string, tag = "8")]
        name: String,
        #[prost(int64, tag = "9")]
        val: i64,
    }

    let mut engine =
        MockQueryEngine::create_with(MockStatusHandle::default(), shared_schemas).await;
    {
        let mut tx = engine.partition_store().transaction();
        // obj_1 has both "profile" and "settings"; obj_2 has "profile".
        // Physical RocksDB scan order yields keys: ["profile", "settings", "profile"]!
        tx.put_user_state(
            &ServiceId::new(None, "MixedKeySvc", "obj_1"),
            &Bytes::from_static(b"profile"),
            ProfileWire {
                name: "alice".to_string(),
                val: 11,
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
            }
            .encode_to_vec(),
        )
        .unwrap();
        tx.commit().await.unwrap();
    }

    let batches: Vec<RecordBatch> = engine
        .execute(
            "SELECT
                service_key,
                key,
                proto_get_str(service_name, key, value, 'name') AS name,
                proto_get_int(service_name, key, value, 'val') AS val_int,
                proto_get_float(service_name, key, value, 'val') AS val_float
            FROM state
            WHERE scope IS NULL AND service_name = 'MixedKeySvc'
            ORDER BY service_key, key",
        )
        .await
        .unwrap()
        .stream
        .map(Result::unwrap)
        .collect()
        .await;
    assert_eq!(batches.len(), 1);
    let b = &batches[0];
    assert_eq!(b.num_rows(), 3);

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

    assert_eq!(names_col.value(0), "alice");
    assert_eq!(names_col.value(1), "dark-mode");
    assert_eq!(names_col.value(2), "bob");
    assert_eq!(ints_col.value(0), 11);
    assert_eq!(ints_col.value(1), 22);
    assert_eq!(ints_col.value(2), 33);
    assert_eq!(floats_col.value(0), 11.0);
    assert_eq!(floats_col.value(1), 22.0);
    assert_eq!(floats_col.value(2), 33.0);

    // 3. Verify all-Scalar UDF evaluation returns Scalar cleanly on multi-row and constant queries:
    let scalar_batches: Vec<RecordBatch> = engine
        .execute(
            "SELECT
                service_key,
                proto_get_str('MixedKeySvc', 'profile', arrow_cast(X'0a056361726f6c102a', 'LargeBinary'), 'name') AS const_name,
                proto_get_int('MixedKeySvc', 'profile', arrow_cast(X'0a056361726f6c102a', 'LargeBinary'), 'val') AS const_val
            FROM state
            WHERE scope IS NULL AND service_name = 'MixedKeySvc'
            ORDER BY service_key, key",
        )
        .await
        .unwrap()
        .stream
        .map(Result::unwrap)
        .collect()
        .await;
    assert_eq!(scalar_batches[0].num_rows(), 3);
    let const_names = scalar_batches[0]
        .column_by_name("const_name")
        .unwrap()
        .as_any()
        .downcast_ref::<LargeStringArray>()
        .unwrap();
    let const_vals = scalar_batches[0]
        .column_by_name("const_val")
        .unwrap()
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(const_names.value(0), "carol");
    assert_eq!(const_vals.value(2), 42);
}
