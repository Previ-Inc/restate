// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! DataFusion Scalar UDFs for querying binary Protobuf state without JSON overhead:
//! - `proto_get_str(service_name, state_key, value, field_path) -> LargeUtf8`
//! - `proto_get_int(service_name, state_key, value, field_path) -> Int64`
//! - `proto_get_float(service_name, state_key, value, field_path) -> Float64`
//! - `proto_get_bool(service_name, state_key, value, field_path) -> Boolean`
//! - `proto_to_json(service_name, state_key, value) -> LargeUtf8`

use std::sync::Arc;

use datafusion::arrow::array::{
    Array, ArrayRef, BinaryArray, BooleanBuilder, Float64Builder, Int64Builder, LargeBinaryArray,
    LargeStringArray, LargeStringBuilder, StringArray,
};
use datafusion::arrow::datatypes::DataType;
use datafusion::common::{DataFusionError, Result as DfResult, ScalarValue};
use datafusion::logical_expr::{ColumnarValue, ScalarUDF, Volatility, create_udf};
use restate_types::schema::service::ServiceMetadataResolver;

use super::DynamicProtoSchemaRegistry;
use super::wire::{extract_bool, extract_f64, extract_i64, extract_str, transcode_to_json};
use crate::TableInventoryBuilder;
type ResolvedPath = Option<(Arc<super::ServiceProtoSchemas>, Arc<Vec<super::PathStep>>)>;
type ResolvedMsgSchema = Option<(
    Arc<super::ServiceProtoSchemas>,
    Arc<super::CompiledMessageSchema>,
)>;

pub fn register_udfs<SMR: ServiceMetadataResolver + Send + Sync + Clone + 'static>(
    inventory: &mut TableInventoryBuilder<'_>,
    registry: DynamicProtoSchemaRegistry<SMR>,
) {
    inventory.add_udf(make_typed_getter_udf(
        "proto_get_str",
        DataType::LargeUtf8,
        registry.clone(),
        |n| LargeStringBuilder::with_capacity(n, n.saturating_mul(24)),
        |b, row| match row.and_then(|(bytes, steps, pool)| extract_str(bytes, steps, pool)) {
            Some(v) => b.append_value(v.as_ref()),
            None => b.append_null(),
        },
    ));
    inventory.add_udf(make_typed_getter_udf(
        "proto_get_int",
        DataType::Int64,
        registry.clone(),
        Int64Builder::with_capacity,
        |b, row| {
            b.append_option(row.and_then(|(bytes, steps, pool)| extract_i64(bytes, steps, pool)))
        },
    ));
    inventory.add_udf(make_typed_getter_udf(
        "proto_get_float",
        DataType::Float64,
        registry.clone(),
        Float64Builder::with_capacity,
        |b, row| {
            b.append_option(row.and_then(|(bytes, steps, pool)| extract_f64(bytes, steps, pool)))
        },
    ));
    inventory.add_udf(make_typed_getter_udf(
        "proto_get_bool",
        DataType::Boolean,
        registry.clone(),
        BooleanBuilder::with_capacity,
        |b, row| {
            b.append_option(row.and_then(|(bytes, steps, pool)| extract_bool(bytes, steps, pool)))
        },
    ));
    inventory.add_udf(make_proto_to_json_udf(registry));
}

#[inline]
fn scalar_str(arg: &ColumnarValue) -> Option<&str> {
    match arg {
        ColumnarValue::Scalar(ScalarValue::Utf8(Some(s)) | ScalarValue::LargeUtf8(Some(s))) => {
            Some(s.as_str())
        }
        _ => None,
    }
}

#[inline]
fn is_uniform_str_slice<'a>(len: usize, mut get: impl FnMut(usize) -> &'a str) -> Option<&'a str> {
    let first = get(0);
    if get(len - 1) == first && (1..len.saturating_sub(1)).all(|i| get(i) == first) {
        Some(first)
    } else {
        None
    }
}

#[inline]
fn uniform_batch_str<'a>(arg: &'a ColumnarValue, buf: &'a Option<ArrayRef>) -> Option<&'a str> {
    if let Some(s) = scalar_str(arg) {
        return Some(s);
    }
    let arr = buf.as_ref()?;
    if arr.is_empty() || arr.null_count() > 0 {
        return None;
    }
    if let Some(s_arr) = arr.as_any().downcast_ref::<LargeStringArray>() {
        return is_uniform_str_slice(s_arr.len(), |i| s_arr.value(i));
    }
    if let Some(s_arr) = arr.as_any().downcast_ref::<StringArray>() {
        return is_uniform_str_slice(s_arr.len(), |i| s_arr.value(i));
    }
    None
}

#[inline]
fn extract_str_arg<'a>(
    arg: &'a ColumnarValue,
    row_idx: usize,
    buf: &'a Option<ArrayRef>,
) -> Option<&'a str> {
    match arg {
        ColumnarValue::Scalar(ScalarValue::Utf8(Some(s)) | ScalarValue::LargeUtf8(Some(s))) => {
            Some(s.as_str())
        }
        ColumnarValue::Array(_) => {
            let arr = buf.as_ref()?;
            if arr.is_null(row_idx) {
                return None;
            }
            if let Some(s_arr) = arr.as_any().downcast_ref::<LargeStringArray>() {
                Some(s_arr.value(row_idx))
            } else if let Some(s_arr) = arr.as_any().downcast_ref::<StringArray>() {
                Some(s_arr.value(row_idx))
            } else {
                None
            }
        }
        _ => None,
    }
}

#[inline]
fn extract_bytes_arg<'a>(
    arg: &'a ColumnarValue,
    row_idx: usize,
    buf: &'a Option<ArrayRef>,
) -> Option<&'a [u8]> {
    match arg {
        ColumnarValue::Scalar(ScalarValue::Binary(Some(b)) | ScalarValue::LargeBinary(Some(b))) => {
            Some(b.as_slice())
        }
        ColumnarValue::Array(_) => {
            let arr = buf.as_ref()?;
            if arr.is_null(row_idx) {
                return None;
            }
            if let Some(b_arr) = arr.as_any().downcast_ref::<LargeBinaryArray>() {
                Some(b_arr.value(row_idx))
            } else if let Some(b_arr) = arr.as_any().downcast_ref::<BinaryArray>() {
                Some(b_arr.value(row_idx))
            } else {
                None
            }
        }
        _ => None,
    }
}

fn to_array_opt(cv: &ColumnarValue) -> Option<ArrayRef> {
    match cv {
        ColumnarValue::Array(a) => Some(a.clone()),
        ColumnarValue::Scalar(_) => None,
    }
}

fn resolve_batch_len(udf_name: &str, args: &[ColumnarValue]) -> DfResult<Option<usize>> {
    let mut array_len: Option<usize> = None;
    for arg in args {
        if let ColumnarValue::Array(arr) = arg {
            match array_len {
                None => array_len = Some(arr.len()),
                Some(expected) if expected != arr.len() => {
                    return Err(DataFusionError::Execution(format!(
                        "{udf_name} received mismatched array lengths: {expected} vs {}",
                        arr.len()
                    )));
                }
                Some(_) => {}
            }
        }
    }
    Ok(array_len)
}

fn finish_columnar_output(arr: ArrayRef, array_len: Option<usize>) -> DfResult<ColumnarValue> {
    match array_len {
        Some(_) => Ok(ColumnarValue::Array(arr)),
        None => Ok(ColumnarValue::Scalar(ScalarValue::try_from_array(&arr, 0)?)),
    }
}

fn resolve_svc_cached<'a, SMR: ServiceMetadataResolver + Send + Sync + 'static>(
    registry: &DynamicProtoSchemaRegistry<SMR>,
    svc_name: &'a str,
    last_svc: &mut Option<(&'a str, Option<Arc<super::ServiceProtoSchemas>>)>,
) -> Option<Arc<super::ServiceProtoSchemas>> {
    match last_svc {
        Some((s, cached)) if *s == svc_name => cached.clone(),
        _ => {
            let resolved = registry.resolve_service_schemas(svc_name);
            *last_svc = Some((svc_name, resolved.clone()));
            resolved
        }
    }
}

fn resolve_path_from_svc(
    svc_schemas: Option<Arc<super::ServiceProtoSchemas>>,
    state_key: &str,
    field_path: &str,
) -> ResolvedPath {
    let svc = svc_schemas?;
    let msg = svc.resolve_state_key(state_key)?;
    let steps = msg.resolve_path(field_path, &svc.pool)?;
    Some((svc, steps))
}

fn eval_typed_getter_batch<SMR, F>(
    registry: &DynamicProtoSchemaRegistry<SMR>,
    args: &[ColumnarValue],
    num_rows: usize,
    mut append_row: F,
) where
    SMR: ServiceMetadataResolver + Send + Sync + 'static,
    F: FnMut(Option<(&[u8], &[super::PathStep], &super::CompiledDescriptorPool)>),
{
    let a0 = to_array_opt(&args[0]);
    let a1 = to_array_opt(&args[1]);
    let a2 = to_array_opt(&args[2]);
    let a3 = to_array_opt(&args[3]);

    if let (Some(svc_name), Some(state_key), Some(field_path), Some(val_arr)) = (
        uniform_batch_str(&args[0], &a0),
        uniform_batch_str(&args[1], &a1),
        uniform_batch_str(&args[3], &a3),
        a2.as_ref()
            .and_then(|a| a.as_any().downcast_ref::<LargeBinaryArray>()),
    ) {
        let resolved = resolve_path_from_svc(
            registry.resolve_service_schemas(svc_name),
            state_key,
            field_path,
        );
        if let Some((svc_schemas, steps)) = resolved {
            let pool = svc_schemas.pool.as_ref();
            let steps_slice = steps.as_slice();
            for i in 0..num_rows {
                append_row((!val_arr.is_null(i)).then(|| (val_arr.value(i), steps_slice, pool)));
            }
        } else {
            for _ in 0..num_rows {
                append_row(None);
            }
        }
        return;
    }

    let mut last_svc: Option<(&str, Option<Arc<super::ServiceProtoSchemas>>)> = None;
    let mut last_lookup: Option<(&str, &str, &str, ResolvedPath)> = None;

    for i in 0..num_rows {
        let (Some(svc_name), Some(state_key), Some(bytes), Some(field_path)) = (
            extract_str_arg(&args[0], i, &a0),
            extract_str_arg(&args[1], i, &a1),
            extract_bytes_arg(&args[2], i, &a2),
            extract_str_arg(&args[3], i, &a3),
        ) else {
            append_row(None);
            continue;
        };

        let resolved = match &last_lookup {
            Some((s, k, p, cached)) if *s == svc_name && *k == state_key && *p == field_path => {
                cached.clone()
            }
            _ => {
                let svc = resolve_svc_cached(registry, svc_name, &mut last_svc);
                let r = resolve_path_from_svc(svc, state_key, field_path);
                last_lookup = Some((svc_name, state_key, field_path, r.clone()));
                r
            }
        };

        if let Some((svc_schemas, steps)) = resolved {
            append_row(Some((bytes, steps.as_slice(), &svc_schemas.pool)));
        } else {
            append_row(None);
        }
    }
}

fn make_typed_getter_udf<SMR, B, E>(
    name: &'static str,
    return_type: DataType,
    registry: DynamicProtoSchemaRegistry<SMR>,
    make_builder: fn(usize) -> B,
    eval_row: E,
) -> ScalarUDF
where
    SMR: ServiceMetadataResolver + Send + Sync + Clone + 'static,
    B: datafusion::arrow::array::ArrayBuilder,
    E: Fn(&mut B, Option<(&[u8], &[super::PathStep], &super::CompiledDescriptorPool)>)
        + Send
        + Sync
        + 'static,
{
    let func = move |args: &[ColumnarValue]| -> DfResult<ColumnarValue> {
        if args.len() != 4 {
            return Err(DataFusionError::Execution(format!(
                "{name} expects 4 arguments: (service_name, state_key, value, field_path)"
            )));
        }
        let array_len = resolve_batch_len(name, args)?;
        let num_rows = array_len.unwrap_or(1);
        let mut builder = make_builder(num_rows);
        eval_typed_getter_batch(&registry, args, num_rows, |row| eval_row(&mut builder, row));
        finish_columnar_output(builder.finish(), array_len)
    };

    create_udf(
        name,
        vec![
            DataType::LargeUtf8,
            DataType::LargeUtf8,
            DataType::LargeBinary,
            DataType::LargeUtf8,
        ],
        return_type,
        Volatility::Stable,
        Arc::new(func),
    )
}

fn append_transcoded_json_row(
    builder: &mut LargeStringBuilder,
    bytes: &[u8],
    msg: &super::CompiledMessageSchema,
    pool: &super::CompiledDescriptorPool,
) {
    if let Some(json_val) = transcode_to_json(bytes, msg, pool)
        && let Ok(json_str) = serde_json::to_string(&json_val)
    {
        builder.append_value(json_str);
    } else {
        builder.append_null();
    }
}

fn eval_proto_to_json_batch<SMR: ServiceMetadataResolver + Send + Sync + 'static>(
    registry: &DynamicProtoSchemaRegistry<SMR>,
    args: &[ColumnarValue],
    num_rows: usize,
    builder: &mut LargeStringBuilder,
) {
    let a0 = to_array_opt(&args[0]);
    let a1 = to_array_opt(&args[1]);
    let a2 = to_array_opt(&args[2]);

    if let (Some(svc_name), Some(state_key), Some(val_arr)) = (
        uniform_batch_str(&args[0], &a0),
        uniform_batch_str(&args[1], &a1),
        a2.as_ref()
            .and_then(|a| a.as_any().downcast_ref::<LargeBinaryArray>()),
    ) {
        let resolved = registry.resolve_service_schemas(svc_name).and_then(|svc| {
            let msg = svc.resolve_state_key(state_key)?.clone();
            Some((svc, msg))
        });
        if let Some((svc_schemas, msg)) = resolved {
            for i in 0..num_rows {
                if val_arr.is_null(i) {
                    builder.append_null();
                } else {
                    append_transcoded_json_row(builder, val_arr.value(i), &msg, &svc_schemas.pool);
                }
            }
        } else {
            for _ in 0..num_rows {
                builder.append_null();
            }
        }
        return;
    }

    let mut last_svc: Option<(&str, Option<Arc<super::ServiceProtoSchemas>>)> = None;
    let mut last_lookup: Option<(&str, &str, ResolvedMsgSchema)> = None;

    for i in 0..num_rows {
        let (Some(svc_name), Some(state_key), Some(bytes)) = (
            extract_str_arg(&args[0], i, &a0),
            extract_str_arg(&args[1], i, &a1),
            extract_bytes_arg(&args[2], i, &a2),
        ) else {
            builder.append_null();
            continue;
        };

        let resolved = match &last_lookup {
            Some((s, k, cached)) if *s == svc_name && *k == state_key => cached.clone(),
            _ => {
                let r = resolve_svc_cached(registry, svc_name, &mut last_svc).and_then(|svc| {
                    let msg = svc.resolve_state_key(state_key)?.clone();
                    Some((svc, msg))
                });
                last_lookup = Some((svc_name, state_key, r.clone()));
                r
            }
        };

        if let Some((svc_schemas, msg)) = resolved {
            append_transcoded_json_row(builder, bytes, &msg, &svc_schemas.pool);
        } else {
            builder.append_null();
        }
    }
}

fn eval_json_udf_call<SMR: ServiceMetadataResolver + Send + Sync + 'static>(
    registry: &DynamicProtoSchemaRegistry<SMR>,
    args: &[ColumnarValue],
) -> DfResult<ColumnarValue> {
    if args.len() != 3 {
        return Err(DataFusionError::Execution(
            "proto_to_json expects 3 arguments: (service_name, state_key, value)".to_string(),
        ));
    }
    let array_len = resolve_batch_len("proto_to_json", args)?;
    let num_rows = array_len.unwrap_or(1);
    let mut builder = LargeStringBuilder::with_capacity(num_rows, num_rows.saturating_mul(128));
    eval_proto_to_json_batch(registry, args, num_rows, &mut builder);
    finish_columnar_output(Arc::new(builder.finish()), array_len)
}

fn make_proto_to_json_udf<SMR: ServiceMetadataResolver + Send + Sync + Clone + 'static>(
    registry: DynamicProtoSchemaRegistry<SMR>,
) -> ScalarUDF {
    create_udf(
        "proto_to_json",
        vec![
            DataType::LargeUtf8,
            DataType::LargeUtf8,
            DataType::LargeBinary,
        ],
        DataType::LargeUtf8,
        Volatility::Stable,
        Arc::new(move |args| eval_json_udf_call(&registry, args)),
    )
}
