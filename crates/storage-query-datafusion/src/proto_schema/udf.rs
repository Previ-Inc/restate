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

use ahash::AHashMap;
use datafusion::arrow::array::{
    Array, ArrayRef, BinaryArray, BinaryViewArray, BooleanBuilder, FixedSizeBinaryArray,
    Float64Builder, Int64Builder, LargeBinaryArray, LargeStringArray, LargeStringBuilder,
    StringArray, StringViewArray, new_empty_array, new_null_array,
};
use datafusion::arrow::datatypes::DataType;
use datafusion::common::types::logical_string;
use datafusion::common::{DataFusionError, Result as DfResult, ScalarValue};
use datafusion::logical_expr::{
    Coercion, ColumnarValue, ScalarUDF, Signature, SimpleScalarUDF, TypeSignatureClass, Volatility,
};

use restate_types::schema::service::ServiceMetadataResolver;

use super::wire::{extract_bool, extract_f64, extract_i64, extract_str, transcode_to_json};
use super::{
    CompiledDescriptorPool, CompiledMessageSchema, DynamicProtoSchemaRegistry, PathStep,
    ServiceProtoSchemas,
};
use crate::{BuildError, TableInventoryBuilder};

type ResolvedPath = Option<(Arc<ServiceProtoSchemas>, Arc<Vec<PathStep>>)>;
type ResolvedMsgSchema = Option<(Arc<ServiceProtoSchemas>, Arc<CompiledMessageSchema>)>;

/// Creates all dynamic Protobuf Scalar UDFs (`proto_get_str`, `proto_get_int`,
/// `proto_get_float`, `proto_get_bool`, `proto_to_json`) backed by `registry`.
pub fn create_udfs<SMR: ServiceMetadataResolver + Send + Sync + Clone + 'static>(
    registry: DynamicProtoSchemaRegistry<SMR>,
) -> Vec<ScalarUDF> {
    vec![
        make_typed_getter_udf(
            "proto_get_str",
            DataType::LargeUtf8,
            registry.clone(),
            |n| LargeStringBuilder::with_capacity(n, n.saturating_mul(24)),
            |b, row| match row.and_then(|(bytes, steps, pool)| extract_str(bytes, steps, pool)) {
                Some(v) => b.append_value(v.as_ref()),
                None => b.append_null(),
            },
        ),
        make_typed_getter_udf(
            "proto_get_int",
            DataType::Int64,
            registry.clone(),
            Int64Builder::with_capacity,
            |b, row| {
                b.append_option(
                    row.and_then(|(bytes, steps, pool)| extract_i64(bytes, steps, pool)),
                )
            },
        ),
        make_typed_getter_udf(
            "proto_get_float",
            DataType::Float64,
            registry.clone(),
            Float64Builder::with_capacity,
            |b, row| {
                b.append_option(
                    row.and_then(|(bytes, steps, pool)| extract_f64(bytes, steps, pool)),
                )
            },
        ),
        make_typed_getter_udf(
            "proto_get_bool",
            DataType::Boolean,
            registry.clone(),
            BooleanBuilder::with_capacity,
            |b, row| {
                b.append_option(
                    row.and_then(|(bytes, steps, pool)| extract_bool(bytes, steps, pool)),
                )
            },
        ),
        make_proto_to_json_udf(registry),
    ]
}

pub fn register_udfs<SMR: ServiceMetadataResolver + Send + Sync + Clone + 'static>(
    inventory: &mut TableInventoryBuilder<'_>,
    registry: DynamicProtoSchemaRegistry<SMR>,
) -> Result<(), BuildError> {
    for udf in create_udfs(registry) {
        inventory.add_udf(udf)?;
    }
    Ok(())
}

#[inline]
fn str_coercion() -> Coercion {
    Coercion::new_exact(TypeSignatureClass::Native(logical_string()))
}

#[inline]
fn binary_coercion() -> Coercion {
    Coercion::new_exact(TypeSignatureClass::Binary)
}

/// Uses `Volatility::Stable` so DataFusion can push filters down and apply CSE within a query,
/// while avoiding cross-query `Immutable` constant folding against dynamically updated schemas.
const PROTO_UDF_VOLATILITY: Volatility = Volatility::Stable;

enum StringColumn<'a> {
    Small(&'a StringArray),
    Large(&'a LargeStringArray),
    View(&'a StringViewArray),
}

impl<'a> StringColumn<'a> {
    #[inline]
    fn from_array(arr: &'a ArrayRef) -> Option<Self> {
        let any = arr.as_any();
        if let Some(a) = any.downcast_ref::<LargeStringArray>() {
            Some(Self::Large(a))
        } else if let Some(a) = any.downcast_ref::<StringArray>() {
            Some(Self::Small(a))
        } else {
            any.downcast_ref::<StringViewArray>().map(Self::View)
        }
    }

    #[inline]
    fn len(&self) -> usize {
        match self {
            Self::Small(a) => a.len(),
            Self::Large(a) => a.len(),
            Self::View(a) => a.len(),
        }
    }

    #[inline]
    fn value(&self, idx: usize) -> &'a str {
        match self {
            Self::Small(a) => a.value(idx),
            Self::Large(a) => a.value(idx),
            Self::View(a) => a.value(idx),
        }
    }
}

enum BinaryColumn<'a> {
    Large(&'a LargeBinaryArray),
    Small(&'a BinaryArray),
    View(&'a BinaryViewArray),
    Fixed(&'a FixedSizeBinaryArray),
}

impl<'a> BinaryColumn<'a> {
    #[inline]
    fn from_array(arr: &'a ArrayRef) -> Option<Self> {
        let any = arr.as_any();
        if let Some(a) = any.downcast_ref::<LargeBinaryArray>() {
            Some(Self::Large(a))
        } else if let Some(a) = any.downcast_ref::<BinaryArray>() {
            Some(Self::Small(a))
        } else if let Some(a) = any.downcast_ref::<BinaryViewArray>() {
            Some(Self::View(a))
        } else {
            any.downcast_ref::<FixedSizeBinaryArray>().map(Self::Fixed)
        }
    }

    #[inline]
    fn is_null(&self, idx: usize) -> bool {
        match self {
            Self::Large(a) => a.is_null(idx),
            Self::Small(a) => a.is_null(idx),
            Self::View(a) => a.is_null(idx),
            Self::Fixed(a) => a.is_null(idx),
        }
    }

    #[inline]
    fn value(&self, idx: usize) -> &'a [u8] {
        match self {
            Self::Large(a) => a.value(idx),
            Self::Small(a) => a.value(idx),
            Self::View(a) => a.value(idx),
            Self::Fixed(a) => a.value(idx),
        }
    }
}

#[inline]
fn scalar_str(arg: &ColumnarValue) -> Option<&str> {
    match arg {
        ColumnarValue::Scalar(s) => s.try_as_str().flatten(),
        ColumnarValue::Array(_) => None,
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
fn uniform_batch_str<'a>(arg: &'a ColumnarValue, buf: Option<&'a ArrayRef>) -> Option<&'a str> {
    if let Some(s) = scalar_str(arg) {
        return Some(s);
    }
    let arr = buf?;
    if arr.is_empty() || arr.null_count() > 0 {
        return None;
    }
    let col = StringColumn::from_array(arr)?;
    is_uniform_str_slice(col.len(), |i| col.value(i))
}

#[inline]
fn extract_str_arg<'a>(
    arg: &'a ColumnarValue,
    row_idx: usize,
    buf: Option<&'a ArrayRef>,
) -> Option<&'a str> {
    match arg {
        ColumnarValue::Scalar(s) => s.try_as_str().flatten(),
        ColumnarValue::Array(_) => {
            let arr = buf?;
            if arr.is_null(row_idx) {
                return None;
            }
            StringColumn::from_array(arr).map(|col| col.value(row_idx))
        }
    }
}

#[inline]
fn extract_bytes_arg<'a>(
    arg: &'a ColumnarValue,
    row_idx: usize,
    buf: Option<&'a ArrayRef>,
) -> Option<&'a [u8]> {
    match arg {
        ColumnarValue::Scalar(
            ScalarValue::Binary(Some(b))
            | ScalarValue::LargeBinary(Some(b))
            | ScalarValue::BinaryView(Some(b))
            | ScalarValue::FixedSizeBinary(_, Some(b)),
        ) => Some(b.as_slice()),
        ColumnarValue::Array(_) => {
            let arr = buf?;
            if arr.is_null(row_idx) {
                return None;
            }
            BinaryColumn::from_array(arr).map(|col| col.value(row_idx))
        }
        _ => None,
    }
}

#[inline]
fn to_array_opt(cv: &ColumnarValue) -> Option<&ArrayRef> {
    match cv {
        ColumnarValue::Array(a) => Some(a),
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

fn short_circuit_trivial(
    return_type: &DataType,
    args: &[ColumnarValue],
    array_len: Option<usize>,
) -> DfResult<Option<ColumnarValue>> {
    if array_len == Some(0) {
        return Ok(Some(ColumnarValue::Array(new_empty_array(return_type))));
    }
    if args
        .iter()
        .any(|arg| matches!(arg, ColumnarValue::Scalar(s) if s.is_null()))
    {
        return match array_len {
            Some(len) => Ok(Some(ColumnarValue::Array(new_null_array(return_type, len)))),
            None => Ok(Some(ColumnarValue::Scalar(ScalarValue::try_new_null(
                return_type,
            )?))),
        };
    }
    Ok(None)
}

fn finish_columnar_output(arr: ArrayRef, array_len: Option<usize>) -> DfResult<ColumnarValue> {
    match array_len {
        Some(_) => Ok(ColumnarValue::Array(arr)),
        None => Ok(ColumnarValue::Scalar(ScalarValue::try_from_array(&arr, 0)?)),
    }
}

fn resolve_path_from_svc(
    svc_schemas: Option<&Arc<ServiceProtoSchemas>>,
    state_key: &str,
    field_path: &str,
) -> ResolvedPath {
    let svc = svc_schemas?;
    let msg = svc.resolve_state_key(state_key)?;
    let steps = msg.resolve_path(field_path, &svc.pool)?;
    Some((Arc::clone(svc), steps))
}

fn eval_typed_getter_batch<SMR, F>(
    registry: &DynamicProtoSchemaRegistry<SMR>,
    args: &[ColumnarValue],
    num_rows: usize,
    mut append_row: F,
) where
    SMR: ServiceMetadataResolver + Send + Sync + 'static,
    F: FnMut(Option<(&[u8], &[PathStep], &CompiledDescriptorPool)>),
{
    let a0 = to_array_opt(&args[0]);
    let a1 = to_array_opt(&args[1]);
    let a2 = to_array_opt(&args[2]);
    let a3 = to_array_opt(&args[3]);

    if let (Some(svc_name), Some(state_key), Some(field_path), Some(val_col)) = (
        uniform_batch_str(&args[0], a0),
        uniform_batch_str(&args[1], a1),
        uniform_batch_str(&args[3], a3),
        a2.and_then(BinaryColumn::from_array),
    ) {
        let resolved = resolve_path_from_svc(
            registry.resolve_service_schemas(svc_name).as_ref(),
            state_key,
            field_path,
        );
        if let Some((svc_schemas, steps)) = resolved {
            let pool = svc_schemas.pool.as_ref();
            let steps_slice = steps.as_slice();
            for i in 0..num_rows {
                append_row((!val_col.is_null(i)).then(|| (val_col.value(i), steps_slice, pool)));
            }
        } else {
            for _ in 0..num_rows {
                append_row(None);
            }
        }
        return;
    }

    let mut svc_cache: AHashMap<&str, Option<Arc<ServiceProtoSchemas>>> = AHashMap::new();
    let mut path_cache: AHashMap<(&str, &str, &str), ResolvedPath> = AHashMap::new();

    for i in 0..num_rows {
        let (Some(svc_name), Some(state_key), Some(bytes), Some(field_path)) = (
            extract_str_arg(&args[0], i, a0),
            extract_str_arg(&args[1], i, a1),
            extract_bytes_arg(&args[2], i, a2),
            extract_str_arg(&args[3], i, a3),
        ) else {
            append_row(None);
            continue;
        };

        let resolved: &ResolvedPath = path_cache
            .entry((svc_name, state_key, field_path))
            .or_insert_with(|| {
                let svc = svc_cache
                    .entry(svc_name)
                    .or_insert_with(|| registry.resolve_service_schemas(svc_name))
                    .as_ref();
                resolve_path_from_svc(svc, state_key, field_path)
            });

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
    E: Fn(&mut B, Option<(&[u8], &[PathStep], &CompiledDescriptorPool)>) + Send + Sync + 'static,
{
    let rt = return_type.clone();
    let func = move |args: &[ColumnarValue]| -> DfResult<ColumnarValue> {
        if args.len() != 4 {
            return Err(DataFusionError::Execution(format!(
                "{name} expects 4 arguments: (service_name, state_key, value, field_path)"
            )));
        }
        let array_len = resolve_batch_len(name, args)?;
        if let Some(short) = short_circuit_trivial(&rt, args, array_len)? {
            return Ok(short);
        }
        let num_rows = array_len.unwrap_or(1);
        let mut builder = make_builder(num_rows);
        eval_typed_getter_batch(&registry, args, num_rows, |row| eval_row(&mut builder, row));
        finish_columnar_output(builder.finish(), array_len)
    };

    ScalarUDF::from(SimpleScalarUDF::new_with_signature(
        name,
        Signature::coercible(
            vec![
                str_coercion(),
                str_coercion(),
                binary_coercion(),
                str_coercion(),
            ],
            PROTO_UDF_VOLATILITY,
        ),
        return_type,
        Arc::new(func),
    ))
}

fn append_transcoded_json_row(
    builder: &mut LargeStringBuilder,
    bytes: &[u8],
    msg: &CompiledMessageSchema,
    pool: &CompiledDescriptorPool,
    scratch: &mut Vec<u8>,
) {
    scratch.clear();
    if let Some(json_val) = transcode_to_json(bytes, msg, pool)
        && serde_json::to_writer(&mut *scratch, &json_val).is_ok()
        && let Ok(json_str) = std::str::from_utf8(scratch)
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

    if let (Some(svc_name), Some(state_key), Some(val_col)) = (
        uniform_batch_str(&args[0], a0),
        uniform_batch_str(&args[1], a1),
        a2.and_then(BinaryColumn::from_array),
    ) {
        let resolved = registry.resolve_service_schemas(svc_name).and_then(|svc| {
            let msg = svc.resolve_state_key(state_key)?.clone();
            Some((svc, msg))
        });
        if let Some((svc_schemas, msg)) = resolved {
            let mut scratch = Vec::with_capacity(256);
            for i in 0..num_rows {
                if val_col.is_null(i) {
                    builder.append_null();
                } else {
                    append_transcoded_json_row(
                        builder,
                        val_col.value(i),
                        &msg,
                        &svc_schemas.pool,
                        &mut scratch,
                    );
                }
            }
        } else {
            for _ in 0..num_rows {
                builder.append_null();
            }
        }
        return;
    }

    let mut svc_cache: AHashMap<&str, Option<Arc<ServiceProtoSchemas>>> = AHashMap::new();
    let mut msg_cache: AHashMap<(&str, &str), ResolvedMsgSchema> = AHashMap::new();
    let mut scratch = Vec::with_capacity(256);

    for i in 0..num_rows {
        let (Some(svc_name), Some(state_key), Some(bytes)) = (
            extract_str_arg(&args[0], i, a0),
            extract_str_arg(&args[1], i, a1),
            extract_bytes_arg(&args[2], i, a2),
        ) else {
            builder.append_null();
            continue;
        };

        let resolved: &ResolvedMsgSchema =
            msg_cache.entry((svc_name, state_key)).or_insert_with(|| {
                let svc = svc_cache
                    .entry(svc_name)
                    .or_insert_with(|| registry.resolve_service_schemas(svc_name))
                    .as_ref()?;
                let msg = svc.resolve_state_key(state_key)?;
                Some((Arc::clone(svc), Arc::clone(msg)))
            });

        if let Some((svc_schemas, msg)) = resolved {
            append_transcoded_json_row(builder, bytes, msg, &svc_schemas.pool, &mut scratch);
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
    if let Some(short) = short_circuit_trivial(&DataType::LargeUtf8, args, array_len)? {
        return Ok(short);
    }
    let num_rows = array_len.unwrap_or(1);
    let mut builder = LargeStringBuilder::with_capacity(num_rows, num_rows.saturating_mul(128));
    eval_proto_to_json_batch(registry, args, num_rows, &mut builder);
    finish_columnar_output(Arc::new(builder.finish()), array_len)
}

fn make_proto_to_json_udf<SMR: ServiceMetadataResolver + Send + Sync + Clone + 'static>(
    registry: DynamicProtoSchemaRegistry<SMR>,
) -> ScalarUDF {
    ScalarUDF::from(SimpleScalarUDF::new_with_signature(
        "proto_to_json",
        Signature::coercible(
            vec![str_coercion(), str_coercion(), binary_coercion()],
            PROTO_UDF_VOLATILITY,
        ),
        DataType::LargeUtf8,
        Arc::new(move |args| eval_json_udf_call(&registry, args)),
    ))
}
