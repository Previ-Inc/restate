// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Zero-allocation binary Protobuf wire-format scanner and dynamic JSON transcoder.

use std::borrow::Cow;

use serde_json::{Map, Number, Value};

use super::{
    CompiledDescriptorPool, CompiledField, CompiledMessageSchema, FieldKind,
    MAX_PROTO_RECURSION_DEPTH, PathStep,
};

const WIRE_VARINT: u8 = 0;
const WIRE_I64: u8 = 1;
const WIRE_LEN: u8 = 2;
const WIRE_I32: u8 = 5;

const MAX_JSON_VALUES: usize = 65_536;
#[inline]
pub fn read_varint(buf: &[u8], pos: &mut usize) -> Option<u64> {
    if *pos < buf.len() {
        let b0 = buf[*pos];
        if (b0 & 0x80) == 0 {
            *pos += 1;
            return Some(u64::from(b0));
        }
    }
    let mut result: u64 = 0;
    let mut shift: u32 = 0;
    while *pos < buf.len() {
        let byte = buf[*pos];
        *pos += 1;
        if shift == 63 && (byte & 0x7E) != 0 {
            return None;
        }
        result |= u64::from(byte & 0x7F) << shift;
        if (byte & 0x80) == 0 {
            return Some(result);
        }
        shift += 7;
        if shift >= 64 {
            return None;
        }
    }
    None
}

#[inline]
fn read_wire_tag(buf: &[u8], pos: &mut usize) -> Option<(u32, u8)> {
    let raw = u32::try_from(read_varint(buf, pos)?).ok()?;
    let field_num = raw >> 3;
    if field_num == 0 {
        return None;
    }
    Some((field_num, (raw & 0x07) as u8))
}

#[inline]
pub fn read_len_delimited<'a>(buf: &'a [u8], pos: &mut usize) -> Option<&'a [u8]> {
    let len = usize::try_from(read_varint(buf, pos)?).ok()?;
    let end = pos.checked_add(len)?;
    let slice = buf.get(*pos..end)?;
    *pos = end;
    Some(slice)
}

#[inline]
fn read_fixed<const N: usize>(buf: &[u8], pos: &mut usize) -> Option<[u8; N]> {
    let end = pos.checked_add(N)?;
    let bytes: [u8; N] = buf.get(*pos..end)?.try_into().ok()?;
    *pos = end;
    Some(bytes)
}

#[inline]
pub fn skip_wire_value(buf: &[u8], pos: &mut usize, wire_type: u8) -> bool {
    match wire_type {
        WIRE_VARINT => read_varint(buf, pos).is_some(),
        WIRE_I64 => read_fixed::<8>(buf, pos).is_some(),
        WIRE_LEN => read_len_delimited(buf, pos).is_some(),
        WIRE_I32 => read_fixed::<4>(buf, pos).is_some(),
        _ => false,
    }
}
#[inline]
fn decode_zigzag64(n: u64) -> i64 {
    ((n >> 1) as i64) ^ (-((n & 1) as i64))
}

#[inline]
fn decode_zigzag32(n: u32) -> i32 {
    ((n >> 1) as i32) ^ (-((n & 1) as i32))
}

#[derive(Clone, Copy)]
enum DecodedScalar<'a> {
    Str(&'a str),
    Bytes(&'a [u8]),
    Bool(bool),
    I64(i64),
    U64(u64),
    F64(f64),
    Enum(i32),
}

#[inline]
fn decode_wire_scalar<'a>(
    buf: &'a [u8],
    pos: &mut usize,
    wire_type: u8,
    kind: &FieldKind,
) -> Option<DecodedScalar<'a>> {
    match (kind, wire_type) {
        (FieldKind::String, WIRE_LEN) => {
            let bytes = read_len_delimited(buf, pos)?;
            std::str::from_utf8(bytes).ok().map(DecodedScalar::Str)
        }
        (FieldKind::Bytes, WIRE_LEN) => read_len_delimited(buf, pos).map(DecodedScalar::Bytes),
        (FieldKind::Bool, WIRE_VARINT) => {
            read_varint(buf, pos).map(|v| DecodedScalar::Bool(v != 0))
        }
        (FieldKind::Enum(_), WIRE_VARINT) => {
            read_varint(buf, pos).map(|v| DecodedScalar::Enum(v as i32))
        }
        (FieldKind::Sint32, WIRE_VARINT) => {
            read_varint(buf, pos).map(|v| DecodedScalar::I64(i64::from(decode_zigzag32(v as u32))))
        }
        (FieldKind::Sint64, WIRE_VARINT) => {
            read_varint(buf, pos).map(|v| DecodedScalar::I64(decode_zigzag64(v)))
        }
        (FieldKind::Int32, WIRE_VARINT) => {
            read_varint(buf, pos).map(|v| DecodedScalar::I64(i64::from(v as i32)))
        }
        (FieldKind::Int64, WIRE_VARINT) => {
            read_varint(buf, pos).map(|v| DecodedScalar::I64(v as i64))
        }
        (FieldKind::Uint32, WIRE_VARINT) => {
            read_varint(buf, pos).map(|v| DecodedScalar::U64(u64::from(v as u32)))
        }
        (FieldKind::Uint64, WIRE_VARINT) => read_varint(buf, pos).map(DecodedScalar::U64),
        (FieldKind::Fixed64, WIRE_I64) => {
            read_fixed::<8>(buf, pos).map(|b| DecodedScalar::U64(u64::from_le_bytes(b)))
        }
        (FieldKind::Sfixed64, WIRE_I64) => {
            read_fixed::<8>(buf, pos).map(|b| DecodedScalar::I64(i64::from_le_bytes(b)))
        }
        (FieldKind::Fixed32, WIRE_I32) => {
            read_fixed::<4>(buf, pos).map(|b| DecodedScalar::U64(u64::from(u32::from_le_bytes(b))))
        }
        (FieldKind::Sfixed32, WIRE_I32) => {
            read_fixed::<4>(buf, pos).map(|b| DecodedScalar::I64(i64::from(i32::from_le_bytes(b))))
        }
        (FieldKind::Double, WIRE_I64) => {
            read_fixed::<8>(buf, pos).map(|b| DecodedScalar::F64(f64::from_le_bytes(b)))
        }
        (FieldKind::Float, WIRE_I32) => {
            read_fixed::<4>(buf, pos).map(|b| DecodedScalar::F64(f64::from(f32::from_le_bytes(b))))
        }
        _ => None,
    }
}

impl<'a> DecodedScalar<'a> {
    #[inline]
    fn into_cow_str(self, kind: &FieldKind, pool: &CompiledDescriptorPool) -> Cow<'a, str> {
        match self {
            DecodedScalar::Str(s) => Cow::Borrowed(s),
            DecodedScalar::Bytes(b) => {
                use base64::Engine;
                Cow::Owned(base64::engine::general_purpose::STANDARD.encode(b))
            }
            DecodedScalar::Bool(b) => Cow::Borrowed(if b { "true" } else { "false" }),
            DecodedScalar::I64(v) => Cow::Owned(v.to_string()),
            DecodedScalar::U64(v) => Cow::Owned(v.to_string()),
            DecodedScalar::F64(v) => Cow::Owned(v.to_string()),
            DecodedScalar::Enum(v) => {
                if let FieldKind::Enum(enum_name) = kind
                    && let Some(name) = pool.resolve_enum_name(enum_name, v)
                {
                    Cow::Owned(name.to_string())
                } else {
                    Cow::Owned(v.to_string())
                }
            }
        }
    }

    #[inline]
    fn into_i64(self) -> Option<i64> {
        match self {
            DecodedScalar::I64(v) => Some(v),
            DecodedScalar::U64(v) => i64::try_from(v).ok(),
            DecodedScalar::Bool(b) => Some(i64::from(b)),
            DecodedScalar::Enum(v) => Some(i64::from(v)),
            DecodedScalar::Str(_) | DecodedScalar::Bytes(_) | DecodedScalar::F64(_) => None,
        }
    }

    #[inline]
    fn into_f64(self) -> Option<f64> {
        match self {
            DecodedScalar::F64(v) => Some(v),
            DecodedScalar::I64(v) => Some(v as f64),
            DecodedScalar::U64(v) => Some(v as f64),
            DecodedScalar::Enum(v) => Some(f64::from(v)),
            DecodedScalar::Bool(_) | DecodedScalar::Str(_) | DecodedScalar::Bytes(_) => None,
        }
    }

    #[inline]
    fn into_json_value(self, kind: &FieldKind, pool: &CompiledDescriptorPool) -> Value {
        match self {
            DecodedScalar::Str(s) => Value::String(s.to_string()),
            DecodedScalar::Bytes(b) => {
                use base64::Engine;
                Value::String(base64::engine::general_purpose::STANDARD.encode(b))
            }
            DecodedScalar::Bool(b) => Value::Bool(b),
            DecodedScalar::I64(v) => Value::Number(Number::from(v)),
            DecodedScalar::U64(v) => Value::Number(Number::from(v)),
            DecodedScalar::F64(v) => Number::from_f64(v).map_or_else(
                || {
                    if v.is_nan() {
                        Value::String("NaN".to_string())
                    } else if v.is_sign_positive() {
                        Value::String("Infinity".to_string())
                    } else {
                        Value::String("-Infinity".to_string())
                    }
                },
                Value::Number,
            ),
            DecodedScalar::Enum(v) => {
                if let FieldKind::Enum(enum_name) = kind
                    && let Some(name) = pool.resolve_enum_name(enum_name, v)
                {
                    Value::String(name.to_string())
                } else {
                    Value::Number(Number::from(v))
                }
            }
        }
    }
}

#[inline]
fn default_scalar_for_kind(kind: &FieldKind) -> Option<DecodedScalar<'static>> {
    match kind {
        FieldKind::String => Some(DecodedScalar::Str("")),
        FieldKind::Bytes => Some(DecodedScalar::Bytes(&[])),
        FieldKind::Bool => Some(DecodedScalar::Bool(false)),
        FieldKind::Enum(_) => Some(DecodedScalar::Enum(0)),
        FieldKind::Double | FieldKind::Float => Some(DecodedScalar::F64(0.0)),
        FieldKind::Uint32 | FieldKind::Uint64 | FieldKind::Fixed32 | FieldKind::Fixed64 => {
            Some(DecodedScalar::U64(0))
        }
        FieldKind::Int32
        | FieldKind::Int64
        | FieldKind::Sint32
        | FieldKind::Sint64
        | FieldKind::Sfixed32
        | FieldKind::Sfixed64 => Some(DecodedScalar::I64(0)),
        FieldKind::Message(_) => None,
    }
}

#[inline]
fn map_entry_key_matches(
    seen_key: Option<Cow<'_, str>>,
    key_kind: &FieldKind,
    expected_key: &str,
    pool: &CompiledDescriptorPool,
) -> bool {
    let resolved_key = seen_key.unwrap_or_else(|| {
        default_scalar_for_kind(key_kind)
            .map(|d| d.into_cow_str(key_kind, pool))
            .unwrap_or(Cow::Borrowed(""))
    });
    resolved_key.as_ref() == expected_key
}

#[inline]
fn extract_map_entry_scalar<'a>(
    entry_buf: &'a [u8],
    key_kind: &FieldKind,
    expected_key: &str,
    value_kind: &FieldKind,
    pool: &CompiledDescriptorPool,
) -> Result<Option<DecodedScalar<'a>>, ()> {
    let mut pos = 0;
    let mut seen_key: Option<Cow<'a, str>> = None;
    let mut val_out: Option<DecodedScalar<'a>> = None;

    while pos < entry_buf.len() {
        let (fnum, wtype) = read_wire_tag(entry_buf, &mut pos).ok_or(())?;
        if fnum == 1 {
            let k = decode_wire_scalar(entry_buf, &mut pos, wtype, key_kind)
                .ok_or(())?
                .into_cow_str(key_kind, pool);
            seen_key = Some(k);
        } else if fnum == 2 {
            let v = decode_wire_scalar(entry_buf, &mut pos, wtype, value_kind).ok_or(())?;
            val_out = Some(v);
        } else if !skip_wire_value(entry_buf, &mut pos, wtype) {
            return Err(());
        }
    }

    if !map_entry_key_matches(seen_key, key_kind, expected_key, pool) {
        return Ok(None);
    }
    Ok(Some(
        val_out
            .or_else(|| default_scalar_for_kind(value_kind))
            .ok_or(())?,
    ))
}

#[inline]
fn extract_map_entry_subslice<'a>(
    entry_buf: &'a [u8],
    key_kind: &FieldKind,
    expected_key: &str,
    pool: &CompiledDescriptorPool,
) -> Result<Option<&'a [u8]>, ()> {
    let mut pos = 0;
    let mut seen_key: Option<Cow<'a, str>> = None;
    let mut val_slice: Option<&'a [u8]> = None;

    while pos < entry_buf.len() {
        let (fnum, wtype) = read_wire_tag(entry_buf, &mut pos).ok_or(())?;
        if fnum == 1 {
            let k = decode_wire_scalar(entry_buf, &mut pos, wtype, key_kind)
                .ok_or(())?
                .into_cow_str(key_kind, pool);
            seen_key = Some(k);
        } else if fnum == 2 {
            if wtype != WIRE_LEN {
                return Err(());
            }
            val_slice = Some(read_len_delimited(entry_buf, &mut pos).ok_or(())?);
        } else if !skip_wire_value(entry_buf, &mut pos, wtype) {
            return Err(());
        }
    }

    if !map_entry_key_matches(seen_key, key_kind, expected_key, pool) {
        return Ok(None);
    }
    Ok(Some(val_slice.unwrap_or(&[])))
}

#[derive(Clone, Copy)]
enum WalkHit<'a> {
    NotReached,
    ParentReached,
    Cleared,
    Value(DecodedScalar<'a>),
}

fn walk_sub_message_step<'a>(
    buf: &'a [u8],
    tag: u32,
    rest: &[PathStep],
    pool: &CompiledDescriptorPool,
) -> Result<WalkHit<'a>, ()> {
    let mut pos = 0;
    let mut acc = WalkHit::NotReached;

    while pos < buf.len() {
        let (field_num, wire_type) = read_wire_tag(buf, &mut pos).ok_or(())?;
        if field_num == tag {
            if wire_type != WIRE_LEN {
                return Err(());
            }
            let sub_slice = read_len_delimited(buf, &mut pos).ok_or(())?;
            match walk_scalar_steps(sub_slice, rest, pool)? {
                WalkHit::NotReached => {}
                WalkHit::ParentReached => {
                    if matches!(acc, WalkHit::NotReached) {
                        acc = WalkHit::ParentReached;
                    }
                }
                hit @ (WalkHit::Cleared | WalkHit::Value(_)) => acc = hit,
            }
        } else if !skip_wire_value(buf, &mut pos, wire_type) {
            return Err(());
        }
    }
    Ok(acc)
}

fn walk_map_sub_message_step<'a>(
    buf: &'a [u8],
    tag: u32,
    key_kind: &FieldKind,
    key_str: &str,
    rest: &[PathStep],
    pool: &CompiledDescriptorPool,
) -> Result<WalkHit<'a>, ()> {
    let mut pos = 0;
    let mut winning_slice: Option<&'a [u8]> = None;

    while pos < buf.len() {
        let (field_num, wire_type) = read_wire_tag(buf, &mut pos).ok_or(())?;
        if field_num == tag {
            if wire_type != WIRE_LEN {
                return Err(());
            }
            let entry_buf = read_len_delimited(buf, &mut pos).ok_or(())?;
            if let Some(val_slice) = extract_map_entry_subslice(entry_buf, key_kind, key_str, pool)?
            {
                winning_slice = Some(val_slice);
            }
        } else if !skip_wire_value(buf, &mut pos, wire_type) {
            return Err(());
        }
    }

    match winning_slice {
        Some(slice) => walk_scalar_steps(slice, rest, pool),
        None => Ok(WalkHit::NotReached),
    }
}

fn walk_scalar_steps<'a>(
    buf: &'a [u8],
    steps: &[PathStep],
    pool: &CompiledDescriptorPool,
) -> Result<WalkHit<'a>, ()> {
    if steps.len() > MAX_PROTO_RECURSION_DEPTH {
        return Err(());
    }
    let (first, rest) = steps.split_first().ok_or(())?;

    if !rest.is_empty() {
        return match first {
            PathStep::SubMessage { tag } => walk_sub_message_step(buf, *tag, rest, pool),
            PathStep::MapSubMessage {
                tag,
                key_kind,
                key_str,
            } => walk_map_sub_message_step(buf, *tag, key_kind, key_str, rest, pool),
            PathStep::Field { .. } | PathStep::MapLookup { .. } => Err(()),
        };
    }

    match first {
        PathStep::Field {
            tag,
            kind,
            oneof_siblings,
            ..
        } => {
            let mut pos = 0;
            let mut state = WalkHit::ParentReached;
            while pos < buf.len() {
                let (field_num, wire_type) = read_wire_tag(buf, &mut pos).ok_or(())?;
                if field_num == *tag {
                    let v = decode_wire_scalar(buf, &mut pos, wire_type, kind).ok_or(())?;
                    state = WalkHit::Value(v);
                } else {
                    if !skip_wire_value(buf, &mut pos, wire_type) {
                        return Err(());
                    }
                    if oneof_siblings.contains(&field_num) {
                        state = WalkHit::Cleared;
                    }
                }
            }
            Ok(state)
        }
        PathStep::MapLookup {
            tag,
            key_kind,
            key_str,
            value_kind,
        } => {
            let mut pos = 0;
            let mut state = WalkHit::NotReached;
            while pos < buf.len() {
                let (field_num, wire_type) = read_wire_tag(buf, &mut pos).ok_or(())?;
                if field_num == *tag {
                    if wire_type != WIRE_LEN {
                        return Err(());
                    }
                    let entry_buf = read_len_delimited(buf, &mut pos).ok_or(())?;
                    if let Some(v) =
                        extract_map_entry_scalar(entry_buf, key_kind, key_str, value_kind, pool)?
                    {
                        state = WalkHit::Value(v);
                    }
                } else if !skip_wire_value(buf, &mut pos, wire_type) {
                    return Err(());
                }
            }
            Ok(state)
        }
        PathStep::SubMessage { .. } | PathStep::MapSubMessage { .. } => Err(()),
    }
}

#[inline]
fn resolve_leaf_or_default<'a, 's>(
    raw: &'a [u8],
    steps: &'s [PathStep],
    pool: &CompiledDescriptorPool,
    allow_field_default: bool,
) -> Option<(DecodedScalar<'a>, &'s FieldKind)> {
    let hit = walk_scalar_steps(raw, steps, pool).ok()?;
    match (hit, steps.last()?) {
        (
            WalkHit::Value(v),
            PathStep::Field { kind, .. }
            | PathStep::MapLookup {
                value_kind: kind, ..
            },
        ) => Some((v, kind)),
        (
            WalkHit::ParentReached | WalkHit::Cleared,
            PathStep::Field {
                kind, has_presence, ..
            },
        ) if allow_field_default && !*has_presence => Some((default_scalar_for_kind(kind)?, kind)),
        _ => None,
    }
}

/// Extract a string (or stringified scalar/enum/map-value) using zero-allocation wire-tag skipping.
#[inline]
pub fn extract_str<'a>(
    raw: &'a [u8],
    steps: &[PathStep],
    pool: &CompiledDescriptorPool,
) -> Option<Cow<'a, str>> {
    let (scalar, kind) = resolve_leaf_or_default(raw, steps, pool, false)?;
    Some(scalar.into_cow_str(kind, pool))
}

/// Extract an integer (`i64`) field using zero-allocation wire-tag skipping.
#[inline]
pub fn extract_i64(raw: &[u8], steps: &[PathStep], pool: &CompiledDescriptorPool) -> Option<i64> {
    let (scalar, _kind) = resolve_leaf_or_default(raw, steps, pool, true)?;
    scalar.into_i64()
}

/// Extract a floating-point (`f64`) field using zero-allocation wire-tag skipping.
#[inline]
pub fn extract_f64(raw: &[u8], steps: &[PathStep], pool: &CompiledDescriptorPool) -> Option<f64> {
    resolve_leaf_or_default(raw, steps, pool, true).and_then(|pair| pair.0.into_f64())
}

/// Extract a boolean field using zero-allocation wire-tag skipping.
#[inline]
pub fn extract_bool(raw: &[u8], steps: &[PathStep], pool: &CompiledDescriptorPool) -> Option<bool> {
    let (DecodedScalar::Bool(b), _) = resolve_leaf_or_default(raw, steps, pool, true)? else {
        return None;
    };
    Some(b)
}
#[inline]
fn packed_element_wire_type(kind: &FieldKind) -> Option<u8> {
    match kind {
        FieldKind::Int32
        | FieldKind::Int64
        | FieldKind::Uint32
        | FieldKind::Uint64
        | FieldKind::Sint32
        | FieldKind::Sint64
        | FieldKind::Bool
        | FieldKind::Enum(_) => Some(WIRE_VARINT),
        FieldKind::Double | FieldKind::Fixed64 | FieldKind::Sfixed64 => Some(WIRE_I64),
        FieldKind::Float | FieldKind::Fixed32 | FieldKind::Sfixed32 => Some(WIRE_I32),
        FieldKind::String | FieldKind::Bytes | FieldKind::Message(_) => None,
    }
}

#[inline]
fn default_json_value(kind: &FieldKind, pool: &CompiledDescriptorPool) -> Value {
    match kind {
        FieldKind::String | FieldKind::Bytes => Value::String(String::new()),
        FieldKind::Bool => Value::Bool(false),
        FieldKind::Enum(enum_name) => {
            if let Some(name) = pool.resolve_enum_name(enum_name, 0) {
                Value::String(name.to_string())
            } else {
                Value::Number(Number::from(0))
            }
        }
        FieldKind::Float | FieldKind::Double => {
            Number::from_f64(0.0).map_or(Value::Null, Value::Number)
        }
        FieldKind::Message(_) => Value::Null,
        _ => Value::Number(Number::from(0)),
    }
}

fn clear_oneof_siblings_json(
    map: &mut Map<String, Value>,
    field: &CompiledField,
    msg: &CompiledMessageSchema,
) {
    for &sib_tag in field.oneof_siblings.iter() {
        if let Some(sib) = msg.fields_by_tag.get(&sib_tag) {
            map.swap_remove(&sib.name);
        }
    }
}

fn merge_message_json(
    dst: &mut Map<String, Value>,
    src: Map<String, Value>,
    msg: &CompiledMessageSchema,
    pool: &CompiledDescriptorPool,
) {
    for (k, v) in src {
        let Some(field) = msg.fields_by_name.get(&k) else {
            dst.insert(k, v);
            continue;
        };
        if !field.oneof_siblings.is_empty() {
            clear_oneof_siblings_json(dst, field, msg);
        }
        if field.map_entry.is_some() {
            match (dst.get_mut(&k), v) {
                (Some(Value::Object(dst_map)), Value::Object(src_map)) => {
                    dst_map.extend(src_map);
                }
                (_, v) => {
                    dst.insert(k, v);
                }
            }
            continue;
        }
        if field.is_repeated {
            match (dst.get_mut(&k), v) {
                (Some(Value::Array(dst_arr)), Value::Array(src_arr)) => {
                    dst_arr.extend(src_arr);
                }
                (_, v) => {
                    dst.insert(k, v);
                }
            }
            continue;
        }
        if let FieldKind::Message(sub_name) = &field.kind
            && let Some(sub_schema) = pool.resolve_message(sub_name)
        {
            match (dst.get_mut(&k), v) {
                (Some(Value::Object(dst_child)), Value::Object(src_child)) => {
                    merge_message_json(dst_child, src_child, sub_schema, pool);
                }
                (_, v) => {
                    dst.insert(k, v);
                }
            }
            continue;
        }
        dst.insert(k, v);
    }
}

#[inline]
fn take_budget(budget: &mut usize) -> Option<()> {
    if *budget == 0 {
        return None;
    }
    *budget -= 1;
    Some(())
}
struct TranscodeCtx<'a> {
    pool: &'a CompiledDescriptorPool,
    depth: usize,
    budget: &'a mut usize,
}

/// Dynamically transcode a binary Protobuf payload into a `serde_json::Value` using its descriptor.
pub fn transcode_to_json(
    buf: &[u8],
    msg: &CompiledMessageSchema,
    pool: &CompiledDescriptorPool,
) -> Option<Value> {
    let mut budget = MAX_JSON_VALUES;
    let mut ctx = TranscodeCtx {
        pool,
        depth: 0,
        budget: &mut budget,
    };
    transcode_to_json_inner(buf, msg, &mut ctx)
}

fn transcode_to_json_inner(
    buf: &[u8],
    msg: &CompiledMessageSchema,
    ctx: &mut TranscodeCtx<'_>,
) -> Option<Value> {
    if ctx.depth >= MAX_PROTO_RECURSION_DEPTH {
        return None;
    }
    take_budget(ctx.budget)?;
    let mut map = Map::with_capacity(msg.fields_by_tag.len().min(buf.len() / 2).min(16));
    let mut pos = 0;

    while pos < buf.len() {
        let (field_num, wire_type) = read_wire_tag(buf, &mut pos)?;

        let Some(field) = msg.fields_by_tag.get(&field_num) else {
            if !skip_wire_value(buf, &mut pos, wire_type) {
                return None;
            }
            continue;
        };

        if let Some((k_kind, v_kind)) = &field.map_entry {
            if wire_type != WIRE_LEN {
                return None;
            }
            let entry_buf = read_len_delimited(buf, &mut pos)?;
            let (k_str, v_val) = decode_map_entry_json(entry_buf, k_kind, v_kind, ctx)?;
            let obj = map
                .entry(field.name.clone())
                .or_insert_with(|| Value::Object(Map::new()));
            if let Value::Object(inner) = obj {
                inner.insert(k_str, v_val);
            }
            continue;
        }

        if field.is_repeated {
            let arr = map
                .entry(field.name.clone())
                .or_insert_with(|| Value::Array(Vec::new()));
            let Value::Array(vec) = arr else {
                return None;
            };
            if wire_type == WIRE_LEN
                && let Some(elem_wire_type) = packed_element_wire_type(&field.kind)
            {
                let packed_buf = read_len_delimited(buf, &mut pos)?;
                let mut ppos = 0;
                while ppos < packed_buf.len() {
                    let elem = decode_single_field_json(
                        packed_buf,
                        &mut ppos,
                        elem_wire_type,
                        &field.kind,
                        ctx,
                    )?;
                    vec.push(elem);
                }
            } else {
                let elem = decode_single_field_json(buf, &mut pos, wire_type, &field.kind, ctx)?;
                vec.push(elem);
            }
            continue;
        }

        if !field.oneof_siblings.is_empty() {
            clear_oneof_siblings_json(&mut map, field, msg);
        }
        let decoded_val = decode_single_field_json(buf, &mut pos, wire_type, &field.kind, ctx)?;
        if let FieldKind::Message(sub_name) = &field.kind
            && let Some(sub_schema) = ctx.pool.resolve_message(sub_name)
            && let Some(Value::Object(existing_obj)) = map.get_mut(&field.name)
            && let Value::Object(new_obj) = decoded_val
        {
            merge_message_json(existing_obj, new_obj, sub_schema, ctx.pool);
        } else {
            map.insert(field.name.clone(), decoded_val);
        }
    }

    Some(Value::Object(map))
}

fn decode_map_entry_json(
    entry_buf: &[u8],
    key_kind: &FieldKind,
    value_kind: &FieldKind,
    ctx: &mut TranscodeCtx<'_>,
) -> Option<(String, Value)> {
    if ctx.depth + 1 >= MAX_PROTO_RECURSION_DEPTH {
        return None;
    }
    take_budget(ctx.budget)?;
    let mut pos = 0;
    let mut key_str: Option<String> = None;
    let mut val_json: Option<Value> = None;
    let mut sub_ctx = TranscodeCtx {
        pool: ctx.pool,
        depth: ctx.depth + 1,
        budget: ctx.budget,
    };

    while pos < entry_buf.len() {
        let (fnum, wtype) = read_wire_tag(entry_buf, &mut pos)?;
        if fnum == 1 {
            let k = decode_wire_scalar(entry_buf, &mut pos, wtype, key_kind)?
                .into_cow_str(key_kind, sub_ctx.pool);
            key_str = Some(k.into_owned());
        } else if fnum == 2 {
            let decoded =
                decode_single_field_json(entry_buf, &mut pos, wtype, value_kind, &mut sub_ctx)?;
            if let FieldKind::Message(sub_name) = value_kind
                && let Some(sub_schema) = sub_ctx.pool.resolve_message(sub_name)
                && let Some(Value::Object(existing_obj)) = val_json.as_mut()
                && let Value::Object(new_obj) = decoded
            {
                merge_message_json(existing_obj, new_obj, sub_schema, sub_ctx.pool);
            } else {
                val_json = Some(decoded);
            }
        } else if !skip_wire_value(entry_buf, &mut pos, wtype) {
            return None;
        }
    }

    let final_key = key_str.unwrap_or_else(|| {
        default_scalar_for_kind(key_kind)
            .map(|d| d.into_cow_str(key_kind, sub_ctx.pool).into_owned())
            .unwrap_or_default()
    });
    let final_val = val_json.unwrap_or_else(|| default_json_value(value_kind, sub_ctx.pool));
    Some((final_key, final_val))
}

fn decode_single_field_json(
    buf: &[u8],
    pos: &mut usize,
    wire_type: u8,
    kind: &FieldKind,
    ctx: &mut TranscodeCtx<'_>,
) -> Option<Value> {
    if let (FieldKind::Message(msg_name), WIRE_LEN) = (kind, wire_type) {
        let sub_buf = read_len_delimited(buf, pos)?;
        let sub_schema = ctx.pool.resolve_message(msg_name)?;
        let mut sub_ctx = TranscodeCtx {
            pool: ctx.pool,
            depth: ctx.depth + 1,
            budget: ctx.budget,
        };
        return transcode_to_json_inner(sub_buf, sub_schema, &mut sub_ctx);
    }
    take_budget(ctx.budget)?;
    Some(decode_wire_scalar(buf, pos, wire_type, kind)?.into_json_value(kind, ctx.pool))
}
