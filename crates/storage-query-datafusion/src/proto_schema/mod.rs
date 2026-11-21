// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Dynamic Protobuf Schema Registry backed by Restate `ServiceMetadata` discovery.
//!
//! Services propagate compiled `google.protobuf.FileDescriptorSet` bytes (base64-encoded)
//! via service metadata key `proto.descriptor_set` (or `proto.fds`) and map state keys
//! to Protobuf message names via `proto.state.<state_key>` (or `proto.state.*`).

pub mod table;
#[cfg(test)]
mod tests;
pub mod udf;
pub mod wire;

use std::borrow::Cow;
use std::collections::{HashMap, HashSet};
use std::fmt::Debug;
use std::sync::{Arc, OnceLock};

use base64::Engine;
use base64::alphabet::STANDARD;
use base64::engine::{DecodePaddingMode, GeneralPurpose, GeneralPurposeConfig};
use moka::sync::Cache;
use parking_lot::RwLock;
use prost::Message;
use prost_types::field_descriptor_proto::{Label, Type};
use prost_types::{DescriptorProto, EnumDescriptorProto, FieldDescriptorProto, FileDescriptorSet};

use restate_types::live::{Live, Pinned};
use restate_types::schema::service::{ServiceMetadata, ServiceMetadataResolver};

const PROTO_FDS_BASE64_ENGINE: GeneralPurpose = GeneralPurpose::new(
    &STANDARD,
    GeneralPurposeConfig::new().with_decode_padding_mode(DecodePaddingMode::Indifferent),
);
pub const PROTO_DESCRIPTOR_SET_KEY: &str = "proto.descriptor_set";
pub const PROTO_FDS_SHORT_KEY: &str = "proto.fds";
pub const PROTO_STATE_KEY_PREFIX: &str = "proto.state.";
pub const PROTO_STATE_WILDCARD_KEY: &str = "proto.state.*";

pub(crate) const MAX_PROTO_RECURSION_DEPTH: usize = 64;
const MAX_CACHED_PATHS_PER_MESSAGE: usize = 256;
const MAX_CACHED_FIELD_PATH_BYTES: usize = 512;
const MAX_FDS_BASE64_LEN: usize = 4 * 1024 * 1024;
const MAX_DESCRIPTOR_NESTING: usize = 32;
const MAX_POOL_MESSAGES: usize = 4_096;
const MAX_POOL_ITEMS: usize = 65_536;
const MAX_IDENTIFIER_LEN: usize = 512;
const MAX_CACHED_SERVICES: usize = 1_024;
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum FieldKind {
    Double,
    Float,
    Int64,
    Uint64,
    Int32,
    Fixed64,
    Fixed32,
    Bool,
    String,
    Bytes,
    Uint32,
    Sfixed32,
    Sfixed64,
    Sint32,
    Sint64,
    Enum(String),
    Message(String),
}

impl FieldKind {
    pub fn type_name(&self) -> &str {
        match self {
            FieldKind::Double => "double",
            FieldKind::Float => "float",
            FieldKind::Int64 => "int64",
            FieldKind::Uint64 => "uint64",
            FieldKind::Int32 => "int32",
            FieldKind::Fixed64 => "fixed64",
            FieldKind::Fixed32 => "fixed32",
            FieldKind::Bool => "bool",
            FieldKind::String => "string",
            FieldKind::Bytes => "bytes",
            FieldKind::Uint32 => "uint32",
            FieldKind::Sfixed32 => "sfixed32",
            FieldKind::Sfixed64 => "sfixed64",
            FieldKind::Sint32 => "sint32",
            FieldKind::Sint64 => "sint64",
            FieldKind::Enum(name) => name.as_str(),
            FieldKind::Message(name) => name.as_str(),
        }
    }
}

#[derive(Clone, Debug)]
pub struct CompiledField {
    pub name: String,
    pub number: u32,
    pub kind: FieldKind,
    pub is_repeated: bool,
    pub has_presence: bool,
    pub oneof_siblings: Arc<[u32]>,
    pub map_entry: Option<(FieldKind, FieldKind)>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CompiledMapKey {
    Str(String),
    I64(i64),
    U64(u64),
    Bool(bool),
}

impl CompiledMapKey {
    pub fn parse(key_kind: &FieldKind, raw: &str) -> Option<Self> {
        match key_kind {
            FieldKind::String => Some(Self::Str(raw.to_string())),
            FieldKind::Int32 | FieldKind::Sint32 | FieldKind::Sfixed32 => {
                raw.parse::<i32>().ok().map(|v| Self::I64(i64::from(v)))
            }
            FieldKind::Int64 | FieldKind::Sint64 | FieldKind::Sfixed64 => {
                raw.parse::<i64>().ok().map(Self::I64)
            }
            FieldKind::Uint32 | FieldKind::Fixed32 => {
                raw.parse::<u32>().ok().map(|v| Self::U64(u64::from(v)))
            }
            FieldKind::Uint64 | FieldKind::Fixed64 => raw.parse::<u64>().ok().map(Self::U64),
            FieldKind::Bool => match raw {
                "true" => Some(Self::Bool(true)),
                "false" => Some(Self::Bool(false)),
                _ => None,
            },
            _ => None,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PathStep {
    SubMessage {
        tag: u32,
        oneof_siblings: Arc<[u32]>,
    },
    MapSubMessage {
        tag: u32,
        key_kind: FieldKind,
        key: CompiledMapKey,
    },
    Field {
        tag: u32,
        kind: FieldKind,
        has_presence: bool,
        oneof_siblings: Arc<[u32]>,
    },
    MapLookup {
        tag: u32,
        key_kind: FieldKind,
        key: CompiledMapKey,
        value_kind: FieldKind,
    },
}

#[derive(Debug)]
pub struct CompiledMessageSchema {
    pub full_name: String,
    pub fields_by_name: HashMap<String, CompiledField>,
    pub fields_by_tag: HashMap<u32, CompiledField>,
    path_cache: Cache<String, Arc<Vec<PathStep>>>,
    summary_json: OnceLock<String>,
}

fn parse_field_path_segments(field_path: &str) -> Option<Vec<Cow<'_, str>>> {
    if field_path.is_empty() {
        return None;
    }
    let mut segments = Vec::with_capacity(4);
    let mut chars = field_path.char_indices();
    let mut seg_start = 0usize;
    let mut owned: Option<String> = None;

    while let Some((idx, ch)) = chars.next() {
        match ch {
            '.' => {
                let seg = match owned.take() {
                    Some(s) => Cow::Owned(s),
                    None => Cow::Borrowed(&field_path[seg_start..idx]),
                };
                segments.push(seg);
                if segments.len() > MAX_PROTO_RECURSION_DEPTH * 2 + 1 {
                    return None;
                }
                seg_start = idx + 1;
            }
            '\\' => {
                let buf = owned.get_or_insert_with(|| field_path[seg_start..idx].to_string());
                let (_, escaped) = chars.next()?;
                match escaped {
                    '.' | '\\' => buf.push(escaped),
                    _ => return None,
                }
            }
            _ => {
                if let Some(buf) = owned.as_mut() {
                    buf.push(ch);
                }
            }
        }
    }

    let last = match owned {
        Some(s) => Cow::Owned(s),
        None => Cow::Borrowed(&field_path[seg_start..]),
    };
    segments.push(last);
    if segments.len() > MAX_PROTO_RECURSION_DEPTH * 2 + 1 {
        return None;
    }
    Some(segments)
}

impl CompiledMessageSchema {
    pub fn resolve_path(
        &self,
        field_path: &str,
        pool: &CompiledDescriptorPool,
    ) -> Option<Arc<Vec<PathStep>>> {
        if let Some(cached) = self.path_cache.get(field_path) {
            return Some(cached);
        }
        let compiled = Arc::new(self.compile_path_uncached(field_path, pool)?);
        if field_path.len() <= MAX_CACHED_FIELD_PATH_BYTES {
            self.path_cache
                .insert(field_path.to_string(), compiled.clone());
        }
        Some(compiled)
    }

    #[cfg(test)]
    pub(crate) fn cached_path_count(&self) -> usize {
        self.path_cache.run_pending_tasks();
        self.path_cache.entry_count() as usize
    }

    fn compile_path_uncached(
        &self,
        field_path: &str,
        pool: &CompiledDescriptorPool,
    ) -> Option<Vec<PathStep>> {
        let segments = parse_field_path_segments(field_path)?;
        let mut iter = segments.into_iter().peekable();
        iter.peek()?;
        let mut steps = Vec::with_capacity(4);
        let mut current_msg = self;

        while let Some(seg) = iter.next() {
            if seg.is_empty() || steps.len() >= MAX_PROTO_RECURSION_DEPTH {
                return None;
            }
            let field = current_msg.fields_by_name.get(seg.as_ref())?;
            if field.is_repeated && field.map_entry.is_none() {
                return None;
            }
            if iter.peek().is_none() {
                return push_leaf_field_step(field, steps);
            }
            if let Some((k_kind, v_kind)) = &field.map_entry {
                let map_key = iter.next()?;
                current_msg = match compile_map_path_step(
                    field.number,
                    k_kind,
                    v_kind,
                    map_key.as_ref(),
                    iter.peek().is_none(),
                    pool,
                    &mut steps,
                )? {
                    Some(next_msg) => next_msg,
                    None => return Some(steps),
                };
                continue;
            }
            let FieldKind::Message(sub_name) = &field.kind else {
                return None;
            };
            steps.push(PathStep::SubMessage {
                tag: field.number,
                oneof_siblings: field.oneof_siblings.clone(),
            });
            current_msg = pool.resolve_message(sub_name)?;
        }

        Some(steps)
    }

    pub fn fields_summary_json(&self) -> &str {
        self.summary_json.get_or_init(|| {
            let mut ordered: Vec<&CompiledField> = self.fields_by_tag.values().collect();
            ordered.sort_by_key(|f| f.number);
            let items: Vec<serde_json::Value> = ordered
                .into_iter()
                .map(|f| {
                    serde_json::json!({
                        "name": f.name,
                        "number": f.number,
                        "type": f.kind.type_name(),
                        "repeated": f.is_repeated,
                        "map": f.map_entry.is_some(),
                    })
                })
                .collect();
            serde_json::to_string(&items).unwrap_or_else(|_| "[]".to_string())
        })
    }
}

#[derive(Debug, Default)]
pub struct CompiledDescriptorPool {
    messages: HashMap<String, Arc<CompiledMessageSchema>>,
    short_names: HashMap<String, Option<String>>,
    enums: HashMap<String, HashMap<i32, String>>,
}

fn push_leaf_field_step(field: &CompiledField, mut steps: Vec<PathStep>) -> Option<Vec<PathStep>> {
    if matches!(field.kind, FieldKind::Message(_)) {
        return None;
    }
    steps.push(PathStep::Field {
        tag: field.number,
        kind: field.kind.clone(),
        has_presence: field.has_presence,
        oneof_siblings: field.oneof_siblings.clone(),
    });
    Some(steps)
}

#[allow(clippy::too_many_arguments)]
fn compile_map_path_step<'p>(
    tag: u32,
    k_kind: &FieldKind,
    v_kind: &FieldKind,
    map_key: &str,
    is_terminal_key: bool,
    pool: &'p CompiledDescriptorPool,
    steps: &mut Vec<PathStep>,
) -> Option<Option<&'p Arc<CompiledMessageSchema>>> {
    let key = CompiledMapKey::parse(k_kind, map_key)?;
    if is_terminal_key {
        if matches!(v_kind, FieldKind::Message(_)) {
            return None;
        }
        steps.push(PathStep::MapLookup {
            tag,
            key_kind: k_kind.clone(),
            key,
            value_kind: v_kind.clone(),
        });
        return Some(None);
    }
    let FieldKind::Message(sub_name) = v_kind else {
        return None;
    };
    steps.push(PathStep::MapSubMessage {
        tag,
        key_kind: k_kind.clone(),
        key,
    });
    Some(Some(pool.resolve_message(sub_name)?))
}
#[derive(Default)]
struct PoolBuilder {
    known_messages: HashSet<String>,
    raw_map_entries: HashMap<String, (FieldKind, FieldKind)>,
    enums: HashMap<String, HashMap<i32, String>>,
    messages: HashMap<String, Arc<CompiledMessageSchema>>,
    short_names: HashMap<String, Option<String>>,
    non_map_message_count: usize,
    remaining_items: usize,
}

impl PoolBuilder {
    fn take(&mut self, n: usize) -> bool {
        if self.remaining_items < n {
            return false;
        }
        self.remaining_items -= n;
        true
    }
}

impl CompiledDescriptorPool {
    pub fn from_file_descriptor_set(fds: &FileDescriptorSet) -> Option<Self> {
        let mut builder = PoolBuilder {
            remaining_items: MAX_POOL_ITEMS,
            ..Default::default()
        };
        for file in &fds.file {
            let pkg = file.package.as_deref().unwrap_or("");
            if pkg.len() > MAX_IDENTIFIER_LEN {
                tracing::warn!("Rejecting FileDescriptorSet: package name exceeds max length");
                return None;
            }
            for en in &file.enum_type {
                collect_enum(&mut builder, pkg, en)?;
            }
            for msg in &file.message_type {
                collect_message_names(&mut builder, pkg, msg, 0)?;
            }
        }

        for file in &fds.file {
            let pkg = file.package.as_deref().unwrap_or("");
            for msg in &file.message_type {
                collect_map_entries_recursive(&mut builder, pkg, msg)?;
            }
        }

        for file in &fds.file {
            let pkg = file.package.as_deref().unwrap_or("");
            let is_proto3 = file.syntax.as_deref() == Some("proto3");
            for msg in &file.message_type {
                compile_message_recursive(&mut builder, pkg, msg, is_proto3)?;
            }
        }

        Some(Self {
            messages: builder.messages,
            short_names: builder.short_names,
            enums: builder.enums,
        })
    }

    pub fn resolve_message(&self, name: &str) -> Option<&Arc<CompiledMessageSchema>> {
        let trimmed = name.trim_start_matches('.');
        if let Some(m) = self.messages.get(trimmed) {
            return Some(m);
        }
        if let Some(Some(full)) = self.short_names.get(trimmed) {
            return self.messages.get(full);
        }
        None
    }

    pub fn resolve_enum_name(&self, enum_type: &str, number: i32) -> Option<&str> {
        let trimmed = enum_type.trim_start_matches('.');
        self.enums.get(trimmed)?.get(&number).map(String::as_str)
    }
}

fn qualify_name(prefix: &str, name: &str) -> String {
    if prefix.is_empty() {
        name.to_string()
    } else {
        format!("{prefix}.{name}")
    }
}

fn collect_enum(builder: &mut PoolBuilder, prefix: &str, en: &EnumDescriptorProto) -> Option<()> {
    let Some(name) = en.name.as_deref() else {
        return Some(());
    };
    if name.len() > MAX_IDENTIFIER_LEN || !builder.take(1) {
        tracing::warn!("Rejecting FileDescriptorSet: enum exceeded identifier or item limit");
        return None;
    }
    let full = qualify_name(prefix, name);
    if full.len() > MAX_IDENTIFIER_LEN {
        tracing::warn!("Rejecting FileDescriptorSet: full enum name exceeds max length");
        return None;
    }
    let mut vals = HashMap::new();
    for v in &en.value {
        let (Some(vname), Some(vnum)) = (v.name.as_deref(), v.number) else {
            continue;
        };
        if vname.len() > MAX_IDENTIFIER_LEN || !builder.take(1) {
            tracing::warn!(
                "Rejecting FileDescriptorSet: enum value exceeded identifier or item limit"
            );
            return None;
        }
        vals.insert(vnum, vname.to_string());
    }
    builder.enums.insert(full, vals);
    Some(())
}

fn collect_message_names(
    builder: &mut PoolBuilder,
    prefix: &str,
    msg: &DescriptorProto,
    depth: usize,
) -> Option<()> {
    if depth >= MAX_DESCRIPTOR_NESTING {
        tracing::warn!("Rejecting FileDescriptorSet: descriptor nesting exceeds max depth");
        return None;
    }
    let Some(name) = msg.name.as_deref() else {
        return Some(());
    };
    if name.len() > MAX_IDENTIFIER_LEN || !builder.take(1) {
        tracing::warn!("Rejecting FileDescriptorSet: message exceeded identifier or item limit");
        return None;
    }
    let full = qualify_name(prefix, name);
    if full.len() > MAX_IDENTIFIER_LEN {
        tracing::warn!("Rejecting FileDescriptorSet: full message name exceeds max length");
        return None;
    }
    let is_map = msg
        .options
        .as_ref()
        .and_then(|o| o.map_entry)
        .unwrap_or(false);
    if !is_map {
        builder.non_map_message_count += 1;
        if builder.non_map_message_count > MAX_POOL_MESSAGES {
            tracing::warn!("Rejecting FileDescriptorSet: message count exceeds MAX_POOL_MESSAGES");
            return None;
        }
    }
    builder.known_messages.insert(full.clone());
    for en in &msg.enum_type {
        collect_enum(builder, &full, en)?;
    }
    for nested in &msg.nested_type {
        collect_message_names(builder, &full, nested, depth + 1)?;
    }
    Some(())
}

fn collect_map_entries_recursive(
    builder: &mut PoolBuilder,
    prefix: &str,
    msg: &DescriptorProto,
) -> Option<()> {
    let Some(name) = msg.name.as_deref() else {
        return Some(());
    };
    let full = qualify_name(prefix, name);
    let is_map = msg
        .options
        .as_ref()
        .and_then(|o| o.map_entry)
        .unwrap_or(false);
    if is_map {
        let mut k_kind = Some(FieldKind::String);
        let mut v_kind = Some(FieldKind::String);
        for f in &msg.field {
            if f.number == Some(1) {
                k_kind = convert_field_kind(f, &full, &builder.known_messages, &builder.enums);
            } else if f.number == Some(2) {
                v_kind = convert_field_kind(f, &full, &builder.known_messages, &builder.enums);
            }
        }
        if let (Some(k), Some(v)) = (k_kind, v_kind) {
            builder.raw_map_entries.insert(full.clone(), (k, v));
        }
    }
    for nested in &msg.nested_type {
        collect_map_entries_recursive(builder, &full, nested)?;
    }
    Some(())
}

fn build_oneof_sibling_map(msg: &DescriptorProto) -> HashMap<i32, Vec<u32>> {
    let mut groups: HashMap<i32, Vec<u32>> = HashMap::new();
    for f in &msg.field {
        if f.proto3_optional.unwrap_or(false) {
            continue;
        }
        if let (Some(idx), Some(fnum)) = (f.oneof_index, f.number)
            && fnum > 0
        {
            groups.entry(idx).or_default().push(fnum as u32);
        }
    }
    groups
}

fn compile_single_field(
    builder: &mut PoolBuilder,
    f: &FieldDescriptorProto,
    enclosing_full: &str,
    is_proto3: bool,
    oneof_groups: &HashMap<i32, Vec<u32>>,
) -> Option<Option<CompiledField>> {
    let (Some(fname), Some(fnum)) = (f.name.as_deref(), f.number) else {
        return Some(None);
    };
    if fnum <= 0 {
        return Some(None);
    }
    if fname.len() > MAX_IDENTIFIER_LEN || !builder.take(1) {
        tracing::warn!("Rejecting FileDescriptorSet: field exceeded identifier or item limit");
        return None;
    }
    let Some(kind) = convert_field_kind(f, enclosing_full, &builder.known_messages, &builder.enums)
    else {
        // Skip unsupported legacy Proto2 group fields rather than mis-mapping them.
        return Some(None);
    };
    let tag = fnum as u32;
    let is_proto3_opt = f.proto3_optional.unwrap_or(false);
    let has_presence = !is_proto3 || is_proto3_opt || f.oneof_index.is_some();
    let oneof_siblings = if !is_proto3_opt
        && let Some(idx) = f.oneof_index
        && let Some(group) = oneof_groups.get(&idx)
        && group.len() > 1
    {
        Arc::from(
            group
                .iter()
                .copied()
                .filter(|&other| other != tag)
                .collect::<Vec<u32>>(),
        )
    } else {
        Arc::from([])
    };
    let map_entry = match &kind {
        FieldKind::Message(tname) => builder
            .raw_map_entries
            .get(tname.trim_start_matches('.'))
            .cloned(),
        _ => None,
    };

    Some(Some(CompiledField {
        name: fname.to_string(),
        number: tag,
        kind,
        is_repeated: f.label == Some(Label::Repeated as i32),
        has_presence,
        oneof_siblings,
        map_entry,
    }))
}

fn compile_message_recursive(
    builder: &mut PoolBuilder,
    prefix: &str,
    msg: &DescriptorProto,
    is_proto3: bool,
) -> Option<()> {
    let Some(name) = msg.name.as_deref() else {
        return Some(());
    };
    let full = qualify_name(prefix, name);
    let is_map = msg
        .options
        .as_ref()
        .and_then(|o| o.map_entry)
        .unwrap_or(false);
    if !is_map {
        let oneof_groups = build_oneof_sibling_map(msg);
        let mut fields_by_name = HashMap::new();
        let mut fields_by_tag = HashMap::new();
        for f in &msg.field {
            if let Some(cf) = compile_single_field(builder, f, &full, is_proto3, &oneof_groups)? {
                fields_by_name.insert(cf.name.clone(), cf.clone());
                fields_by_tag.insert(cf.number, cf);
            }
        }
        let schema = Arc::new(CompiledMessageSchema {
            full_name: full.clone(),
            fields_by_name,
            fields_by_tag,
            path_cache: Cache::builder()
                .max_capacity(MAX_CACHED_PATHS_PER_MESSAGE as u64)
                .build(),
            summary_json: OnceLock::new(),
        });
        builder
            .short_names
            .entry(name.to_string())
            .and_modify(|existing| *existing = None)
            .or_insert_with(|| Some(full.clone()));
        builder.messages.insert(full.clone(), schema);
    }
    for nested in &msg.nested_type {
        compile_message_recursive(builder, &full, nested, is_proto3)?;
    }
    Some(())
}

fn resolve_descriptor_type_name<V>(
    raw_type_name: Option<&str>,
    enclosing_scope: &str,
    known_names: &HashMap<String, V>,
) -> String {
    let raw = raw_type_name.unwrap_or("");
    if let Some(stripped) = raw.strip_prefix('.') {
        return stripped.to_string();
    }
    if raw.is_empty() {
        return String::new();
    }
    let mut scope = enclosing_scope;
    loop {
        let candidate = qualify_name(scope, raw);
        if known_names.contains_key(&candidate) {
            return candidate;
        }
        if scope.is_empty() {
            break;
        }
        scope = match scope.rfind('.') {
            Some(dot) => &scope[..dot],
            None => "",
        };
    }
    raw.to_string()
}

fn resolve_descriptor_message_name(
    raw_type_name: Option<&str>,
    enclosing_scope: &str,
    known_messages: &HashSet<String>,
) -> String {
    let raw = raw_type_name.unwrap_or("");
    if let Some(stripped) = raw.strip_prefix('.') {
        return stripped.to_string();
    }
    if raw.is_empty() {
        return String::new();
    }
    let mut scope = enclosing_scope;
    loop {
        let candidate = qualify_name(scope, raw);
        if known_messages.contains(&candidate) {
            return candidate;
        }
        if scope.is_empty() {
            break;
        }
        scope = match scope.rfind('.') {
            Some(dot) => &scope[..dot],
            None => "",
        };
    }
    raw.to_string()
}

fn convert_field_kind(
    f: &FieldDescriptorProto,
    enclosing_scope: &str,
    known_messages: &HashSet<String>,
    known_enums: &HashMap<String, HashMap<i32, String>>,
) -> Option<FieldKind> {
    let raw_type = f.r#type.and_then(|t| Type::try_from(t).ok());
    match raw_type {
        Some(Type::Double) => Some(FieldKind::Double),
        Some(Type::Float) => Some(FieldKind::Float),
        Some(Type::Int64) => Some(FieldKind::Int64),
        Some(Type::Uint64) => Some(FieldKind::Uint64),
        Some(Type::Int32) => Some(FieldKind::Int32),
        Some(Type::Fixed64) => Some(FieldKind::Fixed64),
        Some(Type::Fixed32) => Some(FieldKind::Fixed32),
        Some(Type::Bool) => Some(FieldKind::Bool),
        Some(Type::String) => Some(FieldKind::String),
        Some(Type::Bytes) => Some(FieldKind::Bytes),
        Some(Type::Uint32) => Some(FieldKind::Uint32),
        Some(Type::Sfixed32) => Some(FieldKind::Sfixed32),
        Some(Type::Sfixed64) => Some(FieldKind::Sfixed64),
        Some(Type::Sint32) => Some(FieldKind::Sint32),
        Some(Type::Sint64) => Some(FieldKind::Sint64),
        Some(Type::Enum) => Some(FieldKind::Enum(resolve_descriptor_type_name(
            f.type_name.as_deref(),
            enclosing_scope,
            known_enums,
        ))),
        Some(Type::Message) => Some(FieldKind::Message(resolve_descriptor_message_name(
            f.type_name.as_deref(),
            enclosing_scope,
            known_messages,
        ))),
        Some(Type::Group) => None,
        None => Some(FieldKind::Bytes),
    }
}

#[derive(Debug)]
pub struct ServiceProtoSchemas {
    pub service_name: String,
    pub revision: u32,
    pub pool: Arc<CompiledDescriptorPool>,
    pub state_key_messages: HashMap<String, Arc<CompiledMessageSchema>>,
    pub wildcard_message: Option<Arc<CompiledMessageSchema>>,
}

fn has_proto_state_mappings(svc: &ServiceMetadata) -> bool {
    (svc.metadata.contains_key(PROTO_DESCRIPTOR_SET_KEY)
        || svc.metadata.contains_key(PROTO_FDS_SHORT_KEY))
        && svc.metadata.iter().any(|(k, v)| {
            !v.trim().is_empty()
                && (k == PROTO_STATE_WILDCARD_KEY
                    || k.strip_prefix(PROTO_STATE_KEY_PREFIX)
                        .is_some_and(|state_key| !state_key.is_empty()))
        })
}

impl ServiceProtoSchemas {
    pub fn from_service_metadata(svc: &ServiceMetadata) -> Option<Self> {
        if !svc.ty.has_state() || !has_proto_state_mappings(svc) {
            return None;
        }
        let b64 = svc
            .metadata
            .get(PROTO_DESCRIPTOR_SET_KEY)
            .or_else(|| svc.metadata.get(PROTO_FDS_SHORT_KEY))?
            .trim();
        if b64.len() > MAX_FDS_BASE64_LEN {
            tracing::warn!(
                service = %svc.name,
                revision = svc.revision,
                len = b64.len(),
                "Rejecting proto.descriptor_set exceeding MAX_FDS_BASE64_LEN"
            );
            return None;
        }
        let raw_bytes = if b64.bytes().any(|b| b.is_ascii_whitespace()) {
            let cleaned: String = b64.chars().filter(|c| !c.is_ascii_whitespace()).collect();
            PROTO_FDS_BASE64_ENGINE.decode(&cleaned)
        } else {
            PROTO_FDS_BASE64_ENGINE.decode(b64)
        };
        let Ok(raw_bytes) = raw_bytes else {
            tracing::warn!(
                service = %svc.name,
                revision = svc.revision,
                "Failed to decode base64 proto.descriptor_set"
            );
            return None;
        };
        if !wire::validate_descriptor_set_wire_nesting(&raw_bytes, MAX_DESCRIPTOR_NESTING) {
            tracing::warn!(
                service = %svc.name,
                revision = svc.revision,
                "Rejecting proto.descriptor_set: failed wire nesting or tag validation"
            );
            return None;
        }
        let Ok(fds) = FileDescriptorSet::decode(raw_bytes.as_slice()) else {
            tracing::warn!(
                service = %svc.name,
                revision = svc.revision,
                "Failed to decode FileDescriptorSet protobuf"
            );
            return None;
        };
        let Some(pool) = CompiledDescriptorPool::from_file_descriptor_set(&fds) else {
            tracing::warn!(
                service = %svc.name,
                revision = svc.revision,
                "Failed to compile FileDescriptorSet within descriptor pool limits"
            );
            return None;
        };
        let pool = Arc::new(pool);

        let mut state_key_messages = HashMap::new();
        let mut wildcard_message = None;

        for (meta_k, meta_v) in &svc.metadata {
            let target_name = meta_v.trim();
            if meta_k == PROTO_STATE_WILDCARD_KEY {
                if target_name.is_empty() {
                    continue;
                }
                if let Some(m) = pool.resolve_message(target_name) {
                    wildcard_message = Some(m.clone());
                } else {
                    tracing::warn!(
                        service = %svc.name,
                        revision = svc.revision,
                        message_name = target_name,
                        "Protobuf message for proto.state.* not found in descriptor pool"
                    );
                }
            } else if let Some(state_key) = meta_k.strip_prefix(PROTO_STATE_KEY_PREFIX)
                && !state_key.is_empty()
                && !target_name.is_empty()
            {
                if let Some(m) = pool.resolve_message(target_name) {
                    state_key_messages.insert(state_key.to_string(), m.clone());
                } else {
                    tracing::warn!(
                        service = %svc.name,
                        revision = svc.revision,
                        state_key,
                        message_name = target_name,
                        "Protobuf message for proto.state.<key> not found in descriptor pool"
                    );
                }
            }
        }

        Some(Self {
            service_name: svc.name.clone(),
            revision: svc.revision,
            pool,
            state_key_messages,
            wildcard_message,
        })
    }

    pub fn resolve_state_key(&self, state_key: &str) -> Option<&Arc<CompiledMessageSchema>> {
        self.state_key_messages
            .get(state_key)
            .or(self.wildcard_message.as_ref())
    }
}

#[derive(Clone, Debug)]
struct CachedServiceEntry {
    verified_gen: Option<u64>,
    revision: Option<u32>,
    metadata_fingerprint: Option<u64>,
    compiled: Option<Arc<ServiceProtoSchemas>>,
}

fn proto_metadata_fingerprint(svc: &ServiceMetadata) -> u64 {
    use std::hash::{DefaultHasher, Hash, Hasher};
    let mut acc = u64::from(svc.revision);
    for (k, v) in &svc.metadata {
        if k.starts_with("proto.") {
            let mut hasher = DefaultHasher::new();
            k.hash(&mut hasher);
            v.hash(&mut hasher);
            acc ^= hasher.finish().rotate_left(5);
        }
    }
    acc
}

/// Live-updating cache of compiled Protobuf schemas resolved from Restate's `ServiceMetadataResolver`.
#[derive(Clone, derive_more::Debug)]
#[debug("DynamicProtoSchemaRegistry")]
pub struct DynamicProtoSchemaRegistry<SMR> {
    resolver: Live<SMR>,
    cache: Cache<String, CachedServiceEntry>,
    snapshot_state: Arc<RwLock<(u64, Option<Arc<SMR>>)>>,
}

impl<SMR: ServiceMetadataResolver + Send + Sync + 'static> DynamicProtoSchemaRegistry<SMR> {
    pub fn new(resolver: Live<SMR>) -> Self {
        let cache = Cache::builder()
            .max_capacity(MAX_CACHED_SERVICES as u64)
            .build();
        Self {
            resolver,
            cache,
            snapshot_state: Arc::new(RwLock::new((0, None))),
        }
    }

    fn current_snapshot_gen(&self, pinned: &mut Pinned<SMR>) -> u64 {
        loop {
            {
                let guard = self.snapshot_state.read();
                if let (cur_gen, Some(prev)) = &*guard
                    && std::ptr::eq(Arc::as_ptr(prev), &**pinned)
                {
                    return *cur_gen;
                }
            }
            let snapshot_arc = self.resolver.snapshot();
            if !std::ptr::eq(Arc::as_ptr(&snapshot_arc), &**pinned) {
                *pinned = self.resolver.pinned();
                continue;
            }
            let mut guard = self.snapshot_state.write();
            if let Some(prev) = &guard.1
                && std::ptr::eq(Arc::as_ptr(prev), Arc::as_ptr(&snapshot_arc))
            {
                return guard.0;
            }
            guard.0 += 1;
            guard.1 = Some(snapshot_arc);
            return guard.0;
        }
    }

    fn resolve_or_compile(
        &self,
        svc: &ServiceMetadata,
        snapshot_gen: Option<u64>,
    ) -> Option<Arc<ServiceProtoSchemas>> {
        if !svc.ty.has_state() || !has_proto_state_mappings(svc) {
            if let Some(cur_gen) = snapshot_gen {
                self.cache.insert(
                    svc.name.clone(),
                    CachedServiceEntry {
                        verified_gen: Some(cur_gen),
                        revision: Some(svc.revision),
                        metadata_fingerprint: None,
                        compiled: None,
                    },
                );
            } else {
                self.cache.invalidate(&svc.name);
            }
            return None;
        }
        let fp = proto_metadata_fingerprint(svc);
        if let Some(entry) = self.cache.get(&svc.name)
            && entry.revision == Some(svc.revision)
            && entry.metadata_fingerprint == Some(fp)
        {
            if snapshot_gen.is_some() && entry.verified_gen != snapshot_gen {
                self.cache.insert(
                    svc.name.clone(),
                    CachedServiceEntry {
                        verified_gen: snapshot_gen,
                        revision: entry.revision,
                        metadata_fingerprint: entry.metadata_fingerprint,
                        compiled: entry.compiled.clone(),
                    },
                );
            }
            return entry.compiled;
        }
        let compiled = ServiceProtoSchemas::from_service_metadata(svc).map(Arc::new);
        self.cache.insert(
            svc.name.clone(),
            CachedServiceEntry {
                verified_gen: snapshot_gen,
                revision: Some(svc.revision),
                metadata_fingerprint: Some(fp),
                compiled: compiled.clone(),
            },
        );
        compiled
    }

    pub fn resolve_service_schemas(&self, service_name: &str) -> Option<Arc<ServiceProtoSchemas>> {
        let mut pinned = self.resolver.pinned();
        if pinned.is_immutable_snapshot() {
            let cur_gen = self.current_snapshot_gen(&mut pinned);
            if let Some(entry) = self.cache.get(service_name)
                && entry.verified_gen == Some(cur_gen)
            {
                return entry.compiled;
            }
            let Some(svc) = pinned.resolve_latest_service(service_name) else {
                self.cache.insert(
                    service_name.to_string(),
                    CachedServiceEntry {
                        verified_gen: Some(cur_gen),
                        revision: None,
                        metadata_fingerprint: None,
                        compiled: None,
                    },
                );
                return None;
            };
            return self.resolve_or_compile(&svc, Some(cur_gen));
        }

        let Some(svc) = pinned.resolve_latest_service(service_name) else {
            self.cache.invalidate(service_name);
            return None;
        };
        self.resolve_or_compile(&svc, None)
    }

    pub fn list_all_service_schemas(&self) -> Vec<Arc<ServiceProtoSchemas>> {
        let mut pinned = self.resolver.pinned();
        let snapshot_gen = pinned
            .is_immutable_snapshot()
            .then(|| self.current_snapshot_gen(&mut pinned));
        let services = pinned.list_services();
        let mut out = Vec::new();
        for svc in services {
            if let Some(schema) = self.resolve_or_compile(&svc, snapshot_gen) {
                out.push(schema);
            }
        }
        out.sort_by(|a, b| a.service_name.cmp(&b.service_name));
        out
    }
}
