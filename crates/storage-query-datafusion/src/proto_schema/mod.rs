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

use std::collections::{HashMap, HashSet};
use std::fmt::Debug;
use std::sync::{Arc, OnceLock};

use base64::Engine;
use dashmap::DashMap;
use parking_lot::RwLock;
use prost::Message;
use prost_types::field_descriptor_proto::{Label, Type};
use prost_types::{DescriptorProto, EnumDescriptorProto, FieldDescriptorProto, FileDescriptorSet};
use restate_types::live::Live;
use restate_types::schema::service::{ServiceMetadata, ServiceMetadataResolver};

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
pub enum PathStep {
    SubMessage {
        tag: u32,
    },
    MapSubMessage {
        tag: u32,
        key_kind: FieldKind,
        key_str: String,
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
        key_str: String,
        value_kind: FieldKind,
    },
}

#[derive(Debug)]
pub struct CompiledMessageSchema {
    pub full_name: String,
    pub fields_by_name: HashMap<String, CompiledField>,
    pub fields_by_tag: HashMap<u32, CompiledField>,
    pub is_map_entry: bool,
    path_cache: RwLock<HashMap<String, Arc<Vec<PathStep>>>>,
    summary_json: OnceLock<String>,
}

impl CompiledMessageSchema {
    pub fn resolve_path(
        &self,
        field_path: &str,
        pool: &CompiledDescriptorPool,
    ) -> Option<Arc<Vec<PathStep>>> {
        if let Some(cached) = self.path_cache.read().get(field_path) {
            return Some(cached.clone());
        }
        let compiled = Arc::new(self.compile_path_uncached(field_path, pool)?);
        if field_path.len() <= MAX_CACHED_FIELD_PATH_BYTES {
            let mut cache = self.path_cache.write();
            if cache.len() < MAX_CACHED_PATHS_PER_MESSAGE {
                cache.insert(field_path.to_string(), compiled.clone());
            }
        }
        Some(compiled)
    }

    #[cfg(test)]
    pub(crate) fn cached_path_count(&self) -> usize {
        self.path_cache.read().len()
    }

    fn compile_path_uncached(
        &self,
        field_path: &str,
        pool: &CompiledDescriptorPool,
    ) -> Option<Vec<PathStep>> {
        let mut iter = field_path.split('.').filter(|s| !s.is_empty()).peekable();
        iter.peek()?;
        let mut steps = Vec::with_capacity(4);
        let mut current_msg = self;

        while let Some(seg) = iter.next() {
            if steps.len() >= MAX_PROTO_RECURSION_DEPTH {
                return None;
            }
            let field = current_msg.fields_by_name.get(seg)?;
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
                    map_key,
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
            steps.push(PathStep::SubMessage { tag: field.number });
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
    if is_terminal_key {
        if matches!(v_kind, FieldKind::Message(_)) {
            return None;
        }
        steps.push(PathStep::MapLookup {
            tag,
            key_kind: k_kind.clone(),
            key_str: map_key.to_string(),
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
        key_str: map_key.to_string(),
    });
    Some(Some(pool.resolve_message(sub_name)?))
}

#[derive(Default)]
struct PoolBuilder {
    raw_map_entries: HashMap<String, (FieldKind, FieldKind)>,
    enums: HashMap<String, HashMap<i32, String>>,
    messages: HashMap<String, Arc<CompiledMessageSchema>>,
    short_names: HashMap<String, Option<String>>,
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
    pub fn from_file_descriptor_set(fds: &FileDescriptorSet) -> Self {
        let mut builder = PoolBuilder {
            remaining_items: MAX_POOL_ITEMS,
            ..Default::default()
        };
        for file in &fds.file {
            let pkg = file.package.as_deref().unwrap_or("");
            if pkg.len() > MAX_IDENTIFIER_LEN {
                continue;
            }
            for en in &file.enum_type {
                collect_enum(&mut builder, pkg, en);
            }
            for msg in &file.message_type {
                collect_map_entries_and_enums(&mut builder, pkg, msg, 0);
            }
        }

        for file in &fds.file {
            let pkg = file.package.as_deref().unwrap_or("");
            if pkg.len() > MAX_IDENTIFIER_LEN {
                continue;
            }
            let is_proto3 = file.syntax.as_deref() == Some("proto3");
            for msg in &file.message_type {
                compile_message_recursive(&mut builder, pkg, msg, is_proto3, 0);
            }
        }

        Self {
            messages: builder.messages,
            short_names: builder.short_names,
            enums: builder.enums,
        }
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

fn collect_enum(builder: &mut PoolBuilder, prefix: &str, en: &EnumDescriptorProto) {
    let Some(name) = en.name.as_deref() else {
        return;
    };
    if name.len() > MAX_IDENTIFIER_LEN || !builder.take(1) {
        return;
    }
    let full = qualify_name(prefix, name);
    if full.len() > MAX_IDENTIFIER_LEN {
        return;
    }
    let mut vals = HashMap::new();
    for v in &en.value {
        let (Some(vname), Some(vnum)) = (v.name.as_deref(), v.number) else {
            continue;
        };
        if vname.len() > MAX_IDENTIFIER_LEN || !builder.take(1) {
            break;
        }
        vals.insert(vnum, vname.to_string());
    }
    builder.enums.insert(full, vals);
}

fn collect_map_entries_and_enums(
    builder: &mut PoolBuilder,
    prefix: &str,
    msg: &DescriptorProto,
    depth: usize,
) {
    if depth >= MAX_DESCRIPTOR_NESTING {
        return;
    }
    let Some(name) = msg.name.as_deref() else {
        return;
    };
    if name.len() > MAX_IDENTIFIER_LEN {
        return;
    }
    let full = qualify_name(prefix, name);
    if full.len() > MAX_IDENTIFIER_LEN {
        return;
    }
    let is_map = msg
        .options
        .as_ref()
        .and_then(|o| o.map_entry)
        .unwrap_or(false);
    if is_map && builder.take(1) {
        let mut k_kind = FieldKind::String;
        let mut v_kind = FieldKind::String;
        for f in &msg.field {
            if f.number == Some(1) {
                k_kind = convert_field_kind(f, &full);
            } else if f.number == Some(2) {
                v_kind = convert_field_kind(f, &full);
            }
        }
        builder
            .raw_map_entries
            .insert(full.clone(), (k_kind, v_kind));
    }
    for en in &msg.enum_type {
        collect_enum(builder, &full, en);
    }
    for nested in &msg.nested_type {
        collect_map_entries_and_enums(builder, &full, nested, depth + 1);
    }
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
) -> Option<CompiledField> {
    let (fname, fnum) = (f.name.as_deref()?, f.number?);
    if fnum <= 0 || fname.len() > MAX_IDENTIFIER_LEN || !builder.take(1) {
        return None;
    }
    let tag = fnum as u32;
    let kind = convert_field_kind(f, enclosing_full);
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

    Some(CompiledField {
        name: fname.to_string(),
        number: tag,
        kind,
        is_repeated: f.label == Some(Label::Repeated as i32),
        has_presence,
        oneof_siblings,
        map_entry,
    })
}

fn compile_message_recursive(
    builder: &mut PoolBuilder,
    prefix: &str,
    msg: &DescriptorProto,
    is_proto3: bool,
    depth: usize,
) {
    if depth >= MAX_DESCRIPTOR_NESTING || builder.messages.len() >= MAX_POOL_MESSAGES {
        return;
    }
    let Some(name) = msg.name.as_deref() else {
        return;
    };
    if name.len() > MAX_IDENTIFIER_LEN || !builder.take(1) {
        return;
    }
    let full = qualify_name(prefix, name);
    if full.len() > MAX_IDENTIFIER_LEN {
        return;
    }
    let is_map = msg
        .options
        .as_ref()
        .and_then(|o| o.map_entry)
        .unwrap_or(false);
    let oneof_groups = build_oneof_sibling_map(msg);

    let mut fields_by_name = HashMap::new();
    let mut fields_by_tag = HashMap::new();
    for f in &msg.field {
        if let Some(cf) = compile_single_field(builder, f, &full, is_proto3, &oneof_groups) {
            fields_by_name.insert(cf.name.clone(), cf.clone());
            fields_by_tag.insert(cf.number, cf);
        }
    }

    let schema = Arc::new(CompiledMessageSchema {
        full_name: full.clone(),
        fields_by_name,
        fields_by_tag,
        is_map_entry: is_map,
        path_cache: RwLock::new(HashMap::new()),
        summary_json: OnceLock::new(),
    });
    if !is_map {
        builder
            .short_names
            .entry(name.to_string())
            .and_modify(|existing| *existing = None)
            .or_insert_with(|| Some(full.clone()));
    }
    for nested in &msg.nested_type {
        compile_message_recursive(builder, &full, nested, is_proto3, depth + 1);
    }
    builder.messages.insert(full, schema);
}

fn resolve_descriptor_type_name(raw_type_name: Option<&str>, enclosing_scope: &str) -> String {
    let raw = raw_type_name.unwrap_or("");
    if let Some(stripped) = raw.strip_prefix('.') {
        stripped.to_string()
    } else if raw.is_empty() {
        String::new()
    } else {
        qualify_name(enclosing_scope, raw)
    }
}

fn convert_field_kind(f: &FieldDescriptorProto, enclosing_scope: &str) -> FieldKind {
    let raw_type = f.r#type.and_then(|t| Type::try_from(t).ok());
    match raw_type {
        Some(Type::Double) => FieldKind::Double,
        Some(Type::Float) => FieldKind::Float,
        Some(Type::Int64) => FieldKind::Int64,
        Some(Type::Uint64) => FieldKind::Uint64,
        Some(Type::Int32) => FieldKind::Int32,
        Some(Type::Fixed64) => FieldKind::Fixed64,
        Some(Type::Fixed32) => FieldKind::Fixed32,
        Some(Type::Bool) => FieldKind::Bool,
        Some(Type::String) => FieldKind::String,
        Some(Type::Bytes) => FieldKind::Bytes,
        Some(Type::Uint32) => FieldKind::Uint32,
        Some(Type::Sfixed32) => FieldKind::Sfixed32,
        Some(Type::Sfixed64) => FieldKind::Sfixed64,
        Some(Type::Sint32) => FieldKind::Sint32,
        Some(Type::Sint64) => FieldKind::Sint64,
        Some(Type::Enum) => FieldKind::Enum(resolve_descriptor_type_name(
            f.type_name.as_deref(),
            enclosing_scope,
        )),
        Some(Type::Message | Type::Group) => FieldKind::Message(resolve_descriptor_type_name(
            f.type_name.as_deref(),
            enclosing_scope,
        )),
        None => FieldKind::Bytes,
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

impl ServiceProtoSchemas {
    pub fn from_service_metadata(svc: &ServiceMetadata) -> Option<Self> {
        let b64 = svc
            .metadata
            .get(PROTO_DESCRIPTOR_SET_KEY)
            .or_else(|| svc.metadata.get(PROTO_FDS_SHORT_KEY))?
            .trim();
        if b64.len() > MAX_FDS_BASE64_LEN {
            return None;
        }
        let raw_bytes = base64::engine::general_purpose::STANDARD.decode(b64).ok()?;
        let fds = FileDescriptorSet::decode(raw_bytes.as_slice()).ok()?;
        let pool = Arc::new(CompiledDescriptorPool::from_file_descriptor_set(&fds));

        let mut state_key_messages = HashMap::new();
        let mut wildcard_message = None;

        for (meta_k, meta_v) in &svc.metadata {
            if meta_k == PROTO_STATE_WILDCARD_KEY {
                if let Some(m) = pool.resolve_message(meta_v.trim()) {
                    wildcard_message = Some(m.clone());
                }
            } else if let Some(state_key) = meta_k.strip_prefix(PROTO_STATE_KEY_PREFIX)
                && !state_key.is_empty()
                && let Some(m) = pool.resolve_message(meta_v.trim())
            {
                state_key_messages.insert(state_key.to_string(), m.clone());
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

type CachedServiceProtoEntry = (u32, u64, Option<Arc<ServiceProtoSchemas>>);

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
    cache: Arc<DashMap<String, CachedServiceProtoEntry>>,
}

impl<SMR: ServiceMetadataResolver + Send + Sync + 'static> DynamicProtoSchemaRegistry<SMR> {
    pub fn new(resolver: Live<SMR>) -> Self {
        let cache = Arc::new(DashMap::with_capacity(16));
        Self { resolver, cache }
    }
    fn resolve_or_compile(&self, svc: &ServiceMetadata) -> Option<Arc<ServiceProtoSchemas>> {
        if !svc.metadata.contains_key(PROTO_DESCRIPTOR_SET_KEY)
            && !svc.metadata.contains_key(PROTO_FDS_SHORT_KEY)
        {
            self.cache.remove(&svc.name);
            return None;
        }
        let fp = proto_metadata_fingerprint(svc);
        if let Some(entry) = self.cache.get(&svc.name)
            && entry.0 == svc.revision
            && entry.1 == fp
        {
            return entry.2.clone();
        }
        let compiled = ServiceProtoSchemas::from_service_metadata(svc).map(Arc::new);
        if self.cache.len() >= MAX_CACHED_SERVICES && !self.cache.contains_key(&svc.name) {
            let live_names: HashSet<String> = self
                .resolver
                .pinned()
                .list_service_names()
                .into_iter()
                .collect();
            self.cache.retain(|k, _| live_names.contains(k));
        }
        if self.cache.len() < MAX_CACHED_SERVICES || self.cache.contains_key(&svc.name) {
            self.cache
                .insert(svc.name.clone(), (svc.revision, fp, compiled.clone()));
        }
        compiled
    }

    pub fn resolve_service_schemas(&self, service_name: &str) -> Option<Arc<ServiceProtoSchemas>> {
        let pinned = self.resolver.pinned();
        let Some(svc) = pinned.resolve_latest_service(service_name) else {
            self.cache.remove(service_name);
            return None;
        };
        self.resolve_or_compile(&svc)
    }

    pub fn list_all_service_schemas(&self) -> Vec<Arc<ServiceProtoSchemas>> {
        let pinned = self.resolver.pinned();
        let services = pinned.list_services();
        let live_names: HashSet<&str> = services.iter().map(|s| s.name.as_str()).collect();
        self.cache.retain(|k, _| live_names.contains(k.as_str()));
        let mut out = Vec::new();
        for svc in services {
            if let Some(schema) = self.resolve_or_compile(&svc) {
                out.push(schema);
            }
        }
        out
    }
}
