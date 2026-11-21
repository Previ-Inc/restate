// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use restate_storage_query_datafusion::proto_schema::CompiledMessageSchema;

use super::schema::SysProtoSchemasBuilder;

#[inline]
pub(crate) fn append_proto_schema_row(
    builder: &mut SysProtoSchemasBuilder,
    service_name: &str,
    revision: u32,
    state_key: &str,
    msg: &CompiledMessageSchema,
) {
    let mut row = builder.row();
    row.service_name(service_name);
    row.revision(u64::from(revision));
    row.state_key(state_key);
    row.message_name(&msg.full_name);
    if row.is_fields_json_defined() {
        row.fields_json(msg.fields_summary_json());
    }
}
