// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use datafusion::arrow::datatypes::DataType;

use restate_util_string::ReString;

use crate::{ShardingScheme, define_schema};

define_schema! {
    /// Dynamically registered Protobuf schemas for Virtual Object and Workflow state.
    ///
    /// Populated automatically from service discovery metadata (`proto.descriptor_set` and `proto.state.<key>`).
    proto_schemas {
        name: "sys_proto_schemas",
        sharding: ShardingScheme::Unsharded(None),
        columns {
            /// The name of the registered user service.
            service_name: ReString => DataType::LargeUtf8,
            /// The revision of the service that registered this schema.
            revision: u64 => DataType::UInt64,
            /// The state key mapped to this Protobuf message (or `*` for wildcard default).
            state_key: ReString => DataType::LargeUtf8,
            /// Fully qualified Protobuf message name (e.g. `acme.v1.UserProfile`).
            message_name: ReString => DataType::LargeUtf8,
            /// JSON array describing all fields (`name`, `number`, `type`, `repeated`, `map`) of the message.
            fields_json: ReString => DataType::LargeUtf8,
        }
    }
}
