# Dynamic Protobuf SQL state introspection

## New Feature

### What Changed

Services can now register Protobuf schemas for their Virtual Object and Workflow state via service discovery metadata, and you can inspect and query those binary Protobuf payloads directly with SQL:

- **Dynamic schema registration via service metadata**:
  - `proto.descriptor_set` (or shorthand `proto.fds`): base64-encoded compiled `google.protobuf.FileDescriptorSet` bytes containing the service's Protobuf definitions.
  - `proto.state.<key>` (or `proto.state.*` as a wildcard default for unmapped keys): maps a state key to a Protobuf message name (either a fully qualified name or an unambiguous short name).
- **Schema introspection table (`sys_proto_schemas`)**:
  Lists dynamically registered Protobuf state schemas across active services with columns `service_name`, `revision`, `state_key`, `message_name`, and `fields_json`.
- **Protobuf SQL query functions**:
  - `proto_get_str(service_name, state_key, value, field_path)`: extracts a string, enum name, base64-encoded `bytes`, or string-formatted scalar field.
  - `proto_get_int(service_name, state_key, value, field_path)`: extracts a 64-bit integer field (`int32`, `int64`, `uint32`, `uint64`, `sint*`, `fixed*`, `sfixed*`, `bool`, or enum number).
  - `proto_get_float(service_name, state_key, value, field_path)`: extracts a 64-bit floating-point field.
  - `proto_get_bool(service_name, state_key, value, field_path)`: extracts a boolean field.
  - `proto_to_json(service_name, state_key, value)`: decodes the full Protobuf payload into a JSON string.

`field_path` uses dot-separated segments to traverse nested messages and map entries (for example, `'customer.address.city'` or `'attributes.region'`).

### Why This Matters

Virtual Objects and Workflows that store Protobuf-encoded state previously exposed only raw bytes in the `state.value` column. With dynamic Protobuf registration, you can discover available message schemas and filter, project, or decode Protobuf state directly in SQL queries without converting entire payloads first.

### Impact on Users

- Opt-in per service via service metadata; services that do not publish Protobuf metadata are unaffected.
- `proto_get_*` and `proto_to_json` return `NULL` when no matching schema is registered, the payload cannot be decoded, or the requested field path is missing or incompatible.
- Schema updates take effect automatically when a new service revision is registered.

### Examples

```sql
-- Discover registered Protobuf schemas and their fields
SELECT service_name, revision, state_key, message_name, fields_json
FROM sys_proto_schemas;

-- Filter and project fields from binary Protobuf state without full JSON decoding
SELECT
  service_key,
  proto_get_str(service_name, key, value, 'customer.email') AS email,
  proto_get_int(service_name, key, value, 'retry_count') AS retries,
  proto_to_json(service_name, key, value) AS payload_json
FROM state
WHERE service_name = 'OrderService'
  AND key = 'order'
  AND proto_get_bool(service_name, key, value, 'is_active') = true;
```
