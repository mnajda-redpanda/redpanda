# Iceberg Mode Configuration: Design Document

## Background

The `redpanda.iceberg.mode` topic property controls how Redpanda translates
Kafka records into Iceberg rows. The existing config supports four modes:

| Value | Meaning |
|---|---|
| `disabled` | Iceberg translation off |
| `key_value` | Both key and value stored as raw binary |
| `value_schema_id_prefix` | Value decoded using schema ID embedded in the record (Confluent wire format) |
| `value_schema_latest[:subject=S,protobuf_name=N]` | Value decoded using the latest schema for a registry subject |

All existing modes store the record key as `redpanda.key: binary` and header
values as `redpanda.headers: list<struct<key: string, value: binary>>`. This
document describes a generalized config format that adds structured key
deserialization and string header values.

Note: the existing `record_key_subject_name_strategy` /
`record_value_subject_name_strategy` topic properties are for schema validation
only and are not consulted by the datalake translation layer. Subject names for
`schema_latest` mode are configured inline in the mode string as described
below.

---

## New Config Format

### Grammar

```
<mode>       ::= "disabled" | "key_value" | <sections>
<sections>   ::= <section> (";" <section>)*
<section>    ::= ("key" | "value" | "headers") ":" <opts>
<opts>       ::= <opt> ("," <opt>)*

-- key/value opts:
<opt>        ::= "mode=" ("binary" | "schema_id_prefix" | "schema_latest")
               | "subject=" <string>
               | "protobuf_name=" <string>

-- headers opts:
<opt>        ::= "value_type=" ("binary" | "string")
               | "on_decode_error=" ("replace" | "null" | "drop")
```

Top-level sections are separated by `;`. Options within a section are
separated by `,`. Section names are `key`, `value`, and `headers`.

### Defaults

All options have implicit defaults so sections (and individual options within
sections) may be omitted:

| Section | Option | Default |
|---|---|---|
| `key` | `mode` | `binary` |
| `key` | `subject` | `<topic>-key` |
| `key` | `protobuf_name` | first message in file |
| `value` | `mode` | `binary` |
| `value` | `subject` | `<topic>-value` |
| `value` | `protobuf_name` | first message in file |
| `headers` | `value_type` | `binary` |
| `headers` | `on_decode_error` | `replace` |

`subject` and `protobuf_name` are only meaningful when `mode=schema_latest`.
`on_decode_error` is only meaningful when `value_type=string`.

A missing section is equivalent to a section with all defaults. So
`value:mode=schema_id_prefix` with no `key:` section means key defaults to
`mode=binary`.

### Examples

```
# Value decoded from embedded schema ID; key stays binary
value:mode=schema_id_prefix

# Value decoded from latest subject schema; key stays binary
value:mode=schema_latest,subject=my-topic-value

# Both key and value decoded from embedded schema ID
key:mode=schema_id_prefix;value:mode=schema_id_prefix

# Key from embedded ID, value from latest schema with explicit subject and protobuf type
key:mode=schema_id_prefix;value:mode=schema_latest,subject=my-topic-value,protobuf_name=com.example.MyMessage

# Header values interpreted as UTF-8 strings; invalid bytes replaced with U+FFFD
headers:value_type=string

# Full combination
key:mode=schema_id_prefix;value:mode=schema_latest,subject=my-topic-value;headers:value_type=string,on_decode_error=null
```

---

## Backward Compatibility: Config Strings

All existing config strings remain valid. They are treated as aliases that
parse into the equivalent new-format internal representation:

| Old string | Equivalent new-format meaning |
|---|---|
| `disabled` | unchanged (special token) |
| `key_value` | `key:mode=binary;value:mode=binary` |
| `value_schema_id_prefix` | `value:mode=schema_id_prefix` |
| `value_schema_latest` | `value:mode=schema_latest` |
| `value_schema_latest:subject=S` | `value:mode=schema_latest,subject=S` |
| `value_schema_latest:protobuf_name=N` | `value:mode=schema_latest,protobuf_name=N` |
| `value_schema_latest:subject=S,protobuf_name=N` | `value:mode=schema_latest,subject=S,protobuf_name=N` |

Old strings produce identical `iceberg_mode` objects to their equivalent
new-format strings. There is no difference in behavior.

---

## Backward Compatibility: Wire Format

`iceberg_mode` is serialized inside `topic_properties` using custom
`write_nested`/`read_nested` functions. The existing wire encoding uses a
`uint8` variant discriminant (0=disabled, 1=key_value, 2=value_schema_id_prefix,
3=value_schema_latest) followed by optional strings for `value_schema_latest`.

### Strategy

A new variant discriminant value (4, `key_and_value_schema`) is added for
configs that cannot be expressed in the old format. The write path selects the
encoding conservatively:

- If `key.mode == binary` and `headers.value_type == binary`: the config is
  expressible in the old format. Write using old discriminant values 0–3.
  Old nodes can read this without any change.
- Otherwise: write discriminant 4 followed by the full key, value, and headers
  section configs.

New nodes always retain the ability to read old discriminant values 0–3.

### Mid-upgrade behavior

During a rolling cluster upgrade:

- **New node writes config expressible in old format** → writes old discriminant
  → old nodes read fine. No issues.
- **New node writes config requiring discriminant 4** → old nodes cannot parse
  this. A feature flag gates activation of new-discriminant configs until the
  cluster is fully upgraded.
- **Old node writes** → new node reads old discriminants 0–3 fine.

---

## Key Schema Configuration

When `key:mode=schema_id_prefix` or `key:mode=schema_latest` is set, the
record key is deserialized using the same schema resolution machinery as the
value.

### Schema placement in the Iceberg table

Key fields are placed as a nested struct under `redpanda.key`, replacing the
current `redpanda.key: binary` field. Value fields continue to appear at the
top level of the table schema. For example, given:

```
key schema:   { foo: int,  bar: string }
value schema: { foo: int,  baaz: string }
```

The resulting Iceberg row is:

```json
{
  "redpanda": {
    "key": { "foo": 27, "bar": "hello" },
    "partition": 3,
    "offset": 1001,
    ...
  },
  "foo": 99,
  "baaz": "claude"
}
```

This placement avoids any collision between key and value field names. A value
field named `redpanda` is handled by the existing collision logic (moved into
`redpanda.data`); no new collision cases are introduced.

In binary key mode (`key:mode=binary`, the default), `redpanda.key` remains
`binary` as today.

---

## Header Value String Decoding

When `headers:value_type=string`, header values are decoded as UTF-8 strings
and the Iceberg column type changes from `binary` to `string`:

```
list<struct<key: string, value: binary>>   →   list<struct<key: string, value: string>>
```

### On decode error

Since arbitrary Kafka header values may not be valid UTF-8, the behavior on
decode failure is configurable via `on_decode_error`:

| Value | Behavior |
|---|---|
| `replace` (default) | Replace each invalid byte sequence with U+FFFD (standard Unicode replacement character). The header entry is always present with a non-null value. |
| `null` | Keep the header key in the list entry but set the value to null. Null is distinguishable from an empty string (`""`), which is a valid header value. |
| `drop` | Remove the entire header entry (key and value) from the list. |

The default `replace` follows standard lossy UTF-8 decode behavior (as in
Python `errors='replace'`, Java `CodingErrorAction.REPLACE`) and avoids NULLs
in the output, which simplifies downstream queries.

In `binary` mode (the default), header values are stored as-is with no decode
attempt and `on_decode_error` has no effect.

---

## Implementation Notes

The implementation is largely a generalization of the existing value translation
machinery:

- **`iceberg_mode`** (`model/metadata.h`): replace the 4-variant
  `std::variant` with `disabled_impl` | `enabled_impl`, where `enabled_impl`
  holds a `section_config` for key and value and a `headers_config`. Old
  variant discriminants 0–3 map directly to `enabled_impl` instances during
  deserialization.

- **`datalake_manager.cc`**: `make_type_resolver` splits into separate key and
  value resolver factories driven by the per-section `mode`. An additional
  `headers_config` is threaded through to the translator.

- **`record_multiplexer`**: add a parallel `_key_type_resolver.resolve_buf_type`
  call alongside the existing value resolver call. Populate
  `record_schema_components::key_identifier` (already defined, always nullopt
  today) from the result.

- **`record_translator::build_type` / `translate_data`**: add `key_type`
  parameter alongside the existing `val_type`. In `build_type`, replace
  `redpanda.key: binary` with the resolved key struct type when key mode is
  structured. In `translate_data`, deserialize the key buffer using
  `value_translating_visitor` (same path as value deserialization today).

- **`build_rp_struct`**: change the `key` parameter from
  `std::optional<iobuf>` to `std::optional<iceberg::value>`. Callers supply
  either a `binary_value` (binary mode) or a `struct_value` (structured mode).

- **`build_headers_value`**: when `value_type=string`, attempt UTF-8 decode of
  each header value and apply the configured `on_decode_error` policy.
  No schema changes are needed since the `value` field is already
  `field_required::no`.

- **`cluster/metrics_reporter.cc`**: add counters alongside the existing
  `topics_with_iceberg_kv` / `topics_with_iceberg_schema_id` /
  `topics_with_iceberg_schema_latest` for the new dimensions:
  `topics_with_iceberg_key_schema` (topics where `key.mode != binary`) and
  `topics_with_iceberg_string_headers` (topics where
  `headers.value_type == string`). Wire these into the JSON snapshot output.
