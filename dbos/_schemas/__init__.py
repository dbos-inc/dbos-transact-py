# Placeholder schema in all table definitions; each engine's schema_translate_map rewrites it to the real schema (a sentinel, not "dbos", to avoid clashing with a user's real "dbos" schema).
SCHEMA_PLACEHOLDER = "__dbos_placeholder_schema__"
