# Symbolic schema name used in all DBOS table definitions. It is never sent to
# the database directly: each engine carries a SQLAlchemy ``schema_translate_map``
# that rewrites this placeholder to the real per-instance schema (or ``None`` for
# SQLite) at execution time. This keeps the schema per-engine instead of mutating
# shared table metadata, so multiple DBOS instances/clients with different schemas
# can coexist in one process. A distinctive sentinel (rather than "dbos") avoids
# accidentally translating user tables that legitimately live in a schema named
# "dbos" within a shared application/datasource database.
SCHEMA_PLACEHOLDER = "__dbos_placeholder_schema__"
