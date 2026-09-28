"""Database-local coordination between personal access and shared migrations."""

# PostgreSQL advisory locks are local to a database. Keep this key stable across
# releases and before installation of the private access-management schema.
APPLICATION_SCHEMA_LOCK = 0x4C4F4F4D415050  # LOOMAPP
