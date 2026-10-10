# Historical records

These pages preserve only context needed to interpret retirement and retained
data. They are not supported deployment procedures or future implementation plans.

- [Shared-cluster retirement](shared-cluster-retirement-2026-09.md): architecture decision, removed capabilities and retained compatibility.
- [Legacy schema retirement](legacy-schema-retirement-2026-09.md): migration 0171's disposition, retained data and upgrade/rollback constraints.
- [Reference audit](shared-cluster-reference-audit.json): the retirement-time snapshot of classified legacy terminology; paths and counts are historical, not a current repository inventory.
- [Staging data lineage](staging-data-lifecycle.md): identities and constraints used by historical backups and database rows.

## Recovering older material

The annotated [archive/shared-cluster-final tag](https://github.com/qianyi-sun/loom/tree/archive/shared-cluster-final)
identifies pre-retirement source at `504600cbcee21e4dfd10619ef34d64f455e21813`.
It is a historical snapshot, not a supported release or operational acceptance.

The former duplicate documentation/deployment archive is recoverable at
[the pre-cleanup commit](https://github.com/qianyi-sun/loom/tree/ce0cdf7c4ee6b3dce762d30ae8ffa7d9c491a987/archive).
Use Git history for old designs, research notes, dated benchmark reports and
retired operational inputs. Do not restore them into the active tree as plans
or current guidance. For example, inspect a specific old file with
`git show ce0cdf7c4ee6b3dce762d30ae8ffa7d9c491a987:archive/README.md`.

Current [restore](../runbooks/nebius-restore.md) and
[lineage conversion](../runbooks/nebius-lineage-conversion.md) procedures remain
under runbooks because they describe supported compatibility operations.

The [retained migration package index](../../database/README.md) explains the
three historical capacity chains and their current source and packaging layout.
