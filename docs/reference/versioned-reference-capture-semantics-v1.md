# Versioned reference capture semantics v1

This is a deeper reference note.

For the primary onboarding path, start with:
- [docs/getting-started/06-entities-and-versioned-references.md](../getting-started/06-entities-and-versioned-references.md)
- [docs/reference/aggregate-reference-semantics-v1.md](aggregate-reference-semantics-v1.md)

This note defines the supported capture semantics for `VERSIONED FOREIGN KEY`
references.

## Automatic capture

When the `AS OF` column is omitted or `NULL`, ASQL resolves the reference token
automatically against the transaction-visible snapshot.

- For non-entity referenced tables, the captured token is the referenced row's
  current visible `_lsn`.
- For entity root tables, the captured token is the latest visible entity
  version number.

## Same-transaction visibility

Statement order is preserved within a transaction.

- Later statements can reference rows inserted or updated by earlier statements
  in the same transaction.
- If an earlier statement in the same transaction mutates an entity root, later
  references to that root capture the pending version that will be committed by
  the transaction.

## Explicit override

Advanced callers can still supply the `AS OF` column explicitly.

- For entity references, the supplied value is treated as an entity version.
- For non-entity references, the supplied value is treated as a raw `LSN`.
- Explicit entity versions may target either an already committed version or a
  pending version created earlier in the same transaction.

## Failure behavior

Writes fail with explicit errors when:

- the referenced row is not visible,
- the referenced entity root has no visible committed or pending version,
- a child entity table is referenced instead of the entity root table,
- an explicit override points to a missing version or row.

## Replay

Replay reconstructs auto-captured reference tokens by reapplying mutations in
WAL order with the same transaction-visible semantics. This preserves
deterministic results across restart and recovery.

## Seed scenario example

A realistic multi-domain seed can now use ordinary inserts without manual raw
`LSN` plumbing.

```sql
BEGIN CROSS DOMAIN billing, clinical, patients;

UPDATE patients.patients
SET full_name = 'Ana López García'
WHERE id = 'patient-1';

UPDATE clinical.admissions
SET status = 'IN_CARE'
WHERE id = 'admission-1';

INSERT INTO billing.invoices (
  id,
  invoice_number,
  patient_id,
  admission_id,
  total_cents
) VALUES (
  'invoice-2',
  'INV-002',
  'patient-1',
  'admission-1',
  8500
);

COMMIT;
```

The engine automatically captures the visible patient and admission reference
tokens for the invoice row, including versions produced earlier in the same
transaction.