import type { ChangeBatch, IngestInput, ISettle } from '../src/index'

/**
 * Call `db.ingest(input)` and immediately ack the produced batch so the next
 * `db.ingest` is unblocked. Tests that need to exercise pending-ack semantics
 * (e.g. observe `Err(PendingAck)`) should call `db.ingest` + `db.ack` manually.
 */
export async function ingestAndAck(
  db: ISettle,
  input: IngestInput,
): Promise<ChangeBatch | null> {
  const batch = await db.ingest(input)
  if (batch) db.ack(batch.sequence)
  return batch
}
