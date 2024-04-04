import { PfdlArtifact } from "./pfdl-artifact";
import { BatchDirent, PfdlBatch } from "./pfdl-batch";
import { PfdlRoot } from "./pfdl-root";
import { PfdlInternalArtifactIdentical } from "./pfdl-internal-artifact-identical";

/**
 * The PfdlInternalBatchGroup is a type that is used to carry information
 * between stages of the loading process. It is an internal
 * structure so should not be relied on externally.
 */
export type PfdlInternalBatchGroup = {
  // the name of the batches grouped here
  // e.g. "2020-01"
  name: string;

  // the list of batches that have the common name
  batches: PfdlBatch[];

  // the dir ents is map of all the batches with this name and the raw directory
  // entries that are discovered in the file system/bucket for the batch
  batchDirEnts: Map<PfdlBatch, BatchDirent[]>;

  // the manifest is a list of all (non-manifest) files and their primary manifest checksum
  // for any single group - the manifest across all batches MUST be the same
  manifest: Record<string, string>;

  // this is the map of all the batches that we believe are identical
  // and their corresponding artifacts
  // artifacts: Map<string, PfdlInternalArtifactIdentical>;
};

/**
 * From the given roots, find all batches and sort them across *all* roots. Batches
 * will be returned in a group to allow batches that are "the same" but are in different
 * clouds.
 *
 * @param roots A sequence of roots whose order is irrelevant as the result is always sorted by batch name
 * @returns a map of batch names and their corresponding group structure - listing all the same batches across all roots
 */
export async function mergeRootBatchesInOrderedGroups(
  roots: PfdlRoot[],
): Promise<Map<string, PfdlInternalBatchGroup>> {
  const initialMap = new Map<string, PfdlInternalBatchGroup>();

  // need to collect the batch names across all the roots
  // and then later we will sort/insert them into a map that is ordered
  for (const r of roots) {
    for (const b of await r.batches()) {
      // as we encounter each named batch we make a batchArtifacts for it
      if (!initialMap.has(b.name)) {
        initialMap.set(b.name, {
          name: b.name,
          // everything starts out empty and gets filled in at various loader stages
          batches: [],
          batchDirEnts: new Map(),
          manifest: {},
        });
      }

      // by this point we know we have definitely created a group for this batch
      const group = initialMap.get(b.name)!;

      group.batches.push(b);
    }
  }

  // the javascript Map iterates keys in *insertion* order - but we would like the
  // iteration order to be meaningful to the clients

  // so we want to insert these alphabetically as our ordering
  // definition is alphabetically (i.e. batch "2010-01" needs to come before "2011-05")

  const orderedMap = new Map<string, PfdlInternalBatchGroup>();

  const batchNames = Array.from(initialMap.keys());
  batchNames.sort();

  for (const a of batchNames) {
    orderedMap.set(a, initialMap.get(a)!);
  }

  return orderedMap;
}
