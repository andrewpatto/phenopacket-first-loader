import { isAbsolute } from "node:path";
import { readdir } from "node:fs/promises";
import { PfdlBatch, BatchDirent } from "./pfdl-batch";
import { PfdlRoot } from "./pfdl-root";
import { PfdlArtifact } from "./pfdl-artifact";
import { ErrorReport, ErrorSpecific } from "./common-types";
import { org } from "./filetypes/phenopackets/phenopackets";
import {
  PfdlDataset,
  PfdlFamily,
  PfdlFile,
  PfdlIndividual,
  PfdlPhenopacket,
} from "./dataset-types";
import { RootFactory } from "./root/root-factory";
import { PfdlError } from "./pfdl-errors";
import {
  mergeRootBatchesInOrderedGroups,
  PfdlInternalBatchGroup,
} from "./pfdl-internal-batch-group";
import { isEqual } from "lodash";
import { PfdlInternalArtifactIdentical } from "./pfdl-internal-artifact-identical";
import { PfdlArtifactHistory } from "./pfdl-artifact-history";

type LoaderStructureReport = {
  state: "data";

  // a map of all artifacts in the loader via artifact name
  // and including the full history of each
  artifacts: Map<string, PfdlArtifactHistory>;

  // the key names of any artifacts which have been marked as
  // deleted (they will still appear in the artifacts map)
  deleted: Set<string>;
};

export type LoaderStructure = ErrorReport | LoaderStructureReport;

type LoaderDatasetReport = {
  state: "data";

  dataset: PfdlDataset;
};

export type LoaderDataset = ErrorReport | LoaderDatasetReport;

export class Loader {
  constructor(private _absoluteRootFolders: string[]) {}

  /**
   * Checks the file layout/structure of the proposed dataset and
   * returns either details of all the structure, or an error
   * structure with details of where the layout is wrong.
   *
   * @returns
   */
  public async checkStructure(): Promise<LoaderStructure> {
    // all of our logic is going to be simpler if we insist on
    // all the roots being absolute paths

    const failedRootAbsolutes: string[] = [];
    const failedRootAccesses: string[] = [];

    // all the input roots need to be recognised as absolute paths
    // this is also the first chance we have to even check if the Root folders exist
    for (const r of this._absoluteRootFolders) {
      // TODO: add an existence check for S3 roots
      if (r.startsWith("s3://")) continue;

      if (!isAbsolute(r)) {
        failedRootAbsolutes.push(r);
      } else {
        try {
          await readdir(r);
        } catch (e) {
          failedRootAccesses.push(r);
        }
      }
    }

    // we found an error - return them all
    if (failedRootAbsolutes.length > 0 || failedRootAccesses.length > 0) {
      const errorRoots = failedRootAbsolutes
        .map((e) => ({
          message: "Root path not recognised as 'absolute' path",
          root: e,
        }))
        .concat(
          failedRootAccesses.map((e) => ({
            message: "Root path not accessible",
            root: e,
          })),
        );
      errorRoots.sort((a, b) => a.root.localeCompare(b.root));

      return {
        state: "error",
        error: "Root paths invalid",
        specific: errorRoots,
      };
    }

    // create all the Root objects - with a factory that knows how to create different
    // flavours (S3 v Posix)
    // the root objects will then be responsible for creating subsequent objects with
    // matching types i.e. S3Root -> S3Batch
    const roots = this._absoluteRootFolders.map((arf) =>
      RootFactory.CreateRoot(arf),
    );

    // find all the batches that exist under our root(s) - but with ordering across
    // all the roots.
    // we are going to process them in order so that "later" batches will be able to override
    // artifacts in older batches
    const batchGroups = await mergeRootBatchesInOrderedGroups(roots);

    // for every group fill in the list of files we discover inside the batch
    {
      // rather than abort on the first error we find - we want to process all the
      // batches and collate error messages - then throw an exception
      const errorEntries: ErrorSpecific[] = [];

      for (const [_, group] of batchGroups) {
        try {
          for (const batch of group.batches) {
            group.batchDirEnts.set(batch, await batch.loadAndCheckEntries());
          }
        } catch (e) {
          if (e instanceof PfdlError) errorEntries.push(...e.specifics);
          else throw e;
        }
      }

      // if we found any base errors we need to abort
      if (errorEntries.length > 0) {
        const specific = Array.from(errorEntries);
        specific.sort((a, b) => a.message.localeCompare(b.message));

        return {
          state: "error",
          error: "Loading entries",
          specific: specific,
        };
      }
    }

    // for every group now load the manifest and check that all the manifests for grouped batches are identical
    // (batches are only allowed to have the same name if they are identical across roots)
    {
      const errorManifests: ErrorSpecific[] = [];

      for (const [_, group] of batchGroups) {
        let firstManifest: Record<string, string> | undefined = undefined;
        let firstBatch: PfdlBatch | undefined = undefined;
        try {
          for (const batch of group.batches) {
            // no matter what we *always* need to load the manifest for every batch
            const batchManifest = await batch.loadAndCheckManifest(
              group.batchDirEnts.get(batch)!,
            );

            if (!firstManifest) {
              firstManifest = batchManifest;
              firstBatch = batch;
            } else {
              if (!isEqual(batchManifest, firstManifest)) {
                // we create two errors - one for each batch entry that has clashed here
                // TODO: add details of where the manifest is different to the errors
                errorManifests.push({
                  message:
                    "Batch name is common between two roots, but the manifest in the batches is different",
                  root: firstBatch!.root.root,
                  batch: firstBatch!.name,
                });
                errorManifests.push({
                  message:
                    "Batch name is common between two roots, but the manifest in the batches is different",
                  root: batch!.root.root,
                  batch: batch!.name,
                });
              }
            }
          }

          if (!firstManifest) throw new Error("Missing manifest");

          group.manifest = firstManifest;
        } catch (e) {
          if (e instanceof PfdlError) errorManifests.push(...e.specifics);
          else throw e;
        }
      }

      if (errorManifests.length > 0) {
        return {
          state: "error",
          error: "Loading manifests",
          specific: errorManifests,
        };
      }
    }

    // at this point we now have all the artifacts of each batch - maintaining
    // still the order of batches in our maps though
    // we now want to make a map keyed by artifact name - and layering in our
    // deletion/update logic
    const artifacts = new Map<string, PfdlArtifactHistory>();

    // linearly through each unique batch name
    // e.g. "2020-01", "2020-05"
    for (const [batch, group] of batchGroups) {
      // going through the artifact names in the batch group (noting that every batch in the group
      // is guaranteed to have exactly the same artifact names!) - these batch groups are essentially
      // identical - just located in different clouds
      for (const [manifestArtifactName, manifestMd5] of Object.entries(
        group.manifest,
      )) {
        // create all the actual artifact entries (this may trigger content loads for small objects)
        const loadedArtifactGroup: PfdlInternalArtifactIdentical = {
          artifacts: await Promise.all(
            group.batches.map((b) =>
              b.loadAndCheckArtifact(manifestArtifactName, manifestMd5),
            ),
          ),
        };

        // if we have seen this before in a previous batch then we are update/deleting
        if (artifacts.has(manifestArtifactName)) {
          // we are processing batches from earlier -> latest, but the semantics
          // of updating are that we want the "latest" item to appear first
          artifacts
            .get(manifestArtifactName)
            ?.reverseSortedByBatchName.unshift(loadedArtifactGroup);
        } else {
          // this is the first time we've seen this artifact name - lets create the history record
          // with a single entry - but first we need to actually make the artifacts

          artifacts.set(manifestArtifactName, {
            reverseSortedByBatchName: [loadedArtifactGroup],
          });
        }
      }
    }

    return {
      state: "data",
      artifacts: new Map(
        [...artifacts].sort((a, b) => a[0].localeCompare(b[0])),
      ),
      deleted: new Set(),
    };
  }

  /**
   * Locates all the phenopackets in the data set and does basically
   * structural checking on them.
   * - all artifacts mentioned are present
   * - all artifacts present are mentioned
   * - basic checks
   *
   * @param structure
   * @returns
   */
  public async checkPhenopacketStructure(
    structure: LoaderStructureReport,
  ): Promise<ErrorReport | null> {
    // if we find errors we want to return details
    const errorEntries: ErrorSpecific[] = [];

    // the "used" artifacts are either phenopackets themselves, or objects that
    // have been mentioned by a phenopacket (or their accompanying index)
    const usedArtifacts = new Set<string>();

    for (const [name, historyOfArtifacts] of structure.artifacts) {
      const artifactGroup = historyOfArtifacts.reverseSortedByBatchName[0];
      const artifact = artifactGroup.artifacts[0];

      // an artifact can still be listed even after it is deleted... so we need
      // to explicitly check in another special set
      if (structure.deleted.has(name)) continue;

      const packet = await artifact.getContentAsPhenopacket();

      // we only want to process phenopackets themselves
      if (!packet) continue;

      // mark off the phenopacket as being "used"
      usedArtifacts.add(name);

      // we search the phenopackets for files and record them here for validity checking
      const filesReferencedByPacket: org.phenopackets.schema.v2.core.IFile[] =
        [];

      // all the files in a single phenopacket add to our found list
      const packetGatherFiles = (
        p: org.phenopackets.schema.v2.IPhenopacket,
      ) => {
        // gather the files
        for (const f of p.files ?? []) filesReferencedByPacket.push(f);

        for (const b of p.biosamples ?? []) {
          for (const f of b.files ?? []) filesReferencedByPacket.push(f);
        }
      };

      if (packet instanceof org.phenopackets.schema.v2.Phenopacket) {
        packetGatherFiles(packet);

        // standard phenopackets become a singleton case - that is a patient id of the patient and an empty case
        if (!packet.subject)
          throw new Error(
            `Subject must be present in Phenopacket - content was ${JSON.stringify(
              packet.toJSON(),
            )}`,
          );
        if (!packet.subject.id)
          throw new Error("Subject::id must be present in Phenopacket");
      } else if (packet instanceof org.phenopackets.schema.v2.Family) {
        if (!packet.id) {
          errorEntries.push({
            root: artifact.batch.root.root,
            batch: artifact.batch.name,
            artifact: artifact.name,
            message:
              "Family Phenopacket must have an id representing the identifier of the family as a batchArtifacts",
          });
          continue;
        }

        // the proband
        if (packet.proband) {
          packetGatherFiles(packet.proband);
        }

        // gather the family files
        for (const f of packet.files ?? []) filesReferencedByPacket.push(f);

        for (const p of packet.relatives ?? []) {
          packetGatherFiles(p);
        }
      } else {
        // mark an error entry as the phenopacket was somehow not something we otherwise recognise
        errorEntries.push({
          root: artifact.batch.root.root,
          batch: artifact.batch.name,
          artifact: artifact.name,
          message:
            "Phenopacket artifact was found but was not recognised as an individual or family phenopacket",
        });

        continue;
      }

      // now check the file entries for validity
      for (const ff of filesReferencedByPacket) {
        // file entries must have URIs or the whole thing won't work
        if (!ff.uri) {
          errorEntries.push({
            root: artifact.batch.root.root,
            batch: artifact.batch.name,
            artifact: artifact.name,
            message: "Phenopacket contained a file entry with no URI",
          });
        } else {
          let fileName;
          if (ff.uri.startsWith("file://")) {
            fileName = ff.uri.slice(7);
          }
          if (ff.uri.startsWith("file:/")) {
            fileName = ff.uri.slice(6);
          }
          // we only work with relative file references
          if (!fileName) {
            errorEntries.push({
              root: artifact.batch.root.root,
              batch: artifact.batch.name,
              artifact: artifact.name,
              message: `Phenopacket contained a file URI '${ff.uri}' that is an absolute URI`,
            });
          } else {
            // every file reference must relate to an artifact in our dataset and which we will mark off
            if (structure.artifacts.has(fileName)) {
              usedArtifacts.add(fileName);

              // TODO: do this better

              // index artifacts exist but are not named in the phenopackets
              // we mark them off here if they exist
              if (structure.artifacts.has(fileName + ".bai"))
                usedArtifacts.add(fileName + ".bai");
              if (structure.artifacts.has(fileName + ".tbi"))
                usedArtifacts.add(fileName + ".tbi");
            } else {
              if (structure.deleted.has(fileName)) {
                errorEntries.push({
                  root: artifact.batch.root.root,
                  batch: artifact.batch.name,
                  artifact: artifact.name,
                  message: `Phenopacket contained a file entry '${ff.uri}' that is referencing a deleted artifact of the dataset`,
                });
              } else {
                errorEntries.push({
                  root: artifact.batch.root.root,
                  batch: artifact.batch.name,
                  artifact: artifact.name,
                  message: `Phenopacket contained a file entry '${ff.uri}' that is referencing a non-existent artifact in the dataset`,
                });
              }
            }
          }
        }
      }
    }

    // now look to see if we have any dataset items that were not referenced (but we only do this if we don't otherwise have errors -
    // because any above errors will cause spurious messages here)
    if (errorEntries.length === 0) {
      for (const [name, historyOfArtifacts] of structure.artifacts) {
        const artifactGroup = historyOfArtifacts.reverseSortedByBatchName[0];
        const artifact = artifactGroup.artifacts[0];

        // an artifact can still be listed even after it is deleted... so we need
        // to explicitly check in another special set
        if (structure.deleted.has(name)) continue;

        if (!usedArtifacts.has(name)) {
          errorEntries.push({
            root: artifact.batch.root.root,
            batch: artifact.batch.name,
            artifact: artifact.name,
            message: `Artifact was not referenced by any phenopacket`,
          });
        }
      }
    }

    if (errorEntries.length > 0)
      return {
        state: "error",
        error: "Phenopacket",
        specific: errorEntries,
      };

    // returning null means everything checked out
    return null;
  }

  public async checkPhenopackets(
    structure: LoaderStructureReport,
  ): Promise<LoaderDataset> {
    const families: PfdlFamily[] = [];
    const individuals: PfdlIndividual[] = [];

    // we sometimes need to use a representative Artifact - navigating our
    // histories and the fact that artifacts can span multiple roots. This just
    // puts the very simple logic in one spot (we pick the first!)
    const getRepresentativeArtifact = (artifactName: string): PfdlArtifact => {
      const artifactHistory = structure.artifacts.get(artifactName);
      if (!artifactHistory)
        throw new Error(`Artifact ${artifactName} is not known`);

      // it should not be possible for there to be histories or identicals with less than 1 entry
      return artifactHistory.reverseSortedByBatchName[0].artifacts[0];
    };

    const inplaceConvertFile = (f: PfdlFile) => {
      // the object we get passed in will be the original phenopacket IFile (even though the
      // type says otherwise)
      const origFile = f as unknown as org.phenopackets.schema.v2.core.IFile;

      // the phenopacket Uri we are going to convert to a "name" - and then link in an "artifact"
      let fileName;
      if (origFile.uri?.startsWith("file://")) {
        fileName = origFile.uri.slice(7);
      }
      if (origFile.uri?.startsWith("file:/")) {
        fileName = origFile.uri.slice(6);
      }
      if (!fileName)
        throw new Error(
          `Unrecognised prefix in Phenopacket file URI '${origFile.uri}' - this error should have been captured earlier`,
        );

      delete origFile.uri;

      const artifactHistory = structure.artifacts.get(fileName)!;
      const artifactFirst = artifactHistory.reverseSortedByBatchName[0];

      // artifacts are identical - but differing cloud services can provide different checksums
      // so we want to merge all the checksums we know
      const mergedChecksums: Record<string, string> = {};

      for (const a of artifactFirst.artifacts) {
        for (const [c, cValue] of Object.entries(a.getChecksums())) {
          if (mergedChecksums[c] && mergedChecksums[c] != cValue)
            throw new Error(
              `Found an artifact '${fileName}' that was meant to be identical but differed in checksum ${c} - ${mergedChecksums[c]} v ${cValue}`,
            );
          mergedChecksums[c] = cValue;
        }
      }

      f.name = fileName;
      f.artifacts = {
        uris: artifactHistory.reverseSortedByBatchName[0].artifacts.map(
          (a) => a.uri,
        ),
        size: artifactHistory.reverseSortedByBatchName[0].artifacts[0].size,
        checksums: mergedChecksums,
      };
    };

    const inplaceConvertPhenopacket = (p: PfdlPhenopacket) => {
      // we are going to mutate all the file entries throughout the phenopacket to add in artifact links
      for (const f of p.files ?? []) {
        inplaceConvertFile(f);

        // we need to go deep into the nested biosamples to fix all the files
        for (const b of p.biosamples ?? []) {
          for (const f of b.files ?? []) inplaceConvertFile(f);
        }
      }
    };

    const inplaceTakeOutConsents = async (
      files: PfdlFile[] | null | undefined,
    ): Promise<any | undefined> => {
      if (!files) return undefined;

      let foundConsent: any;
      let i = files.length;
      while (i--) {
        const cp = await getRepresentativeArtifact(
          files[i].name,
        ).getContentAsConsentpacket();
        if (cp) {
          // we don't want to have to try to come up with our own merging logic
          if (foundConsent) {
            throw new Error(
              "Only one consentpacket can be applied at any level (family, individual, biosample)",
            );
          }

          foundConsent = cp;

          // because consent is brought into our data - we don't want it to appear as an artifact anymore
          files.splice(i, 1);
        }
      }
      return foundConsent;
    };

    const inplaceLoadConsent = async (p: PfdlPhenopacket) => {
      // we each level if we discover a consent packet reference then we bring in that consent data
      p.consent = await inplaceTakeOutConsents(p.files);

      // look for special consent at the biosample level
      for (const b of p.biosamples ?? []) {
        b.consent = await inplaceTakeOutConsents(b.files);
      }
    };

    for (const [name, historyOfArtifacts] of structure.artifacts) {
      const artifactGroup = historyOfArtifacts.reverseSortedByBatchName[0];
      const artifact = artifactGroup.artifacts[0];

      // an artifact can still be listed even after it is deleted... so we need
      // to explicitly check in another special set
      if (structure.deleted.has(name)) continue;

      // we will do an instanceof check to determine what type of phenopacket is returned
      const pp: any = await artifact.getContentAsPhenopacket();

      // we only want to process actual phenopackets - any artifact processed that does *not*
      // match a schema will return null
      if (!pp) continue;

      // NOTE: we will be making JSON copies of the phenopacket and then
      // "in-place" mutating the objects to make them match our desired PFDL schema

      if (pp instanceof org.phenopackets.schema.v2.Phenopacket) {
        const pfdl: PfdlPhenopacket = pp.toJSON();

        inplaceConvertPhenopacket(pfdl);

        await inplaceLoadConsent(pfdl);

        individuals.push(pfdl);
      }

      if (pp instanceof org.phenopackets.schema.v2.Family) {
        const pfdl: PfdlFamily = pp.toJSON();

        // family can have its own collection of files
        for (const f of pfdl.files ?? []) inplaceConvertFile(f);

        // now all the individuals in the family, both proband and relatives
        if (pfdl.proband) inplaceConvertPhenopacket(pfdl.proband);
        for (const p of pfdl.relatives ?? []) inplaceConvertPhenopacket(p);

        // find consents and import
        pfdl.consent = await inplaceTakeOutConsents(pfdl.files);
        if (pfdl.proband) await inplaceLoadConsent(pfdl.proband);
        for (const p of pfdl.relatives ?? []) await inplaceLoadConsent(p);

        families.push(pfdl);
      }
    }

    return {
      state: "data",
      dataset: {
        individuals: individuals,
        families: families,
      },
    };
  }
}
