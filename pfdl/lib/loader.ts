import { isAbsolute, join } from "node:path";
import { readdir, access } from "fs/promises";
import { Dirent } from "fs";
import { Batch, BatchArtifactsReport } from "./batch";
import { Root } from "./root";
import { Artifact } from "./artifact";
import { ErrorReport, ErrorSpecific } from "./common-types";
import { BatchArtifacts, BatchStructureData } from "./batch";

type LoaderStructureReport = {
  state: "data";

  // a map of all artifacts in the loader
  artifacts: Map<string, Artifact[]>;

  // the key names of any artifacts which have been marked as
  // deleted (they will still appear in the artifacts map)
  deleted: Set<string>;
};

export type LoaderStructure = ErrorReport | LoaderStructureReport;

export class Loader {
  constructor(private _absoluteRootFolders: string[]) {}

  /**
   * From the given roots, find all batches and sort them across *all* roots
   *
   * @param roots A sequence of roots whose order is irrelevant as the result is always sorted by batch name
   * @returns
   */
  private async findBatchesInOrder(
    roots: Root[]
  ): Promise<Map<string, Batch> | ErrorSpecific[]> {
    const allNamesUnordered = new Map<string, Batch>();

    const errors: ErrorSpecific[] = [];

    // need to collect the batch names across all the roots
    // checking for duplicates
    // and then later we will sort/insert them into a map
    for (const r of roots) {
      for (const b of await r.batches()) {
        if (allNamesUnordered.has(b.name)) {
          errors.push({
            message: "Batch name has occurred in another root",
            root: r.root,
            batch: b.name,
          });
          errors.push({
            message: "Batch name has occurred in another root",
            root: allNamesUnordered.get(b.name)?.root?.root,
            batch: allNamesUnordered.get(b.name)?.name,
          });
        } else {
          allNamesUnordered.set(b.name, b);
        }
      }
    }

    if (errors.length > 0)
      return errors;

    // the javascript Map stores keys in *insertion* order
    // so we want to insert these alphabetically
    const map = new Map<string, Batch>();

    for (const a of Array.from(allNamesUnordered.keys()).toSorted()) {
      map.set(a, allNamesUnordered.get(a)!);
    }

    return map;
  }

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
      if (!isAbsolute(r)) {
        failedRootAbsolutes.push(r);
      } else {
        try {
          await access(r);
        } catch (e) {
          failedRootAccesses.push(r);
        }
      }
    }

    if (failedRootAbsolutes.length > 0 || failedRootAccesses.length > 0)
      return {
        state: "error",
        error: "Root paths invalid",
        specific: failedRootAbsolutes
          .map((e) => ({
            message: "Root path not recognised as 'absolute' path",
            root: e,
          }))
          .concat(
            failedRootAccesses.map((e) => ({
              message: "Root path not accessible",
              root: e,
            }))
          )
          .toSorted((a, b) => a.root.localeCompare(b.root)),
      };

    // find all the batches that exist under our root(s) - but with ordering across
    // all the roots
    // we are going to process them in order so that "later" batches will be able override
    // artifacts in older batches
    const batches = await this.findBatchesInOrder(
      this._absoluteRootFolders.map((arf) => new Root(arf))
    );

    if (!(batches instanceof Map)) {
      return {
        state: "error",
        error: "Artifacts",
        specific: batches
      }
    }

    // now construct all the corresponding directory entries
    const dataBatchEntries = new Map<Batch, BatchArtifactsReport>();
    const errorEntries: ErrorSpecific[] = [];

    for (const [_, b] of batches.entries()) {
      const a = await b.findArtifacts();

      if (a.state === "error") {
        errorEntries.push(...a.specific);
      } else {
        dataBatchEntries.set(b, a);
      }
    }

    // if we found any base errors we need to abort
    if (errorEntries.length > 0) {
      return {
        state: "error",
        error: "Artifacts",
        specific: errorEntries.toSorted((a, b) =>
          a.message.localeCompare(b.message)
        ),
      };
    }

    // now compare manifests to content
    const dataBatchArtifacts = new Map<Batch, BatchStructureData>();
    const errorArtifacts: ErrorSpecific[] = [];

    for (const [b, a] of dataBatchEntries) {
      const report = await b.checkManifests(a);

      if (report.state === "error") errorArtifacts.push(...report.specific);
      else dataBatchArtifacts.set(b, report);
    }

    if (errorArtifacts.length > 0) {
      return {
        state: "error",
        error: "Artifacts",
        specific: errorArtifacts,
      };
    }

    // at this point we now have all the artifacts of each batch - maintaining
    // still the order of batches in our maps though
    // we now want to make a map keyed by artifact name - and layering in our
    // deletion/update logic
    const artifacts = new Map<string, Artifact[]>();

    for (const [batch, batchArtifacts] of dataBatchArtifacts) {
      for (const a of batchArtifacts.artifacts) {
        // if we have seen this before in a previous batch then we are update/deleting
        if (artifacts.has(a.name)) {
          // we are processing batches from earlier -> latest, but the semantics
          // of updating are that we want the "latest" item to appear first
          artifacts.get(a.name)?.unshift(a);
        } else {
          // this is the first time we've seen this object - lets create the record
          // with a single entry
          artifacts.set(a.name, [a]);
        }
      }
    }

    return {
      state: "data",
      artifacts: new Map(
        [...artifacts].sort((a, b) => a[0].localeCompare(b[0]))
      ),
      deleted: new Set(),
    };
  }
}
