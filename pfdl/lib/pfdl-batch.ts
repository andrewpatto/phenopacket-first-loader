import { PfdlRoot } from "./pfdl-root";
import { PfdlArtifact } from "./pfdl-artifact";
import { ErrorReport, ErrorSpecific } from "./common-types";
import { PosixArtifact } from "./posix/posix-artifact";
import { PfdlError } from "./pfdl-errors";
import { md5SumsToObject } from "./sums-helper";

export type BatchManifestData = {
  // the manifest is a list of all (non-manifest) files and their primary manifest checksum
  manifest: Record<string, string>;

  // the artifacts are constructed to wrap each entry in the manifest
  artifacts: PfdlArtifact[];
};

/**
 * A type that matches standard POSIX Dirent but only with features
 * we want to use across all types.
 */
export type BatchDirent = {
  isFile(): boolean;
  name: string;
};

export abstract class PfdlBatch {
  protected constructor(
    protected _root: PfdlRoot,
    protected _batchName: string,
  ) {}

  public get name(): string {
    return this._batchName;
  }

  public get root(): PfdlRoot {
    return this._root;
  }

  protected abstract batchPath(name?: string): string;

  protected abstract loadContent(name: string): Promise<Buffer>;

  public abstract createArtifact(
    batch: PfdlBatch,
    name: string,
    artifactPath: string,
  ): Promise<PfdlArtifact>;

  /**
   * For the batch introspect the file system/bucket to find out the list
   * of files/objects that actually exist on disk.
   */
  public abstract loadAndCheckEntries(): Promise<BatchDirent[]>;

  /**
   * For a given set of directory entries, confirm that there is a manifest in the
   * directory and that all files are listed in the manifest.
   *
   * @param entries
   */
  public async loadAndCheckManifest(
    entries: BatchDirent[],
  ): Promise<Record<string, string>> {
    let manifestContent: Record<string, string> | undefined = undefined;

    for (const e of entries)
      if (e.name === "md5sums.txt")
        manifestContent = md5SumsToObject(
          (await this.loadContent(e.name)).toString("ascii"),
        );

    if (!manifestContent)
      throw new PfdlError(
        "No manifest file (e.g. 'md5sums.txt') found in batch",
        [
          {
            message: "No manifest file (e.g. 'md5sums.txt') found in batch",
            root: this._root.root,
            batch: this._batchName,
          },
        ],
      );

    // a Set of all the names actually present on disk
    const entryNames = new Set<string>(entries.map((e) => e.name));

    const missingArtifacts: string[] = [];
    const extraArtifacts: string[] = [];

    // loop through everything listed in the manifest to find anything missing
    for (const [md5name, md5sum] of Object.entries(manifestContent)) {
      if (!entryNames.has(md5name)) {
        missingArtifacts.push(md5name);
      } else {
        entryNames.delete(md5name);
      }
    }

    // loop through everything in the artifacts to find anything extra
    for (const e of entryNames) {
      if (e !== "md5sums.txt") extraArtifacts.push(e);
    }

    if (missingArtifacts.length > 0 || extraArtifacts.length > 0) {
      throw new PfdlError(
        "Discrepancy between manifest and content of batch",
        missingArtifacts
          .map((e) => ({
            message: "An entry is listed in the manifest but not present",
            root: this._root.root,
            batch: this._batchName,
            artifact: e,
          }))
          .concat(
            extraArtifacts.map((e) => ({
              message: "An entry is present but not in the manifest",
              root: this._root.root,
              batch: this._batchName,
              artifact: e,
            })),
          ),
      );
    }

    return manifestContent;
  }

  public async loadAndCheckArtifact(manifestName: string, manifestMd5: string) {
    // make artifacts which is the first point to load the files (this will be the first location
    // for checksum errors and all sorts of other random filesystem/cloud/permission stuff)
    const loadingErrors: ErrorSpecific[] = [];

    try {
      const a = await this.createArtifact(
        this,
        manifestName,
        this.batchPath(manifestName),
      );

      try {
        a.setChecksum("md5", manifestMd5);
      } catch (checksumError: any) {
        loadingErrors.push({
          message: checksumError.message,
          root: this._root.root,
          batch: this._batchName,
          artifact: manifestName,
        });
      }

      return a;
    } catch (createError: any) {
      throw new PfdlError(createError.message, [
        {
          message: createError.message,
          root: this._root.root,
          batch: this._batchName,
          artifact: manifestName,
        },
      ]);
    }
  }
}
