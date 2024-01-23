import { isAbsolute } from "node:path";
import { Root } from "./root";
import { Dirent } from "fs";
import { readFile, readdir } from "node:fs/promises";
import { join } from "node:path";
import { Artifact } from "./artifact";
import { ErrorReport } from "./common-types";

export type BatchStructureData = {
  state: "data";
  artifacts: Artifact[];
};

export type BatchStructure = BatchStructureData | ErrorReport;

export type BatchArtifactsReport = {
  state: "data";
  entries: Dirent[];
};

export type BatchArtifacts = BatchArtifactsReport | ErrorReport;

export class Batch {
  constructor(
    private _root: Root,
    private _batchName: string
  ) {}

  public get name(): string {
    return this._batchName;
  }

  public get root(): Root {
    return this._root;
  }

  protected batchPath(name?: string): string {
    if (name) return join(this._root.root, this._batchName, name);
    else return join(this._root.root, this._batchName);
  }

  protected async loadContent(name: string): Promise<Buffer> {
    return await readFile(this.batchPath(name));
  }

  public async findArtifacts(): Promise<BatchArtifacts> {
    const entries = await readdir(this.batchPath(), { withFileTypes: true });

    const entryNotPlains: string[] = [];

    for (const e of entries) {
      if (!e.isFile()) entryNotPlains.push(e.name);
    }

    if (entryNotPlains.length > 0)
      return {
        state: "error",
        error: "Finding artifacts",
        specific: entryNotPlains.map((e) => ({
          message:
            "Artifact is not a plain object (e.g. is a sub-directory or named pipe)",
          root: this._root.root,
          batch: this._batchName,
          artifact: e,
        })),
      };

    return {
      state: "data",
      entries: entries,
    };
  }

  public async checkManifests(
    artifacts: BatchArtifactsReport
  ): Promise<BatchStructure> {
    let manifestContent: { [file: string]: string } | undefined = undefined;

    for (const e of artifacts.entries)
      if (e.name === "md5sums.txt")
        manifestContent = this.md5SumsToObject(
          (await this.loadContent(e.name)).toString("ascii")
        );

    if (!manifestContent)
      return {
        state: "error",
        error: "No manifest file (e.g. 'md5sums.txt') found in batch",
        specific: [
          {
            message: "No manifest file (e.g. 'md5sums.txt') found in batch",
            root: this._root.root,
            batch: this._batchName,
          },
        ],
      };

    // a Set of all the names actually present on disk
    const entryNames = new Set<string>(artifacts.entries.map((e) => e.name));

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
      if (e !== "md5sums.txt")
        extraArtifacts.push(e);
    }

    if (missingArtifacts.length > 0 || extraArtifacts.length > 0) {
      return {
        state: "error",
        error: "Discrepency between manifest and content of batch",
        specific: missingArtifacts
          .map((e) => ({
            message: "Artifact is listed in the manifest but not present",
            root: this._root.root,
            batch: this._batchName,
            artifact: e,
          }))
          .concat(
            extraArtifacts.map((e) => ({
              message: "Artifact is present but not in the manifest",
              root: this._root.root,
              batch: this._batchName,
              artifact: e,
            }))
          ),
      };
    }

    return {
      state: "data",
      artifacts: await Promise.all(Object.entries(manifestContent).map(async ([md5name, md5value]) => {
        const a = await Artifact.CreateArtifact(this, md5name, this.batchPath(md5name));
        a.setChecksum("MD5", md5value);
       
        return a;
      })),
    };
  }

  private md5SumsToObject(content: string) {
    // For each file, ‘md5sum’ outputs by default, the MD5 md5Checksum,
    // a space, a flag indicating binary or text input mode, and the
    // file name. Binary mode is indicated with ‘*’, text mode
    // with ‘ ’ (space). Binary mode is the default on systems where
    // it’s significant, otherwise text mode is the default.
    let sums: { [file: string]: string } = {};

    for (const line of content.split("\n")) {
      // skip possible blank last line
      if (line.trim().length == 0)
        continue;

        if (line.startsWith("\\")) {
        // TODO we could support this but no need unless this is actually encountered (which is doubtful)
        // Without --zero, if file contains a backslash, newline,
        // or carriage return, the line is started with a backslash, and
        // each problematic character in the file name is escaped
        // with a backslash, making the output unambiguous
        // even in the presence of arbitrary file names.
        throw new Error("We do not support md5sums with escaped file names");
      }

      const checksum = line.slice(0, 32);

      // TODO check the md5Checksum is valid - NOT that the content matches - just literally does it match the
      // definition for an md5 sum?

      const file = line.slice(34);

      // we ignore the type - should we?? (will be either space or *)
      const t = line.slice(33, 34);

      if (file === "md5sums.txt")
        continue;

      sums[file] = checksum;
    }

    return sums;
  }
}
