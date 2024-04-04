import { join } from "node:path";
import { PfdlRoot } from "../pfdl-root";
import { readdir, readFile } from "node:fs/promises";
import { BatchDirent, PfdlBatch } from "../pfdl-batch";
import { PfdlArtifact } from "../pfdl-artifact";
import { PosixArtifact } from "./posix-artifact";
import { PfdlError } from "../pfdl-errors";

export class PosixBatch extends PfdlBatch {
  public static CreatePosixBatch = async (
    root: PfdlRoot,
    batchName: string,
  ): Promise<PfdlBatch> => {
    return new PosixBatch(root, batchName);
  };

  protected batchPath(name?: string): string {
    if (name) return join(this._root.root, this._batchName, name);
    else return join(this._root.root, this._batchName);
  }

  protected async loadContent(name: string): Promise<Buffer> {
    return await readFile(this.batchPath(name));
  }

  public async loadAndCheckEntries(): Promise<BatchDirent[]> {
    const entries = await readdir(this.batchPath(), { withFileTypes: true });

    const entryNotPlains: string[] = [];

    for (const e of entries) {
      if (!e.isFile()) entryNotPlains.push(e.name);
    }

    if (entryNotPlains.length > 0)
      throw new PfdlError(
        "Finding artifacts",
        entryNotPlains.map((e) => ({
          message:
            "PfdlArtifact is not a plain object (e.g. is a sub-directory or named pipe)",
          root: this._root.root,
          batch: this._batchName,
          artifact: e,
        })),
      );

    return entries;
  }

  public async createArtifact(
    batch: PfdlBatch,
    name: string,
    artifactPath: string,
  ): Promise<PfdlArtifact> {
    return PosixArtifact.CreatePosixArtifact(batch, name, artifactPath);
  }
}
