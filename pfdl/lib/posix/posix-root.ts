import { isAbsolute } from "node:path";
import { readdir } from "node:fs/promises";
import { PfdlBatch } from "../pfdl-batch";
import { PosixBatch } from "./posix-batch";
import { PfdlRoot } from "../pfdl-root";

export class PosixRoot extends PfdlRoot {
  private _batches?: PfdlBatch[] = undefined;

  constructor(private absoluteRootFolder: string) {
    super();

    // note: this is already checked before constructing Root
    // objects so that proper error messages can be returned - so
    // this is just a double-check (hence throwing an Error rather
    // than returning an error data structure)
    if (!isAbsolute(absoluteRootFolder))
      throw new Error("Root folder must be absolute");
  }

  public get root(): string {
    return this.absoluteRootFolder;
  }

  /**
   * Return the batches found under this root in
   * no particular (or consistent) order. The result
   * of this operation is cached.
   *
   * @returns
   */
  public async batches(): Promise<PfdlBatch[]> {
    if (this._batches) return this._batches;

    // find all the directories
    const subs = (
      await readdir(this.absoluteRootFolder, { withFileTypes: true })
    ).filter((d) => d.isDirectory());

    // we require them to be ordered for consistency of results
    subs.sort((d1, d2) => d1.name.localeCompare(d2.name));

    // turn them into Batch objects
    this._batches = await Promise.all(
      subs.map((d) => PosixBatch.CreatePosixBatch(this, d.name)),
    );

    return this._batches;
  }
}
