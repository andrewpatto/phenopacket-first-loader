import { isAbsolute, join } from "node:path";
import { readdir, access } from "node:fs/promises";
import { Batch } from "./batch";

export class Root {
  private _batches?: Batch[] = undefined;

  constructor(private absoluteRootFolder: string) {
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
  public async batches(): Promise<Batch[]> {
    if (this._batches)
      return this._batches;

    // find all the directories
    const subs = (
      await readdir(this.absoluteRootFolder, { withFileTypes: true })
    )
      .filter((d) => d.isDirectory());

    // we require them to be ordered for consistency of results
    subs.sort((d1, d2) => d1.name.localeCompare(d2.name));

    // turn them into Batch objects
    this._batches = subs.map((d) => new Batch(this, d.name));

    return this._batches;
  }
}
