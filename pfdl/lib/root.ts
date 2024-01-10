import { isAbsolute, join } from "node:path";
import { readdir, access } from "fs/promises";
import { Batch } from "./batch.ts";

export class Root {
  private _batches?: Batch[] = undefined;

  constructor(private absoluteRootFolder: string) {
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

    const subs = (
      await readdir(this.absoluteRootFolder, { withFileTypes: true })
    )
      .filter((d) => d.isDirectory())
      .toSorted((d1, d2) => d1.name.localeCompare(d2.name))
      .map((d) => new Batch(this, d.name))

    this._batches = subs;

    return this._batches;
  }
}
