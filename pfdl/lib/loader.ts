import { isAbsolute, join } from "node:path";
import { readdir, access } from "fs/promises";
import { Batch } from "./batch";
import { Root } from "./root";

export class Loader {
  private _roots: Root[];
  private _batches?: Map<string, Batch>;

  constructor(absoluteRootFolders: string[]) {
    this._roots = absoluteRootFolders.map((rf) => new Root(rf));
  }

  public async allBatches(): Promise<Map<string, Batch>> {

    if (this._batches)
        return this._batches;
    
    const all: Batch[] = [];
    const allNames: Set<string> = new Set<string>();

    // need to collect the batch names across all the roots
    // and then we will sort/insert them into a map
    for (const r of this._roots) {
      for (const b of await r.batches()) {
        if (allNames.has(b.name))
          throw new Error(`Batch name ${b.name} occurs in two different roots`);

        all.push(b);
        allNames.add(b.name);
      }
    }

    all.sort((b1, b2) => b1.name.localeCompare(b2.name));

    // the javascript Map stores keys in *insertion* order
    // so we want to insert these alphabetically
    const map = new Map<string, Batch>();

    for (const a of all) {
      map.set(a.name, a);
    }

    this._batches = map;

    return this._batches;
  }

  public async checkStructure() {
    // this is the first chance we have to even check if the Root folders exist
    for (const r of this._roots)
      await access(r.root);

    const b = await this.allBatches();

    b.forEach(async (v) => await v.checkStructure());
  }
}
