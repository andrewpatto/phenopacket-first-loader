import { isAbsolute } from "node:path";
import { Root } from "./root";
import { readFile, readdir } from "node:fs/promises";
import { join } from "node:path";

export type StructureReport = {
    errorMessage?: string;


}

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

  public async checkStructure() {
    const entries = await readdir(this.batchPath(), { withFileTypes: true });

    for (const e of entries) {
      if (!e.isFile)
        throw new Error(`Batch entry ${e.name} is not a plain file object`);
    }

    let md5s: { [file: string]: string } | undefined = undefined;

    for (const e of entries)
        if (e.name === "md5sums.txt")
            md5s = this.md5SumsToObject((await this.loadContent(e.name)).toString("ascii"));

    if (!md5s)
        throw new Error("No md5sums.txt");

    const names = new Set<string>(entries.map((e) => e.name));

    for(const [md5name, md5sum] of Object.entries(md5s)) {
        if (!names.has(md5name))
            throw new Error(`Batch is missing files ${md5name} that was declared in the md5sums.txt`);
    }

  }

  private md5SumsToObject(content: string) {
    // For each file, ‘md5sum’ outputs by default, the MD5 md5Checksum,
    // a space, a flag indicating binary or text input mode, and the
    // file name. Binary mode is indicated with ‘*’, text mode
    // with ‘ ’ (space). Binary mode is the default on systems where
    // it’s significant, otherwise text mode is the default.
    let sums: { [file: string]: string } = {};

    for (const line of content.split("\n")) {
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

      sums[file] = checksum;
    }

    return sums;
  }
}
