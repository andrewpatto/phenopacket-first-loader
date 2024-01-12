import { Batch } from "./batch";

export type ChecksumAlgorithms = "MD5" | "AWS-ETAG";


export class Artifact {
  private _checksums: Map<ChecksumAlgorithms,string> = new Map<ChecksumAlgorithms,string>();
  constructor(
    private _batch: Batch,
    private _name: string
  ) {}

  public get batch(): Batch {
    return this._batch;
  }

  public get name(): string {
    return this._name;
  }

  public setChecksum(alg: ChecksumAlgorithms, value: string) {
    this._checksums.set(alg, value);
  }

  public getChecksum(alg: ChecksumAlgorithms) {
    return this._checksums.get(alg);
  }
}
