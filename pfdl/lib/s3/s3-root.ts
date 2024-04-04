import { PfdlBatch } from "../pfdl-batch";
import { PfdlRoot } from "../pfdl-root";
import { S3Batch } from "./s3-batch";
import { listSubFolders } from "./s3-helpers";

export class S3Root extends PfdlRoot {
  private readonly _bucket: string;
  private readonly _key: string;

  private _batches?: PfdlBatch[] = undefined;

  constructor(private s3RootUri: string) {
    super();

    const url = new URL(s3RootUri);

    if (url.protocol !== "s3:")
      throw new Error(
        `S3Root constructor() must be passed a valid S3 URI - instead it got ${url.protocol} as a protocol`,
      );

    // not a proper check but might stop some invalid bucket name mixups
    if (!url.hostname || url.hostname.length < 3)
      throw new Error(
        `S3Root constructor() must be passed a valid S3 URI - instead it got ${url.host} as a possible bucket name`,
      );

    this._bucket = url.hostname;
    // S3 keys do not actually start with a leading / - that we will get from the url.pathname - so we remove
    this._key = url.pathname.substring(1);

    // for consistency throughout our own S3 code - we always refer to S3 folders as keys with trailing slashes
    // (this is also consistent with how some of the AWS s3 code works)
    if (!this._key.endsWith("/")) this._key = this._key + "/";
  }

  public get root(): string {
    return this.s3RootUri;
  }

  public get bucket(): string {
    return this._bucket;
  }

  public get key(): string {
    return this._key;
  }

  /**
   * Return the batches found under this root in
   * no particular order. The result
   * of this operation is cached.
   *
   * @returns
   */
  public async batches(): Promise<PfdlBatch[]> {
    if (this._batches) return this._batches;

    const result: PfdlBatch[] = [];
    for await (const subKey of listSubFolders(this.bucket, this.key)) {
      // our batch names need to be consistent across S3 and posix
      // so we define a batch name to be *just* the folder name (with no trailing slash and no leading key prefix)
      // (our s3 batch class will need to add trailing slashes back in again)
      const batchName = subKey.slice(this.key.length, -1);

      result.push(await S3Batch.CreateS3Batch(this, batchName));
    }
    this._batches = result;

    return this._batches;
  }
}
