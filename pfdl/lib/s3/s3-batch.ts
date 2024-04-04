import { join } from "node:path/posix";
import { BatchDirent, PfdlBatch } from "../pfdl-batch";
import { GetObjectCommand, S3Client } from "@aws-sdk/client-s3";
import { S3Root } from "./s3-root";
import { listFolder } from "./s3-helpers";
import { PfdlArtifact } from "../pfdl-artifact";
import { S3Artifact } from "./s3-artifact";
import { PfdlError } from "../pfdl-errors";

export class S3Batch extends PfdlBatch {
  public static CreateS3Batch = async (
    root: S3Root,
    batchName: string,
  ): Promise<PfdlBatch> => {
    return new S3Batch(root, batchName);
  };

  /**
   * Create paths within this batch.
   *
   * @param name if present return a path of an object with this name in the batch, otherwise the overall batch path
   * @protected
   */
  protected batchPath(name?: string): string {
    // note we are dealing with S3 paths - which are posix style paths
    // so the join here is explicitly the posix join
    if (name) return join((this.root as S3Root).key, this._batchName, name);
    else return join((this.root as S3Root).key, this._batchName) + "/";
  }

  /**
   * For the given artifact name return the content in a Buffer.
   *
   * @param name
   * @protected
   */
  protected async loadContent(name: string): Promise<Buffer> {
    const s3Client = new S3Client({});

    const get = await s3Client.send(
      new GetObjectCommand({
        Bucket: (this.root as S3Root).bucket,
        Key: this.batchPath(name),
      }),
    );

    if (!get.Body) throw new Error("Could not get S3 content");

    return Buffer.from(await get.Body.transformToByteArray());
  }

  /**
   * Return the list of artifacts in the batch.
   */
  public async loadAndCheckEntries(): Promise<BatchDirent[]> {
    const entries: BatchDirent[] = [];

    for await (const subKey of listFolder(
      (this.root as S3Root).bucket,
      this.batchPath(),
    )) {
      entries.push(subKey);
    }

    const entryNotPlains: string[] = [];

    for (const e of entries) {
      if (!e.isFile()) entryNotPlains.push(e.name);
    }

    if (entryNotPlains.length > 0)
      throw new PfdlError(
        "Finding artifacts",
        entryNotPlains.map((e) => ({
          message:
            "Entry is not a plain object (e.g. is a sub-directory or named pipe)",
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
    return S3Artifact.CreateS3Artifact(
      batch,
      name,
      (this._root as S3Root).bucket,
      artifactPath,
    );
  }
}
