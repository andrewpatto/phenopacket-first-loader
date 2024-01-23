import { stat, readFile } from "node:fs/promises";
import { Batch } from "./batch";
import { resolveContentToPhenopacket } from "./phenopacket-load-helper";

export type ChecksumAlgorithms = "MD5" | "AWS-ETAG";

// we limit ourselves to only ever loading the content for objects under this size
// the only objects *we* ever need to read are things like manifests and phenopackets which
// should be under this size
const CONTENT_LIMIT_BYTES = 128 * 1024;

/**
 * An artifact is an object from a filesystem or cloud storage.
 */
export class Artifact {
  /**
   * Create an artifact object.
   *
   * @param batch the batch the artifact came in
   * @param name the name of the artifact
   * @param fullArtifactPath a filesystem path that can be used to access the artifact
   * @returns
   */
  public static CreateArtifact = async (
    batch: Batch,
    name: string,
    fullArtifactPath: string
  ) => {
    const stats = await stat(fullArtifactPath);

    return new Artifact(
      batch,
      name,
      stats.size,
      stats.size < CONTENT_LIMIT_BYTES
        ? await readFile(fullArtifactPath, {
            encoding: null,
          }) /* explicit encoding null = return as Buffer */
        : undefined
    );
  };

  private _checksums: Map<ChecksumAlgorithms, string> = new Map<
    ChecksumAlgorithms,
    string
  >();

  private constructor(
    private _batch: Batch,
    private _name: string,
    private _size: number,
    private _content?: Buffer
  ) {}

  public get batch(): Batch {
    return this._batch;
  }

  public get name(): string {
    return this._name;
  }

  public get size(): number {
    return this._size;
  }

  public setChecksum(alg: ChecksumAlgorithms, value: string) {
    this._checksums.set(alg, value);
  }

  public getChecksum(alg: ChecksumAlgorithms) {
    return this._checksums.get(alg);
  }

  /**
   * Returns the artifact content as a phenopacket (if it is one) or null if not.
   *
   * @returns a phenopacket instance if this artifact is a phenopacket
   */
  public getContentAsPhenopacket() {
    // if the file was too large to load its content we are presuming it is not a phenopacket
    // obviously - we might one day encounter a super large phenopacket - but everything
    // then will fail in a sane way (it will be as if the phenopacket is not present) and
    // we can revisit our limits
    if (!this._content) return null;

    return resolveContentToPhenopacket(this._content);
  }

  public toJSON() {
    return {
      name: this.name,
      size: this.size
    }
  }
}
