import { stat, readFile } from "node:fs/promises";
import { Batch } from "./batch";
import { isAbsolute } from "node:path";
import { resolveContentToConsentpacket } from "./filetypes/consentpacket-load-helper";
import { resolveContentToPhenopacket } from "./filetypes/phenopackets-load-helper";
import { createHash } from "node:crypto";

export type ChecksumAlgorithms = "MD5" | "AWS-ETAG";

// we limit ourselves to only ever loading the content for objects under this size
// the only objects *we* ever need to read are things like manifests and phenopackets which
// should be under this size
const CONTENT_LIMIT_BYTES = 128 * 1024;

/**
 * An artifact is an object from a filesystem or cloud storage.
 */
export abstract class Artifact {
  protected _checksums: Map<ChecksumAlgorithms, string> = new Map<
    ChecksumAlgorithms,
    string
  >();

  protected constructor(
    private _batch: Batch,
    private _name: string,
    private _size: number,
    private _content?: Buffer,
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
    if (this._checksums.has(alg)) {
      const already = this._checksums.get(alg);
      if (already != value)
        throw new Error(`Checksum mismatch ${value} (expected) v ${already} (actual)`);
    }
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

  public getContentAsConsentpacket() {
    // if the file was too large to load its content we are presuming it is not a consentpacket
    if (!this._content) return null;

    return resolveContentToConsentpacket(this._content);
  }

  public toJSON(): any {
    return {
      size: this.size,
      checksums: Object.fromEntries(this._checksums),
    };
  }
}

export class FilesystemArtifact extends Artifact {
  /**
   * Create an artifact object representing a file in a POSIX file system.
   *
   * @param batch the batch the artifact came in
   * @param name the name of the artifact
   * @param filesystemPath an absolute filesystem path that can be used to access the artifact
   * @returns
   */
  public static CreateFilesystemArtifact = async (
    batch: Batch,
    name: string,
    filesystemPath: string,
  ): Promise<Artifact> => {
    // our loader should already have made our paths absolute but just as a check
    // the rationale here is that a "dataset" must be something that is not dependent on the current directory
    // of the person using it
    if (!isAbsolute(filesystemPath))
      throw new Error(
        `At the point of creating an artifact for '${filesystemPath}' the loader should be using absolute paths`,
      );

    const stats = await stat(filesystemPath);

    let content: Buffer | undefined = undefined;
    let md5: string | undefined = undefined;

    if (stats.size < CONTENT_LIMIT_BYTES) {
      /* explicit encoding null = return as Buffer */
      content = await readFile(filesystemPath, {
        encoding: null,
      });

      md5 = createHash("md5").update(content).digest("hex");
    }

    return new FilesystemArtifact(
      filesystemPath,
      batch,
      name,
      stats.size,
      content,
      md5,
    );
  };

  protected constructor(
    private fileSystemPath: string,
    _batch: Batch,
    _name: string,
    _size: number,
    _content?: Buffer,
    _contentMd5?: string,
  ) {
    super(_batch, _name, _size, _content);

    if (_content && _contentMd5) this.setChecksum("MD5", _contentMd5);
  }

  public toJSON() {
    return {
      uri: `file://${this.fileSystemPath}`,
      size: this.size,
      checksums: Object.fromEntries(this._checksums),
    };
  }
}

export class S3Artifact extends Artifact {
  /**
   * Create an artifact object representing a file in an S3 bucket.
   *
   * @param batch the batch the artifact came in
   * @param name the name of the artifact
   * @param bucket an S3 bucket name
   * @param key the S3 object key
   * @returns
   */
  public static CreateS3Artifact = async (
    batch: Batch,
    name: string,
    bucket: string,
    key: string,
  ): Promise<Artifact> => {
    // TODO

    return new S3Artifact(batch, name, 0, undefined);
  };

  public toJSON() {
    return {
      size: this.size,
      checksums: Object.fromEntries(this._checksums),
    };
  }
}
