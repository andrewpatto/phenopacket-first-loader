import { PfdlBatch } from "./pfdl-batch";
import { resolveContentToConsentpacket } from "./filetypes/consentpacket-load-helper";
import { resolveContentToPhenopacket } from "./filetypes/phenopackets-load-helper";
import { ChecksumAlgorithms } from "./pfdl-artifact-common";

/**
 * An artifact is an object stored in a filesystem or cloud storage.
 */
export abstract class PfdlArtifact {
  protected _checksums: Map<ChecksumAlgorithms, string> = new Map<
    ChecksumAlgorithms,
    string
  >();

  protected constructor(
    private _batch: PfdlBatch,
    private _name: string,
    private _size: number,
    private _content?: Buffer,
  ) {}

  public get batch(): PfdlBatch {
    return this._batch;
  }

  public get name(): string {
    return this._name;
  }

  public get size(): number {
    return this._size;
  }

  public abstract get uri(): string;

  public setChecksum(alg: ChecksumAlgorithms, value: string) {
    if (this._checksums.has(alg)) {
      const already = this._checksums.get(alg);
      if (already != value)
        throw new Error(
          `Checksum mismatch ${value} (expected) v ${already} (actual)`,
        );
    }
    this._checksums.set(alg, value);
  }

  public getChecksum(alg: ChecksumAlgorithms) {
    return this._checksums.get(alg);
  }

  public getChecksums(): Record<string, string> {
    return Object.fromEntries(this._checksums);
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
}
