import { PfdlBatch } from "./pfdl-batch";

export abstract class PfdlRoot {
  /**
   * The URL/URI/path that describes the location of this root.
   * e.g. "s3://bucket/key" or "/Users/person/place"
   */
  public abstract get root(): string;

  /**
   * The set of entries located at the root describing
   * an "upload" batch.
   */
  public abstract batches(): Promise<PfdlBatch[]>;
}
