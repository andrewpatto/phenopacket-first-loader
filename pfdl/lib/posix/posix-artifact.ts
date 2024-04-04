import { readFile, stat } from "node:fs/promises";
import { PfdlBatch } from "../pfdl-batch";
import { isAbsolute } from "node:path";
import { createHash } from "node:crypto";
import { PfdlArtifact } from "../pfdl-artifact";
import { CONTENT_LIMIT_BYTES } from "../pfdl-artifact-common";

export class PosixArtifact extends PfdlArtifact {
  /**
   * Create an artifact object representing a file in a POSIX file system.
   *
   * @param batch the batch the artifact came in
   * @param name the name of the artifact
   * @param filesystemPath an absolute filesystem path that can be used to access the artifact
   * @returns
   */
  public static CreatePosixArtifact = async (
    batch: PfdlBatch,
    name: string,
    filesystemPath: string,
  ): Promise<PfdlArtifact> => {
    // our loader should already have made our paths absolute but just as a check
    // the rationale here is that a "dataset" must be something that is not dependent on the current directory
    // of the person using it so we cannot use relative paths for the actual objects
    if (!isAbsolute(filesystemPath))
      throw new Error(
        `At the point of creating an artifact for '${filesystemPath}' the loader must be using absolute POSIX paths`,
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

    return new PosixArtifact(
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
    _batch: PfdlBatch,
    _name: string,
    _size: number,
    _content?: Buffer,
    _contentMd5?: string,
  ) {
    super(_batch, _name, _size, _content);

    if (_content && _contentMd5) this.setChecksum("md5", _contentMd5);
  }

  get uri(): string {
    return `file://${this.fileSystemPath}`;
  }
}
