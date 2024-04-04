import { PfdlBatch } from "../pfdl-batch";
import { createHash } from "node:crypto";
import {
  GetObjectCommand,
  HeadObjectCommand,
  S3Client,
} from "@aws-sdk/client-s3";
import { isInteger } from "lodash";
import { PfdlArtifact } from "../pfdl-artifact";
import { CONTENT_LIMIT_BYTES } from "../pfdl-artifact-common";

export class S3Artifact extends PfdlArtifact {
  /**
   * Create an artifact object representing a file in an S3 bucket.
   *
   * @param batch the batch the artifact came in
   * @param name the name of the artifact
   * @param bucket the S3 bucket name
   * @param key the S3 object key
   * @returns
   */
  public static CreateS3Artifact = async (
    batch: PfdlBatch,
    name: string,
    bucket: string,
    key: string,
  ): Promise<PfdlArtifact> => {
    const s3Client = new S3Client({});

    const firstHeadOutput = await s3Client.send(
      new HeadObjectCommand({
        Bucket: bucket,
        Key: key,
      }),
    );

    if (!isInteger(firstHeadOutput.ContentLength)) {
      throw new Error("Did not get size from S3 HEAD");
    }

    const size: number = firstHeadOutput.ContentLength!;

    let content: Buffer | undefined = undefined;

    // The entity tag is a hash of the object. The ETag reflects changes only
    // to the contents of an object, not its metadata. The ETag may or may
    // not be an MD5 digest of the object data. Whether or not it is depends
    // on how the object was created and how it is encrypted as described below:
    // Objects created by the PUT Object, POST Object, or Copy operation,
    // or through the AWS Management Console, and are encrypted by SSE-S3
    // or plaintext, have ETags that are an MD5 digest of their object data.
    // Objects created by the PUT Object, POST Object, or Copy operation,
    // or through the AWS Management Console, and are encrypted by SSE-C
    // or SSE-KMS, have ETags that are not an MD5 digest of their object data.
    // If an object is created by either the Multipart Upload or Part Copy
    // operation, the ETag is not an MD5 digest, regardless of the method
    // of encryption. If an object is larger than 16 MB, the AWS Management
    // Console will upload or copy that object as a Multipart Upload, and
    // therefore the ETag will not be an MD5 digest.
    let md5: string | undefined = undefined;

    let etag5mib: string | undefined = undefined;
    let etag8mib: string | undefined = undefined;
    let etag64mib: string | undefined = undefined;

    if (size < CONTENT_LIMIT_BYTES) {
      const getOutput = await s3Client.send(
        new GetObjectCommand({
          Bucket: bucket,
          Key: key,
        }),
      );

      if (!getOutput.Body) throw new Error("Could not get S3 content");

      content = Buffer.from(await getOutput.Body.transformToByteArray());

      md5 = createHash("md5").update(content).digest("hex");
    } else {
      const partHeadOutput = await s3Client.send(
        new HeadObjectCommand({
          Bucket: bucket,
          Key: key,
          // for anything that we aren't loading the entire content -
          // we need to know whether this S3 object was multi-part uploaded so we can work
          // out how well we can trust the e-tags
          PartNumber: 1,
        }),
      );

      if (partHeadOutput.PartsCount) {
        // we cannot deduce the real MD5

        // but we can set the etag if the conditions are right
        if (partHeadOutput.ETag) {
          if (
            partHeadOutput.ContentLength === 5 * 1048576 &&
            !partHeadOutput.ETag.endsWith("-1")
          )
            etag5mib = partHeadOutput.ETag.slice(1, -1);
          if (
            partHeadOutput.ContentLength === 8 * 1048576 &&
            !partHeadOutput.ETag.endsWith("-1")
          )
            etag8mib = partHeadOutput.ETag.slice(1, -1);
          if (
            partHeadOutput.ContentLength === 64 * 1048576 &&
            !partHeadOutput.ETag.endsWith("-1")
          )
            etag64mib = partHeadOutput.ETag.slice(1, -1);
        }
      } else {
        if (
          partHeadOutput.ETag &&
          partHeadOutput.ServerSideEncryption == "AES256"
        )
          md5 = partHeadOutput.ETag.slice(1, -1);
      }
    }

    const a = new S3Artifact(bucket, key, batch, name, size, content);

    // if we managed to find an MD5 then lets set it
    if (md5) a.setChecksum("md5", md5);

    // if we managed to find an etag with "standard" part size then lets set it
    if (etag5mib) a.setChecksum("aws-etag-5mib", etag5mib);
    if (etag8mib) a.setChecksum("aws-etag-8mib", etag8mib);
    if (etag64mib) a.setChecksum("aws-etag-64mib", etag64mib);

    return a;
  };

  protected constructor(
    private s3Bucket: string,
    private s3Key: string,
    _batch: PfdlBatch,
    _name: string,
    _size: number,
    _content?: Buffer,
  ) {
    super(_batch, _name, _size, _content);
  }

  get uri(): string {
    return `s3://${this.s3Bucket}/${this.s3Key}`;
  }
}
