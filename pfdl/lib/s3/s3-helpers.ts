import {
  ListObjectsV2Command,
  ListObjectsV2Output,
  S3Client,
} from "@aws-sdk/client-s3";
import { BatchDirent } from "../pfdl-batch";
import { relative } from "node:path/posix";

export async function* listSubFolders(bucket: string, rootFolder: string) {
  if (!rootFolder.endsWith("/"))
    throw new Error("listSubFolders can only be passed a rootFolder with a trailing slash");

  const s3Client = new S3Client({});

  let continuationToken: string | undefined = undefined;

  do {
    const listObjectsOutput: ListObjectsV2Output = await s3Client.send(
        new ListObjectsV2Command({
          Bucket: bucket,
          Prefix: rootFolder,
          Delimiter: "/",
          ContinuationToken: continuationToken,
        }),
    );

    if (listObjectsOutput.CommonPrefixes) {
      for (const common of listObjectsOutput.CommonPrefixes) {
        if (common.Prefix)
          yield common.Prefix!;
      }
    }

    continuationToken = listObjectsOutput.NextContinuationToken;
  } while (continuationToken);
}


/**
 * Return pseudo DirEnt structures for the objects that are contained
 * within a "folder" in S3.
 *
 * @param bucket
 * @param rootFolder
 */
export async function* listFolder(bucket: string, rootFolder: string) {
  if (!rootFolder.endsWith("/"))
    throw new Error("listFolder can only be passed a rootFolder with a trailing slash");

  const s3Client = new S3Client({});

  let continuationToken: string | undefined = undefined;

  do {
    const listObjectsOutput: ListObjectsV2Output = await s3Client.send(
      new ListObjectsV2Command({
        Bucket: bucket,
        Prefix: rootFolder,
        ContinuationToken: continuationToken,
      }),
    );

    if (listObjectsOutput.Contents) {
      for (const objContent of listObjectsOutput.Contents) {
        const key = objContent.Key;

        if (!key) continue;

        const name = relative(rootFolder, key);

        // when we make folders in the S3 console - we end up with entries corresponding
        // to just the rootFolder itself
        if (!name)
          continue;

        yield {
          isFile(): boolean {
            return true;
          },
          name: name,
        } as BatchDirent;
      }
    }

    continuationToken = listObjectsOutput.NextContinuationToken;
  } while (continuationToken);
}
