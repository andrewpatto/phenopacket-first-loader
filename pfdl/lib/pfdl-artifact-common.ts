// see https://www.iana.org/assignments/hash-function-text-names/hash-function-text-names.txt
// and also ones we have made up
export type ChecksumAlgorithms =
  | "md5"
  | "aws-etag-5mib"
  | "aws-etag-8mib"
  | "aws-etag-64mib"
  | "quickxor";

// we limit ourselves to only ever loading the content for objects under this size
// the only objects *we* ever need to read are things like manifests and phenopackets which
// should be under this size

// note: for S3 we currently do some "part" querying - which will fail if parts are smaller than this
// https://docs.aws.amazon.com/AmazonS3/latest/userguide/qfacts.html
// which should be fine - the minimum part size is 5MiB
export const CONTENT_LIMIT_BYTES = 128 * 1024;
