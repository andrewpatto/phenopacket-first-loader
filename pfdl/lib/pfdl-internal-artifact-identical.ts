import { PfdlArtifact } from "./pfdl-artifact";

/**
 * The PfdlInternalArtifactIdentical is a type that is used
 * to represent a set of identical artifacts with the same name
 * and from batches with the same name. That is,
 * it represents the concept of the "same" object that
 * occurs in two different roots.
 *
 * So something like
 * S3 bucket -> dataset -> batch "2020-01" -> object "me.fastq.gz"
 * and
 * Posix dir -> dataset -> batch "2020-01" -> file "me.fastq.gz"
 *
 * and where both have the same MD5 and size.
 *
 * It is possible for "me.fastq.gz" to appear in an entirely
 * different batch (say with name "2023"). That would NOT be
 * represented here - EVEN IF IT HAS THE SAME MD5 and SIZE.
 *
 * It is an internal
 * structure so should not be relied on externally and is
 * subject to change.
 */
export type PfdlInternalArtifactIdentical = {
  artifacts: PfdlArtifact[];
};
