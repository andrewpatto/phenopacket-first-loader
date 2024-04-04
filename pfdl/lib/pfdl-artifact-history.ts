import {PfdlArtifact} from "./pfdl-artifact";
import {PfdlInternalArtifactIdentical} from "./pfdl-internal-artifact-identical";

/**
 */
export type PfdlArtifactHistory = {
    // all the artifacts living with the same "name"
    // but scattered over different batch names
    // note that the ArtifactGroup holds artifacts that
    // occur with the same "name" for the same "batch name"
    // (these are therefore the same artifact - just from different root systems)
    reverseSortedByBatchName: PfdlInternalArtifactIdentical[];
};
