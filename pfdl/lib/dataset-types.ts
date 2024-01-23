import { Artifact } from "./artifact";
import { org } from "../phenopackets/phenopackets";

export type Identifiable = {
  externalIdentifiers: { system: string; value: string }[];
};

export type Consentable = {
  consent?: any;
};

export type Biosample = Identifiable &
  Consentable &
  Pick<
    org.phenopackets.schema.v2.core.IBiosample,
    | "derivedFromId"
    | "description"
    | "sampledTissue"
    | "sampleType"
    | "phenotypicFeatures"
    | "measurements"
    | "taxonomy"
    | "timeOfCollection"
  > & {
    artifacts: Artifact[];
  };

export type Individual = Identifiable &
  Consentable &
  Omit<
    org.phenopackets.schema.v2.core.IIndividual,
    "id" | "alternateIds"
  > & {
    biosamples: Biosample[];
  };

export type Family = Identifiable &
  Consentable & {
    individuals: Individual[];
  };

export type Dataset = Identifiable &
  Consentable & {
    families: Family[];
  };
