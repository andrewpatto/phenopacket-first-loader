import { Artifact } from "./artifact";
import { org} from "./filetypes/phenopackets/phenopackets";

// take the phenopackets interface definitions and make into extended types for our loader


/**
 * Merge keys of U into T, overriding value types with those in U.
 */
type Override<T, U extends Partial<Record<keyof T, unknown>>> = FinalType<Omit<T, keyof U> & U>;

/**
 * Make a type assembled from several types/utilities more readable.
 * (e.g. the type will be shown as the final resulting type instead of as a bunch of type utils wrapping the initial type).
 */
type FinalType<T> = T extends infer U ? { [K in keyof U]: U[K] } : never;

export type PfdlConsentable = {
  consent?: any;
};

export type PfdlFile = Omit<
    org.phenopackets.schema.v2.core.File,
    "uri"
> & {
  name: string;
  artifact: Artifact;
}

export type PfdlBiosample = Override<org.phenopackets.schema.v2.core.IBiosample, {
  // id?: string | null;
  // individualId?: string | null;
  // derivedFromId?: string | null;
  // description?: string | null;
  // sampledTissue?: org.phenopackets.schema.v2.core.IOntologyClass | null;
  // sampleType?: org.phenopackets.schema.v2.core.IOntologyClass | null;
  // phenotypicFeatures?: org.phenopackets.schema.v2.core.IPhenotypicFeature[] | null;
  // measurements?: org.phenopackets.schema.v2.core.IMeasurement[] | null;
  // taxonomy?: org.phenopackets.schema.v2.core.IOntologyClass | null;
  // timeOfCollection?: org.phenopackets.schema.v2.core.ITimeElement | null;
  // histologicalDiagnosis?: org.phenopackets.schema.v2.core.IOntologyClass | null;
  // tumorProgression?: org.phenopackets.schema.v2.core.IOntologyClass | null;
  // tumorGrade?: org.phenopackets.schema.v2.core.IOntologyClass | null;
  // pathologicalStage?: org.phenopackets.schema.v2.core.IOntologyClass | null;
  // pathologicalTnmFinding?: org.phenopackets.schema.v2.core.IOntologyClass[] | null;
  // diagnosticMarkers?: org.phenopackets.schema.v2.core.IOntologyClass[] | null;
  // procedure?: org.phenopackets.schema.v2.core.IProcedure | null;
  files?: PfdlFile[] | null
  // materialSample?: org.phenopackets.schema.v2.core.IOntologyClass | null;
  // sampleProcessing?: org.phenopackets.schema.v2.core.IOntologyClass | null;
  // sampleStorage?: org.phenopackets.schema.v2.core.IOntologyClass | null;
}> & PfdlConsentable;

export type PfdlIndividual = Override<org.phenopackets.schema.v2.core.IIndividual, {
  // id?: string | null;
  // alternateIds?: string[] | null;
  // dateOfBirth?: google.protobuf.ITimestamp | null;
  // timeAtLastEncounter?: org.phenopackets.schema.v2.core.ITimeElement | null;
  // vitalStatus?: org.phenopackets.schema.v2.core.IVitalStatus | null;
  // sex?: org.phenopackets.schema.v2.core.Sex | null;
  // karyotypicSex?: org.phenopackets.schema.v2.core.KaryotypicSex | null;
  // gender?: org.phenopackets.schema.v2.core.IOntologyClass | null;
  //taxonomy?: org.phenopackets.schema.v2.core.IOntologyClass | null;
  }>;

export type PfdlPhenopacket = Override<org.phenopackets.schema.v2.IPhenopacket, {
  // id?: string | null;
  subject?: PfdlIndividual | null;
  // phenotypicFeatures?: org.phenopackets.schema.v2.core.IPhenotypicFeature[] | null;
  // measurements?: org.phenopackets.schema.v2.core.IMeasurement[] | null;
  biosamples?: PfdlBiosample[] | null;
  // interpretations?: org.phenopackets.schema.v2.core.IInterpretation[] | null;
  // diseases?: org.phenopackets.schema.v2.core.IDisease[] | null;
  // medicalActions?: org.phenopackets.schema.v2.core.IMedicalAction[] | null;
  files?: PfdlFile[] | null;
  // metaData?: org.phenopackets.schema.v2.core.IMetaData | null;
}> & PfdlConsentable;


export type PfdlFamily = Override<org.phenopackets.schema.v2.IFamily, {
  // id?: string | null;
  proband?: PfdlPhenopacket | null;
  relatives?: PfdlPhenopacket[] | null;
  // consanguinousParents?: boolean | null;
  // pedigree?: org.phenopackets.schema.v2.core.IPedigree | null;
  files?: PfdlFile[] | null;
  // metaData?: org.phenopackets.schema.v2.core.IMetaData | null;
}> & PfdlConsentable;

export type PfdlDataset = {
  individuals: PfdlIndividual[];
  families: PfdlFamily[];
} & PfdlConsentable;


/*
Omit<
    org.phenopackets.schema.v2.core.IIndividual,
    "id" | "alternateIds"
  > & {
    biosamples: Biosample[];
  };

 */