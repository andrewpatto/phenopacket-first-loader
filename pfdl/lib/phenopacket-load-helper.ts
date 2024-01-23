import { org } from "../phenopackets/phenopackets";
import isEmpty from "lodash/isEmpty";

// NOTE there is no particular marker in the phenopacket to say what type of packet it is
// so we need to do some sniffing of content

// NOTE the decoded protobuf objects have arrays (albeit empty) - even where there was no data
// present (i.e pp.subject will be [] even if the protobuf had no "subject")
// so we need to always be careful what sort of "is null" etc tests we are making -
// so mainly we use lodash isEmpty and we look for where content *is* present

/**
 * Test if the given phenopacket could actually be an individual.
 *
 * @param pp
 */
function matchesIndividual(pp: org.phenopackets.schema.v2.Phenopacket) {
  if (pp) {
    // FROM Java implementation
    // // Top-level fields unique to a phenopacket, both v1 and v2. If we see this field, the content must be a phenopacket.
    //     private static final List<String> PHENOPACKET_FIELDS = List.of(
    //             "subject", "phenotypicFeatures", "measurements", "interpretations", "medicalActions",
    //             "biosamples", "genes", "variants", "diseases"
    //     );
    if (
      !isEmpty(pp.subject) ||
      !isEmpty(pp.phenotypicFeatures) ||
      !isEmpty(pp.measurements) ||
      !isEmpty(pp.interpretations) ||
      !isEmpty(pp.medicalActions) ||
      !isEmpty(pp.biosamples) ||
      !isEmpty(pp.diseases)
    )
      return true;
  }

  return false;
}

/**
 * Test if the given phenopacket could actually be a family.
 *
 * @param pf
 */
function matchesFamily(pf: org.phenopackets.schema.v2.Family) {
  if (pf) {
    if (!isEmpty(pf.proband) || !isEmpty(pf.relatives) || !isEmpty(pf.pedigree))
      return true;
  }

  return false;
}

/**
 * Test if the given phenopacket could actually be a cohort.
 *
 * @param pc
 */
function matchesCohort(pc: org.phenopackets.schema.v2.Cohort) {
  if (pc) {
    if (!isEmpty(pc.description) || !isEmpty(pc.members)) return true;
  }

  return false;
}

/**
 * Given content as a Buffer, test the content for validity and if
 * suitable return as a decoded Phenopacket object. If not a valid phenopacket
 * object, return null.
 *
 * @param content
 */
export async function resolveContentToPhenopacket(
  content: Buffer
): Promise<
  | org.phenopackets.schema.v2.Phenopacket
  | org.phenopackets.schema.v2.Family
  // NOTE until we have a strong use case - we are disabling cohort phenopackets
  // we just really don't want to think about it as individuals and families are
  // mainly what we want to support
  // | org.phenopackets.schema.v2.Cohort
  | null
> {
  // primarily we will be seeing phenopackets in JSON representation so we try
  // that way
  try {
    const contentString = content.toString("utf-8");
    const jsonParsed = JSON.parse(contentString);

    try {
      const pp = org.phenopackets.schema.v2.Phenopacket.fromObject(jsonParsed);

      if (matchesIndividual(pp)) {
        // console.log("INDIVIDUAL");
        // console.log(JSON.stringify(pp.toJSON()));

        return pp;
      }
    } catch (ex) {}

    try {
      const pf = org.phenopackets.schema.v2.Family.fromObject(jsonParsed);

      if (matchesFamily(pf)) {
        // console.log("FAMILY");
        // console.log(JSON.stringify(pf.toJSON()));
        return pf;
      }
    } catch (ex) {}

    try {
      const pc = org.phenopackets.schema.v2.Cohort.fromObject(jsonParsed);

      if (matchesCohort(pc)) {
        throw new Error("Cohort phenopackets are currently not supported");
        // return pc;
      }
    } catch (ex) {}
  } catch (e) {}

  // possibly the phenopacket content is represented in binary protobuf format
  // so we also try to decode that way
  try {
    try {
      const pp = org.phenopackets.schema.v2.Phenopacket.decode(content);

      if (matchesIndividual(pp)) {
        return pp;
      }
    } catch (ex) {}

    try {
      const pf = org.phenopackets.schema.v2.Family.decode(content);

      if (matchesFamily(pf)) {
        return pf;
      }
    } catch (ex) {}

    try {
      const pc = org.phenopackets.schema.v2.Cohort.decode(content);

      if (matchesCohort(pc)) {
        throw new Error("Cohort phenopackets are currently not supported");
        // return pc;
      }
    } catch (ex) {}
  } catch (e) {}

  return null;
}
