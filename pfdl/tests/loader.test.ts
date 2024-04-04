import { Loader } from "../lib/pfdl";
import { resolve, join } from "node:path";
import { ErrorReport } from "../lib/common-types";
import "jest-extended";

const examplesFolder = resolve("..", "examples");
const examplesBadFolder = resolve("..", "examples-bad");

// this is not really a unit test - it just helps out that if we are failing our
// E2E test that we get printouts of the error messages
test("basic end to end with printing of error messages if found", async () => {
  const l = new Loader([join(examplesFolder, "test1")]);

  const structure = await l.checkStructure();

  if (structure.state !== "data") {
    fail(JSON.stringify(structure, null, 2));
  } else {
    const r = await l.checkPhenopacketStructure(structure);

    if (r) {
      fail(JSON.stringify(r, null, 2));
    }

    const f = await l.checkPhenopackets(structure);

    if (f.state !== "data") {
      fail(JSON.stringify(f, null, 2));
    }
  }
});

test("basic end to end with single root", async () => {
  const l = new Loader([join(examplesFolder, "test1")]);

  const structure = await l.checkStructure();

  expect(structure.state).toBe("data");

  if (structure.state !== "data") return;

  {
    // the keys are artifact names (i.e. filenames) sorted alphabetically
    const artifactNames = Array.from(structure.artifacts.keys());

    expect(artifactNames).toBeArrayOfSize(18);

    expect(artifactNames[0]).toBe("A.consentpacket.json");
    expect(artifactNames[1]).toBe("A.phenopacket.json");

    // expect the fastqs to be an array - with the latest batch appearing
    // first (i.e batch 2020-02-06 has replaced the items in 2020-01-12)
    expect(artifactNames[11]).toBe("NEXTSEQ_ABCD_PERSONA_TOPUP_R2.fastq.gz");

    {
      // we expect this artifact to have two "historical" entries
      const r2artifactHistory = structure.artifacts.get(artifactNames[11]);
      expect(r2artifactHistory?.reverseSortedByBatchName).toBeArrayOfSize(2);

      // we look at the first historical entry
      {
        const r2artifactIdentical0 =
          r2artifactHistory?.reverseSortedByBatchName[0]!;

        expect(r2artifactIdentical0.artifacts[0].batch.name).toBe("2020-02-06");
        expect(r2artifactIdentical0.artifacts[0].getChecksum("md5")).toBe(
          "d41d8cd98f00b204e9800998ecf8427e",
        );
      }

      // and the second historical entry
      {
        const r2artifactIdentical1 =
          r2artifactHistory?.reverseSortedByBatchName[1]!;

        expect(r2artifactIdentical1.artifacts[0].batch.name).toBe("2020-01-12");
        expect(r2artifactIdentical1.artifacts[0].getChecksum("md5")).toBe(
          "9d5ed678fe57bcca610140957afab571",
        );
      }
    }

    // expect the phenopacket to be a single history entry
    expect(artifactNames[17]).toBe("SIMPSONS.phenopacket.json");

    {
      const simpsonsPhenoArtifactHistory = structure.artifacts.get(artifactNames[17]);

      expect(simpsonsPhenoArtifactHistory?.reverseSortedByBatchName).toBeArrayOfSize(1);
    }
  }

  const r = await l.checkPhenopacketStructure(structure);

  expect(r).toBeNull();

  const f = await l.checkPhenopackets(structure);

  console.log(JSON.stringify(f, null, 2));
});

test("loader root that is not an absolute path is rejected", async () => {
  const l = new Loader([
    join("b-path"),
    join(examplesFolder, "test1"),
    join("..", "a-path"),
  ]);

  const b = await l.checkStructure();

  expect(b.state).toBe("error");
  expect((b as ErrorReport).specific).toBeArrayOfSize(2);

  expect((b as ErrorReport).specific[0].message).toBe(
    "Root path not recognised as 'absolute' path",
  );
  expect((b as ErrorReport).specific[0].root).toBe("../a-path");

  expect((b as ErrorReport).specific[1].message).toBe(
    "Root path not recognised as 'absolute' path",
  );
  expect((b as ErrorReport).specific[1].root).toBe("b-path");
});

test("loader root that does not exist is rejected", async () => {
  const l = new Loader([join(examplesBadFolder, "THIS-DOES not exist")]);

  const b = await l.checkStructure();

  expect(b.state).toBe("error");

  expect((b as ErrorReport).specific).toBeArrayOfSize(1);

  expect((b as ErrorReport).specific[0].message).toBe(
    "Root path not accessible",
  );
  expect((b as ErrorReport).specific[0].root).toEndWith("THIS-DOES not exist");
});

test("loader root that exists but is not a folder is rejected", async () => {
  const l = new Loader([
    join(examplesBadFolder, "missing-manifest", "batch", "A.bam"),
  ]);

  const b = await l.checkStructure();

  expect(b.state).toBe("error");

  expect((b as ErrorReport).specific).toBeArrayOfSize(1);

  expect((b as ErrorReport).specific[0].message).toBe(
    "Root path not accessible",
  );
  expect((b as ErrorReport).specific[0].root).toEndWith("A.bam");
});

test("batch with non-file artifacts is rejected", async () => {
  const l = new Loader([join(examplesBadFolder, "has-non-files")]);

  const b = await l.checkStructure();

  expect(b.state).toBe("error");

  expect((b as ErrorReport).specific).toBeArrayOfSize(1);

  expect((b as ErrorReport).specific[0].message).toBe(
    "PfdlArtifact is not a plain object (e.g. is a sub-directory or named pipe)",
  );
  expect((b as ErrorReport).specific[0].root).toBe(
    join(examplesBadFolder, "has-non-files"),
  );
  expect((b as ErrorReport).specific[0].batch).toBe("batch");
  expect((b as ErrorReport).specific[0].artifact).toBe("this_is_not_allowed");
});

test("missing md5sums.txt in a batch", async () => {
  const l = new Loader([join(examplesBadFolder, "missing-manifest")]);

  const b = await l.checkStructure();

  expect(b.state).toBe("error");

  expect((b as ErrorReport).specific).toBeArrayOfSize(1);

  expect((b as ErrorReport).specific[0].message).toBe(
    "No manifest file (e.g. 'md5sums.txt') found in batch",
  );
  expect((b as ErrorReport).specific[0].root).toBe(
    join(examplesBadFolder, "missing-manifest"),
  );
  expect((b as ErrorReport).specific[0].batch).toBe("batch");
});
