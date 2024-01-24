import { expect, test } from "bun:test";
import { Loader } from "../lib/loader.ts";
import { resolve, join } from "node:path";
import { ErrorReport } from "../lib/common-types.ts";

const examplesFolder = resolve("..", "examples");
const examplesBadFolder = resolve("..", "examples-bad");

// this is not really a unit test - it just helps out that if we are failing our
// E2E test that we get printouts of the error messages
test("basic end to end with printing of error messages if found", async () => {
  const l = new Loader([join(examplesFolder, "test1")]);

  const structure = await l.checkStructure();

  if (structure.state !== "data") {
    expect(structure.state).fail(JSON.stringify(structure, null, 2));
  } else {
    const r = await l.checkPhenopacketStructure(structure);

    if (r) {
      expect(r).fail(JSON.stringify(r, null, 2));
    }

    const f = await l.checkPhenopackets(structure);

    if (f.state !== "data") {
      expect(f.state).fail(JSON.stringify(f, null, 2));
    }
  }
});

test("basic end to end with single root", async () => {
  const l = new Loader([join(examplesFolder, "test1")]);

  const structure = await l.checkStructure();

  expect(structure.state).toBe("data");

  if (structure.state !== "data") return;

  {
    // the keys are filenames sorted alphabetically
    const batchNames = Array.from(structure.artifacts.keys());

    expect(batchNames).toBeArrayOfSize(18);

    expect(batchNames[0]).toBe("A.consentpacket.json");
    expect(batchNames[1]).toBe("A.phenopacket.json");

    // expect the fastqs to be an array - with the latest batch appearing
    // first (i.e batch 2020-02-06 has replaced the items in 2020-01-12)
    expect(batchNames[11]).toBe("NEXTSEQ_ABCD_PERSONA_TOPUP_R2.fastq.gz");
    const r2artifacts = structure.artifacts.get(batchNames[11])!;
    expect(r2artifacts).toBeArrayOfSize(2);
    expect(r2artifacts[0].batch.name).toBe("2020-02-06");
    expect(r2artifacts[0].getChecksum("MD5")).toBe(
      "d41d8cd98f00b204e9800998ecf8427e"
    );
    expect(r2artifacts[1].batch.name).toBe("2020-01-12");
    expect(r2artifacts[1].getChecksum("MD5")).toBe(
      "9d5ed678fe57bcca610140957afab571"
    );

    expect(batchNames[17]).toBe("SIMPSONS.phenopacket.json");
    expect(structure.artifacts.get(batchNames[17])).toBeArrayOfSize(1);
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
    "Root path not recognised as 'absolute' path"
  );
  expect((b as ErrorReport).specific[0].root).toBe("../a-path");

  expect((b as ErrorReport).specific[1].message).toBe(
    "Root path not recognised as 'absolute' path"
  );
  expect((b as ErrorReport).specific[1].root).toBe("b-path");
});

test("loader root that does not exist is rejected", async () => {
  const l = new Loader([join(examplesBadFolder, "THIS-DOES not exist")]);

  const b = await l.checkStructure();

  expect(b.state).toBe("error");

  expect((b as ErrorReport).specific).toBeArrayOfSize(1);

  expect((b as ErrorReport).specific[0].message).toBe(
    "Root path not accessible"
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
    "Root path not accessible"
  );
  expect((b as ErrorReport).specific[0].root).toEndWith("A.bam");
});

test("batch with non-file artifacts is rejected", async () => {
  const l = new Loader([join(examplesBadFolder, "has-non-files")]);

  const b = await l.checkStructure();

  expect(b.state).toBe("error");

  expect((b as ErrorReport).specific).toBeArrayOfSize(1);

  expect((b as ErrorReport).specific[0].message).toBe(
    "Artifact is not a plain object (e.g. is a sub-directory or named pipe)"
  );
  expect((b as ErrorReport).specific[0].root).toBe(
    join(examplesBadFolder, "has-non-files")
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
    "No manifest file (e.g. 'md5sums.txt') found in batch"
  );
  expect((b as ErrorReport).specific[0].root).toBe(
    join(examplesBadFolder, "missing-manifest")
  );
  expect((b as ErrorReport).specific[0].batch).toBe("batch");
});

test("batch name must be unique across all roots", async () => {
  const l = new Loader([
    join(examplesBadFolder, "duplicate-batches-1"),
    join(examplesBadFolder, "duplicate-batches-2"),
  ]);

  const b = await l.checkStructure();

  expect(b.state).toBe("error");

  expect((b as ErrorReport).specific).toBeArrayOfSize(2);

  expect((b as ErrorReport).specific[0].message).toBe(
    "Batch name has occurred in another root"
  );
  expect((b as ErrorReport).specific[0].batch).toBe("001");

  expect((b as ErrorReport).specific[1].message).toBe(
    "Batch name has occurred in another root"
  );
  expect((b as ErrorReport).specific[1].batch).toBe("001");
});
