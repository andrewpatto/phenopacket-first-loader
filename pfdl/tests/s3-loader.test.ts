import { Loader } from "../lib/pfdl";
import "jest-extended";
import { RootFactory } from "../lib/root/root-factory";
import { S3Root } from "../lib/s3/s3-root";

// this is not really a unit test - it just helps out that if we are failing our
// E2E test that we get printouts of the error messages
test("basic end to end with printing of error messages if found", async () => {
  const l = new Loader(["s3://umccr-10f-data-dev/pfdl-test1"]);

  const structure = await l.checkStructure();

  if (structure.state !== "data") {
    console.error(JSON.stringify(structure, null, 2));
  } else {
    const r = await l.checkPhenopacketStructure(structure);

    if (r) {
      console.error(JSON.stringify(r, null, 2));
    }

    const f = await l.checkPhenopackets(structure);

    if (f.state !== "data") {
      console.error(JSON.stringify(f, null, 2));
    }

    console.log(JSON.stringify(f, null, 2));
  }
});

test("basic folder work in S3", async () => {
  const r: S3Root = RootFactory.CreateRoot(
    "s3://umccr-10f-data-dev/pfdl-test1",
  ) as S3Root;

  expect(r.root).toBe("s3://umccr-10f-data-dev/pfdl-test1");
  expect(r.bucket).toBe("umccr-10f-data-dev");
  expect(r.key).toBe("pfdl-test1/");

  const b = await r.batches();

  expect(b).toBeArrayOfSize(4);

  // our batches method does not establish any order.. but for test assertions we need to
  const bSorted = b.sort((a, b) => a.name.localeCompare(b.name));

  expect(bSorted[0].root).toEqual(r);

  expect(bSorted[0].name).toBe("2020-01-02");
  expect(bSorted[1].name).toBe("2020-01-12");
  expect(bSorted[2].name).toBe("2020-02-06");
  expect(bSorted[3].name).toBe("2021");
});

// note: we possibly instead should just ban using a trailing slash
test("trailing slash in S3", async () => {
  const r: S3Root = RootFactory.CreateRoot(
    "s3://umccr-10f-data-dev/pfdl-test1/",
  ) as S3Root;

  expect(r.root).toBe("s3://umccr-10f-data-dev/pfdl-test1/");
  expect(r.bucket).toBe("umccr-10f-data-dev");
  expect(r.key).toBe("pfdl-test1/");
});

test("basic batch work in S3", async () => {
  const r: S3Root = RootFactory.CreateRoot(
    "s3://umccr-10f-data-dev/pfdl-test1",
  ) as S3Root;

  const b = await r.batches();

  expect(b).toBeArrayOfSize(4);

  // our batches method does not establish any order.. but for test assertions we need to
  const bSorted = b.sort((a, b) => a.name.localeCompare(b.name));

  const a = await bSorted[0].loadAndCheckEntries();

  expect(a).toBeArrayOfSize(9);
});
