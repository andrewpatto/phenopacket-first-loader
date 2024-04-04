import { Loader } from "../lib/pfdl";
import "jest-extended";
import { RootFactory } from "../lib/root/root-factory";
import { S3Root } from "../lib/s3/s3-root";
import { join, resolve } from "node:path";
import {ErrorReport} from "../lib/common-types";

const examplesFolder = resolve("..", "examples");

test("basic end to end where the roots have identical content that should be merged", async () => {
  const l = new Loader([
    join(examplesFolder, "test1"),
    "s3://umccr-10f-data-dev/pfdl-test1",
  ]);

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

test.skip("batch name must be unique across all roots", async () => {
/*  const l = new Loader([
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
  expect((b as ErrorReport).specific[1].batch).toBe("001"); */
});
