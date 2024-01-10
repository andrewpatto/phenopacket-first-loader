import { expect, test } from "bun:test";
import { Loader } from "../lib/loader.ts";
import { resolve, join } from "node:path";

const examplesFolder = resolve("..", "examples");
const examplesBadFolder = resolve("..", "examples-bad");

test("basic end to end with single root", async () => {
  const l = new Loader([
    join(examplesFolder, "test1")
  ]);

  const b = await l.allBatches();

  expect(b.size).toBe(4);

  // the keys direct from the allBatches map are required to be in sorted order
  const batchNames = Array.from(b.keys());

  expect(batchNames[0]).toBe("2020-01-02");
  expect(batchNames[1]).toBe("2020-01-12");
  expect(batchNames[2]).toBe("2020-02-06");
  expect(batchNames[3]).toBe("2021");

  await l.checkStructure();



});

test("loader root does not exist as a folder", async () => {
  const l = new Loader([
    join(examplesBadFolder, "THIS-DOES not exist")
  ]);

  await l.checkStructure();
});


test("missing md5sums.txt in a batch", async () => {
  const l = new Loader([
    join(examplesBadFolder, "missing-sums")
  ]);

  await l.checkStructure();
});
