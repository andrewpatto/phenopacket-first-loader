#!/usr/bin/env tsx

import { Command } from "commander";
import { resolve } from "node:path";
import { Loader } from "../lib/pfdl";

const program = new Command();

program.option("--roots <paths...>");

program.parse();

const options = program.opts();

if (options.roots) {
  // if the paths were specified relatively we want to make them
  // absolute before passing on
  const absoluteRootPaths = options.roots;

  for (let i = 0; i < absoluteRootPaths.length; i++) {
    absoluteRootPaths[i] = resolve(absoluteRootPaths[i]);
  }

  const l = new Loader(absoluteRootPaths);

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
    } else {
      console.log(JSON.stringify(f.dataset, null, 2));
    }
  }
}
