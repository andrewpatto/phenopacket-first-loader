import { Loader} from "pfdl";
import {join, resolve} from "node:path";

const examplesFolder = resolve("..", "examples");

const l = new Loader([join(examplesFolder, "test1")]);

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
    } else
    {
        console.log(JSON.stringify(f.dataset, null, 2));
    }
}
