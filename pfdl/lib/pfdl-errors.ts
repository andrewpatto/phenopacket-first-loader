import {ErrorSpecific} from "./common-types";

export class PfdlError extends Error {

    constructor(message: string, public specifics: ErrorSpecific[]) {
        super(message);
    }
}