import {S3Root} from "../s3/s3-root";
import {PosixRoot} from "../posix/posix-root";
import {PfdlRoot} from "../pfdl-root";

export class RootFactory {
    public static CreateRoot = (root: string): PfdlRoot => {
        if (root.startsWith("s3://")) return new S3Root(root);
        else return new PosixRoot(root);
    };
}
