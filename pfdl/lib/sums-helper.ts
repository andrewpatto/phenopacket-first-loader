/**
 * Given an md5sums.txt content, return all the entries as a map of "name" to "checksum".
 *
 * @param content
 */
export function md5SumsToObject(content: string): Record<string, string> {
  // For each file, ‘md5sum’ outputs by default, the MD5 md5Checksum,
  // a space, a flag indicating binary or text input mode, and the
  // file name. Binary mode is indicated with ‘*’, text mode
  // with ‘ ’ (space). Binary mode is the default on systems where
  // it’s significant, otherwise text mode is the default.
  let sums: { [file: string]: string } = {};

  for (const line of content.split("\n")) {
    // skip possible blank last line
    if (line.trim().length == 0) continue;

    if (line.startsWith("\\")) {
      // TODO we could support this but no need unless this is actually encountered (which is doubtful)
      // Without --zero, if file contains a backslash, newline,
      // or carriage return, the line is started with a backslash, and
      // each problematic character in the file name is escaped
      // with a backslash, making the output unambiguous
      // even in the presence of arbitrary file names.
      throw new Error("We do not support md5sums.txt with escaped file names");
    }

    const checksum = line.slice(0, 32);

    // TODO check the md5Checksum is valid - NOT that the content matches - just literally does it match the
    // definition for an md5 sum?

    const file = line.slice(34);

    // we ignore the type - should we?? (will be either space or *)
    const t = line.slice(33, 34);

    if (file === "md5sums.txt") continue;

    sums[file] = checksum;
  }

  return sums;
}
