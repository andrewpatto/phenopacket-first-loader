export async function resolveContentToConsentpacket(
  content: Buffer,
): Promise<any | null> {
  try {
    const contentString = content.toString("utf-8");
    const jsonParsed = JSON.parse(contentString);

    // TODO use actual schema here

    if ("schemaMajorVersion" in jsonParsed) return jsonParsed;
  } catch (e) {
    return null;
  }

  return null;
}
