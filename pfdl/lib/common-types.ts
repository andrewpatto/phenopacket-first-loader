export type ErrorReport = {
    state: "error";
    error: string;
    specific: ErrorSpecific[];
  };

export type ErrorSpecific = {
  message: string;
  root?: string;
  batch?: string;
  artifact?: string;
}
/*
export function compareErrorSpecific(a: ErrorSpecific, b: ErrorSpecific): number {

    if (a.message !== b.message)
      return a.message.localeCompare(b.message);

    if (a.root !== b.root)
      return a.root?.localeCompare(b.root);


}*/