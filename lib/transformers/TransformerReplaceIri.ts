import { DataFactory } from 'rdf-data-factory';

const DF = new DataFactory();

/**
 * A quad transformer that replaces (parts of) IRIs.
 */
export class TransformerReplaceIri {
  private readonly search: RegExp;
  private readonly replacement: string;

  public constructor(searchRegex: string, replacementString: string) {
    console.log("New")
    this.search = new RegExp(searchRegex, 'u');
    this.replacement = replacementString;
  }

  public transformTerm(term: string): string {
    const replaced = term.replace(this.search, this.replacement);
    return replaced;
  }
}
