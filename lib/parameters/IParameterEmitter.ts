/**
 * Emits parameters in a tabular structure.
 */
export interface IParameterEmitter {
  emitHeader: (columnNames: string[]) => boolean;
  emitRow: (columns: string[]) => boolean;
  flush: () => void;
  waitForDrain: (writeResult: boolean) => Promise<void>;
}
