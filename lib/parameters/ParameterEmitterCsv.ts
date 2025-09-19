import type { WriteStream } from 'node:fs';
import * as fs from 'node:fs';
import type { IParameterEmitter } from './IParameterEmitter';

/**
 * Emits parameters as CSV files.
 */
export class ParameterEmitterCsv implements IParameterEmitter {
  private readonly destinationPath: string;
  private readonly fileStream: WriteStream;
  private readonly separator: string;

  private headerLength = -1;

  public constructor(destinationPath: string, separator = ',') {
    this.destinationPath = destinationPath;
    this.separator = separator;
    this.fileStream = fs.createWriteStream(this.destinationPath);
  }

  public emitHeader(columnNames: string[]): boolean {
    // Validate columns
    if (this.headerLength === -1) {
      this.headerLength = columnNames.length;
    } else {
      throw new Error('Attempted to emit header more than once.');
    }

    return this.emitRow(columnNames);
  }

  public emitRow(columns: string[]): boolean {
    // Validate columns
    if (columns.length !== this.headerLength) {
      throw new Error(`A column of length ${columns.length} was emitted, while length ${this.headerLength} is required.`);
    }

    return this.fileStream.write(`${columns.join(this.separator)}\n`);
  }

  public async waitForDrain(writeResult: boolean): Promise<void> {
    if (writeResult) {
      return;
    }
    // Wait for 'drain' event to resume writing
    return new Promise((resolve) => {
      this.fileStream.once('drain', () => resolve());
    });
  }

  public flush(): void {
    this.fileStream.end();
  }
}
