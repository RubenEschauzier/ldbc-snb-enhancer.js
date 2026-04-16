import { ParameterEmitterCsv } from '../../lib/parameters/ParameterEmitterCsv';

const writeStream = {
  write: jest.fn(),
  once: jest.fn(),
  end: jest.fn(),
};
jest.mock('node:fs', () => ({
  createWriteStream(_filePath: string) {
    return writeStream;
  },
}));

describe('ParameterEmitterCsv', () => {
  let emitter: ParameterEmitterCsv;

  beforeEach(async() => {
    emitter = new ParameterEmitterCsv('output.csv');
  });

  describe('emitHeader', () => {
    it('to produce a header', () => {
      emitter.emitHeader([ 'a', 'b' ]);
      expect(writeStream.write).toHaveBeenCalledWith(`a,b\n`);
    });

    it('to throw when invoked twice', () => {
      emitter.emitHeader([ 'a', 'b' ]);
      expect(() => emitter.emitHeader([ 'a', 'b' ]))
        .toThrow('Attempted to emit header more than once.');
    });
  });

  describe('emitRow', () => {
    beforeEach(() => {
      emitter.emitHeader([ 'a', 'b' ]);
    });

    it('to produce a row', () => {
      emitter.emitRow([ 'a1', 'b1' ]);
      expect(writeStream.write).toHaveBeenCalledWith(`a1,b1\n`);
    });

    it('to produce multiple rows', () => {
      emitter.emitRow([ 'a1', 'b1' ]);
      emitter.emitRow([ 'a2', 'b2' ]);
      emitter.emitRow([ 'a3', 'b3' ]);
      expect(writeStream.write).toHaveBeenCalledWith(`a1,b1\n`);
      expect(writeStream.write).toHaveBeenCalledWith(`a2,b2\n`);
      expect(writeStream.write).toHaveBeenCalledWith(`a3,b3\n`);
    });

    it('to throw when column length is different from header length', () => {
      expect(() => emitter.emitRow([ 'a1', 'b1', 'c1' ]))
        .toThrow('A column of length 3 was emitted, while length 2 is required.');
    });
  });

  describe('flush', () => {
    it('to end the stream', () => {
      emitter.flush();
      expect(writeStream.end).toHaveBeenCalledTimes(1);
    });
  });

  describe('waitForDrain', () => {
    it('should return immediately when write succeeds', async() => {
      await expect(emitter.waitForDrain(true)).resolves.toBeUndefined();
      expect(writeStream.once).not.toHaveBeenCalled();
    });

    it('should wait for drain when write fails', async() => {
      writeStream.once.mockImplementation((_event: string, callback: () => void) => {
        callback();
      });
      await expect(emitter.waitForDrain(false)).resolves.toBeUndefined();
      expect(writeStream.once).toHaveBeenCalledWith('drain', expect.any(Function));
    });
  });
});
