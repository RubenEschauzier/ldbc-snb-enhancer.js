import { DataFactory } from 'rdf-data-factory';
import { EnhancerSimilarity } from '../lib/EnhancerSimilarity';
import type { IEnhancementHandler } from '../lib/handlers/IEnhancementHandler';
import { ParameterEmitterCsv } from '../lib/parameters/ParameterEmitterCsv';
import { DataSelectorSequential } from './selector/DataSelectorSequential';
import { IParameterEmitter } from '../lib/parameters/IParameterEmitter';

const streamifyString = require('streamify-string');

const DF = new DataFactory();

const files: Record<string, string> = {};
const writeStream = {
  on: jest.fn(),
  once: jest.fn(),
  emit: jest.fn(),
  end: jest.fn(),
};

describe('EnhancerSimilarity', () => {
  let enhancer: EnhancerSimilarity;
  let handlers: IEnhancementHandler[];

  let emitterSimilarityPeople: IParameterEmitter;
  let emitterSimilarityPosts: IParameterEmitter;
  let emitterSimilarityComments: IParameterEmitter

  beforeEach(() => {
    handlers = [
      {
        generate: jest.fn(),
      },
      {
        generate: jest.fn(),
      },
    ];

    emitterSimilarityPeople = {
        emitHeader: jest.fn(),
        emitRow: jest.fn(),
        flush: jest.fn(),
        waitForDrain: jest.fn(),
    };
    emitterSimilarityPosts = {
        emitHeader: jest.fn(),
        emitRow: jest.fn(),
        flush: jest.fn(),
        waitForDrain: jest.fn(),
    };
    emitterSimilarityComments = {
        emitHeader: jest.fn(),
        emitRow: jest.fn(),
        flush: jest.fn(),
        waitForDrain: jest.fn(),
    };


    // Initialize with dummy parameters based on your constructor
    enhancer = new EnhancerSimilarity(
      {
        personsPath: 'source-persons.ttl',
        activitiesPath: 'source-activities.ttl',
        destinationPathData: 'destination.ttl',
        dataSelector: new DataSelectorSequential(),
        parameterEmitterSimilaritiesPeople: emitterSimilarityPeople,
        parameterEmitterSimilaritiesPosts: emitterSimilarityPosts,
        parameterEmitterSimilaritiesComments: emitterSimilarityComments,
      },
    );
  });

  it('should instantiate correctly', () => {
    expect(enhancer).toBeInstanceOf(EnhancerSimilarity);
  });
});
