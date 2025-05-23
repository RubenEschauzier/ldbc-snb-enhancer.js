import type { Writable } from 'node:stream';
import type * as RDF from '@rdfjs/types';
import type { IEnhancementContext, IEnhancementContextSimilarity } from './IEnhancementContext';

/**
 * Generates quads based on a given set of people.
 */
export interface IEnhancementHandler {
  generate: (writeStream: RDF.Stream & Writable, context: IEnhancementContext) => Promise<void>;
}


/**
 * Generates quads based on a given set of people.
 */
export interface IEnhancementHandlerSimilarity {
  generate: (writeStream: RDF.Stream & Writable, context: IEnhancementContextSimilarity) => Promise<void>;
}

