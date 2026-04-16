import { PassThrough, Readable } from 'node:stream';
import { DataFactory } from 'rdf-data-factory';
import 'jest-rdf';
import { EnhancerSimilarity } from '../lib/EnhancerSimilarity';
import type { IEnhancementHandlerSimilarity } from '../lib/handlers/IEnhancementHandler';
import type { IParameterEmitter } from '../lib/parameters/IParameterEmitter';
import { TransformerReplaceIri } from '../lib/transformers/TransformerReplaceIri';
import { DataSelectorSequential } from './selector/DataSelectorSequential';

const streamifyString = require('streamify-string');

const DF = new DataFactory();

const files: Record<string, string> = {};
const writeStream = {
  on: jest.fn(),
  once: jest.fn(),
  emit: jest.fn(),
  end: jest.fn(),
};

jest.mock('node:fs', () => ({
  createReadStream(filePath: string) {
    if (filePath in files) {
      return streamifyString(files[filePath]);
    }
    const ret = new Readable();
    ret._read = () => {
      ret.emit('error', new Error('Unknown file in EnhancerSimilarity'));
    };
    return ret;
  },
  createWriteStream(_filePath: string) {
    return writeStream;
  },
}));

jest.mock('rdf-serialize', () => ({
  rdfSerializer: {
    serialize: jest.fn(() => ({
      pipe: jest.fn(),
    })),
  },
}));

function createEmitter(): IParameterEmitter {
  return {
    emitHeader: jest.fn(),
    emitRow: jest.fn(() => true),
    flush: jest.fn(),
    waitForDrain: jest.fn(() => Promise.resolve()),
  };
}

describe('EnhancerSimilarity', () => {
  let enhancer: EnhancerSimilarity;
  let handlers: IEnhancementHandlerSimilarity[];
  let emitterSimilarityPeople: IParameterEmitter;
  let emitterSimilarityPosts: IParameterEmitter;
  let emitterSimilarityComments: IParameterEmitter;

  beforeEach(() => {
    for (const key of Object.keys(files)) {
      delete files[key];
    }
    jest.clearAllMocks();

    handlers = [
      {
        generate: jest.fn(),
      },
      {
        generate: jest.fn(),
      },
    ];

    emitterSimilarityPeople = createEmitter();
    emitterSimilarityPosts = createEmitter();
    emitterSimilarityComments = createEmitter();

    enhancer = new EnhancerSimilarity({
      personsPath: 'source-persons.ttl',
      activitiesPath: 'source-activities.ttl',
      destinationPathData: 'destination.ttl',
      dataSelector: new DataSelectorSequential(),
      handlers,
      parameterEmitterSimilaritiesPeople: emitterSimilarityPeople,
      parameterEmitterSimilaritiesPosts: emitterSimilarityPosts,
      parameterEmitterSimilaritiesComments: emitterSimilarityComments,
    });

    files['source-persons.ttl'] = `<ex:s> <ex:p> <ex:o>.`;
    files['source-activities.ttl'] = `<ex:s> <ex:p> <ex:o>.`;
  });

  describe('constructor', () => {
    it('should emit required and optional headers', () => {
      const emitterPosts = createEmitter();
      const emitterComments = createEmitter();

      enhancer = new EnhancerSimilarity({
        personsPath: 'source-persons.ttl',
        activitiesPath: 'source-activities.ttl',
        destinationPathData: 'destination.ttl',
        dataSelector: new DataSelectorSequential(),
        parameterEmitterPosts: emitterPosts,
        parameterEmitterComments: emitterComments,
        parameterEmitterSimilaritiesPeople: emitterSimilarityPeople,
        parameterEmitterSimilaritiesPosts: emitterSimilarityPosts,
        parameterEmitterSimilaritiesComments: emitterSimilarityComments,
      });

      expect(enhancer).toBeInstanceOf(EnhancerSimilarity);
      expect(emitterPosts.emitHeader).toHaveBeenCalledWith([ 'post' ]);
      expect(emitterComments.emitHeader).toHaveBeenCalledWith([ 'comment' ]);
      expect(emitterSimilarityPeople.emitHeader).toHaveBeenCalledWith([ 'person', 'similarities' ]);
      expect(emitterSimilarityPosts.emitHeader).toHaveBeenCalledWith([ 'person', 'similarities' ]);
      expect(emitterSimilarityComments.emitHeader).toHaveBeenCalledWith([ 'person', 'similarities' ]);
    });
  });

  describe('generate', () => {
    beforeEach(() => {
      files['source-persons.ttl'] = `@prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
@prefix snvoc: <http://www.ldbc.eu/ldbc_socialnet/1.0/vocabulary/> .
@prefix sn: <http://www.ldbc.eu/ldbc_socialnet/1.0/data/> .
sn:person1 rdf:type snvoc:Person ;
  snvoc:hasInterest sn:interest1 ;
  snvoc:studyAt _:study1 .
_:study1 snvoc:hasOrganisation sn:university1 .
sn:person2 rdf:type snvoc:Person ;
  snvoc:hasInterest sn:interest2 .
sn:thing rdf:type snvoc:SomethingElse .`;

      files['source-activities.ttl'] = `@prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
@prefix snvoc: <http://www.ldbc.eu/ldbc_socialnet/1.0/vocabulary/> .
@prefix sn: <http://www.ldbc.eu/ldbc_socialnet/1.0/data/> .
sn:post1 rdf:type snvoc:Post ; snvoc:hasCreator sn:person1 .
sn:post2 rdf:type snvoc:Post ; snvoc:hasCreator sn:person2 .
sn:comment1 rdf:type snvoc:Comment ; snvoc:hasCreator sn:person2 .`;
    });

    it('should run handlers and finish output', async() => {
      const logger = { log: jest.fn() };
      enhancer = new EnhancerSimilarity({
        personsPath: 'source-persons.ttl',
        activitiesPath: 'source-activities.ttl',
        destinationPathData: 'destination.ttl',
        dataSelector: new DataSelectorSequential(),
        handlers,
        logger,
        parameterEmitterSimilaritiesPeople: emitterSimilarityPeople,
        parameterEmitterSimilaritiesPosts: emitterSimilarityPosts,
        parameterEmitterSimilaritiesComments: emitterSimilarityComments,
      });

      await enhancer.generate();

      expect(handlers[0].generate).toHaveBeenCalledWith(
        expect.any(PassThrough),
        expect.objectContaining({
          rdfObjectLoader: (<any> enhancer).rdfObjectLoader,
          dataSelector: (<any> enhancer).dataSelector,
          people: [
            expect.anything(),
            expect.anything(),
          ],
          interests: [
            expect.anything(),
            expect.anything(),
          ],
          universities: [
            expect.anything(),
          ],
        }),
      );
      expect(handlers[1].generate).toHaveBeenCalledWith(expect.any(PassThrough), expect.any(Object));
      expect(logger.log).toHaveBeenCalledWith('Loading context');
      expect(logger.log).toHaveBeenCalledWith('Preparing output stream');
      expect(logger.log).toHaveBeenCalledWith('Reading background data: activities');
      expect(logger.log).toHaveBeenCalledWith('Reading background data: people');
      expect(logger.log).toHaveBeenCalledWith('Ending');
      const mockSerialize: jest.Mock = require('rdf-serialize').rdfSerializer.serialize;
      expect(mockSerialize).toHaveBeenCalledWith(expect.any(PassThrough), { contentType: 'text/turtle' });
    });

    it('should run with no handlers', async() => {
      enhancer = new EnhancerSimilarity({
        personsPath: 'source-persons.ttl',
        activitiesPath: 'source-activities.ttl',
        destinationPathData: 'destination.ttl',
        dataSelector: new DataSelectorSequential(),
        parameterEmitterSimilaritiesPeople: emitterSimilarityPeople,
        parameterEmitterSimilaritiesPosts: emitterSimilarityPosts,
        parameterEmitterSimilaritiesComments: emitterSimilarityComments,
      });

      await enhancer.generate();
    });
  });

  describe('extractPeopleInterests', () => {
    beforeEach(async() => {
      await (<any> enhancer).rdfObjectLoader.context;
    });

    it('should handle a dummy file', async() => {
      await expect(enhancer.extractPeopleInterests()).resolves.toEqual({
        people: [],
        interests: [],
        universities: [],
        peopleHasInterest: {},
        peopleStudyAt: {},
        predicates: [
          DF.namedNode('ex:p'),
        ],
        personClasses: [],
      });
    });

    it('should handle an empty file', async() => {
      files['source-persons.ttl'] = '';
      await expect(enhancer.extractPeopleInterests()).resolves.toEqual({
        people: [],
        interests: [],
        universities: [],
        peopleHasInterest: {},
        peopleStudyAt: {},
        predicates: [],
        personClasses: [],
      });
    });

    it('should reject on an erroring stream', async() => {
      delete files['source-persons.ttl'];
      await expect(enhancer.extractPeopleInterests()).rejects.toThrow('Unknown file in EnhancerSimilarity');
    });

    it('should handle a valid file', async() => {
      files['source-persons.ttl'] = `@prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
@prefix snvoc: <http://www.ldbc.eu/ldbc_socialnet/1.0/vocabulary/> .
@prefix sn: <http://www.ldbc.eu/ldbc_socialnet/1.0/data/> .
sn:person1 rdf:type snvoc:Person ;
  snvoc:hasInterest sn:interest1 ;
  snvoc:studyAt _:study1 .
_:study1 snvoc:hasOrganisation sn:university1 .
sn:person2 rdf:type snvoc:Person ;
  snvoc:hasInterest sn:interest2 .
sn:other rdf:type snvoc:OtherClass .`;

      const data = await enhancer.extractPeopleInterests();
      expect(data.people).toEqualRdfTermArray([
        DF.namedNode('http://www.ldbc.eu/ldbc_socialnet/1.0/data/person1'),
        DF.namedNode('http://www.ldbc.eu/ldbc_socialnet/1.0/data/person2'),
      ]);
      expect(data.interests).toEqualRdfTermArray([
        DF.namedNode('http://www.ldbc.eu/ldbc_socialnet/1.0/data/interest1'),
        DF.namedNode('http://www.ldbc.eu/ldbc_socialnet/1.0/data/interest2'),
      ]);
      expect(data.universities).toEqualRdfTermArray([
        DF.namedNode('http://www.ldbc.eu/ldbc_socialnet/1.0/data/university1'),
      ]);
      expect(data.peopleHasInterest).toMatchObject({
        'http://www.ldbc.eu/ldbc_socialnet/1.0/data/person1': [
          DF.namedNode('http://www.ldbc.eu/ldbc_socialnet/1.0/data/interest1'),
        ],
        'http://www.ldbc.eu/ldbc_socialnet/1.0/data/person2': [
          DF.namedNode('http://www.ldbc.eu/ldbc_socialnet/1.0/data/interest2'),
        ],
      });
      expect(data.peopleStudyAt).toMatchObject({
        'http://www.ldbc.eu/ldbc_socialnet/1.0/data/person1': [
          DF.namedNode('http://www.ldbc.eu/ldbc_socialnet/1.0/data/university1'),
        ],
      });
      expect(data.predicates).toEqual(expect.arrayContaining([
        DF.namedNode('http://www.w3.org/1999/02/22-rdf-syntax-ns#type'),
        DF.namedNode('http://www.ldbc.eu/ldbc_socialnet/1.0/vocabulary/hasInterest'),
        DF.namedNode('http://www.ldbc.eu/ldbc_socialnet/1.0/vocabulary/studyAt'),
        DF.namedNode('http://www.ldbc.eu/ldbc_socialnet/1.0/vocabulary/hasOrganisation'),
      ]));
      expect(data.personClasses).toEqual(expect.arrayContaining([
        DF.namedNode('http://www.ldbc.eu/ldbc_socialnet/1.0/vocabulary/Person'),
        DF.namedNode('http://www.ldbc.eu/ldbc_socialnet/1.0/vocabulary/OtherClass'),
      ]));
    });
  });

  describe('extractActivities', () => {
    beforeEach(async() => {
      await (<any> enhancer).rdfObjectLoader.context;
    });

    it('should handle a dummy file', async() => {
      await expect(enhancer.extractActivities()).resolves.toEqual({
        posts: [],
        postsCreator: {},
        comments: [],
        commentsCreator: {},
        activityClasses: [
          DF.namedNode('http://www.ldbc.eu/ldbc_socialnet/1.0/vocabulary/Post'),
          DF.namedNode('http://www.ldbc.eu/ldbc_socialnet/1.0/vocabulary/Comment'),
        ],
      });
    });

    it('should handle an empty file', async() => {
      files['source-activities.ttl'] = '';
      await expect(enhancer.extractActivities()).resolves.toEqual({
        posts: [],
        postsCreator: {},
        comments: [],
        commentsCreator: {},
        activityClasses: [
          DF.namedNode('http://www.ldbc.eu/ldbc_socialnet/1.0/vocabulary/Post'),
          DF.namedNode('http://www.ldbc.eu/ldbc_socialnet/1.0/vocabulary/Comment'),
        ],
      });
    });

    it('should reject on an erroring stream', async() => {
      delete files['source-activities.ttl'];
      await expect(enhancer.extractActivities()).rejects.toThrow('Unknown file in EnhancerSimilarity');
    });

    it('should handle a valid file with creator mappings and optional emitters', async() => {
      const emitterPosts = createEmitter();
      const emitterComments = createEmitter();
      enhancer = new EnhancerSimilarity({
        personsPath: 'source-persons.ttl',
        activitiesPath: 'source-activities.ttl',
        destinationPathData: 'destination.ttl',
        dataSelector: new DataSelectorSequential(),
        parameterEmitterPosts: emitterPosts,
        parameterEmitterComments: emitterComments,
        parameterEmitterSimilaritiesPeople: emitterSimilarityPeople,
        parameterEmitterSimilaritiesPosts: emitterSimilarityPosts,
        parameterEmitterSimilaritiesComments: emitterSimilarityComments,
      });
      await (<any> enhancer).rdfObjectLoader.context;

      files['source-activities.ttl'] = `@prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
@prefix snvoc: <http://www.ldbc.eu/ldbc_socialnet/1.0/vocabulary/> .
@prefix sn: <http://www.ldbc.eu/ldbc_socialnet/1.0/data/> .
sn:post1 rdf:type snvoc:Post .
sn:post1 snvoc:hasCreator sn:person1 .
sn:comment1 rdf:type snvoc:Comment .
sn:comment1 snvoc:hasCreator sn:person2 .`;

      const data = await enhancer.extractActivities();
      expect(data.posts).toEqualRdfTermArray([
        DF.namedNode('http://www.ldbc.eu/ldbc_socialnet/1.0/data/post1'),
      ]);
      expect(data.comments).toEqualRdfTermArray([
        DF.namedNode('http://www.ldbc.eu/ldbc_socialnet/1.0/data/comment1'),
      ]);
      expect(data.postsCreator).toMatchObject({
        'http://www.ldbc.eu/ldbc_socialnet/1.0/data/post1': [
          DF.namedNode('http://www.ldbc.eu/ldbc_socialnet/1.0/data/person1'),
        ],
      });
      expect(data.commentsCreator).toMatchObject({
        'http://www.ldbc.eu/ldbc_socialnet/1.0/data/comment1': [
          DF.namedNode('http://www.ldbc.eu/ldbc_socialnet/1.0/data/person2'),
        ],
      });

      expect(emitterPosts.emitHeader).toHaveBeenCalledWith([ 'post' ]);
      expect(emitterPosts.emitRow).toHaveBeenCalledWith([ 'http://www.ldbc.eu/ldbc_socialnet/1.0/data/post1' ]);
      expect(emitterPosts.flush).toHaveBeenCalledTimes(1);
      expect(emitterComments.emitHeader).toHaveBeenCalledWith([ 'comment' ]);
      expect(emitterComments.emitRow).toHaveBeenCalledWith([ 'http://www.ldbc.eu/ldbc_socialnet/1.0/data/comment1' ]);
      expect(emitterComments.flush).toHaveBeenCalledTimes(1);
    });
  });

  describe('helper methods', () => {
    it('personToActivities should invert activity creators', () => {
      expect(
        enhancer.personToActivities({
          'post:a': [ DF.namedNode('person:1') ],
          'post:b': [ DF.namedNode('person:1') ],
          'post:c': [ DF.namedNode('person:2') ],
        }),
      ).toEqual({
        'person:1': [ 'post:a', 'post:b' ],
        'person:2': [ 'post:c' ],
      });
    });

    it('interestsToVector should map interests and universities', () => {
      const peopleSemanticVectors = enhancer.interestsToVector(
        {
          person1: [
            DF.namedNode('interest:1'),
            DF.namedNode('interest:2'),
          ],
          person2: [
            DF.namedNode('interest:2'),
          ],
        },
        {
          person1: [
            DF.namedNode('university:1'),
          ],
        },
        [
          DF.namedNode('interest:1'),
          DF.namedNode('interest:2'),
        ],
        [
          DF.namedNode('university:1'),
          DF.namedNode('university:2'),
        ],
      );

      expect(peopleSemanticVectors).toEqual({
        person1: [ 1, 1, 1, 0 ],
        person2: [ 0, 1, 0, 0 ],
      });
    });

    it('cosineSimilarity should calculate similarity', () => {
      expect(enhancer.cosineSimilarity([ 1, 1 ], [ 1, 1 ])).toBeCloseTo(1);
      expect(enhancer.cosineSimilarity([ 1, 0 ], [ 0, 1 ])).toBe(0);
    });

    it('sortAndTruncateSimilarities should sort and truncate to default max', () => {
      const entries = [
        { entity: 'a', similarity: 0.2 },
        { entity: 'b', similarity: 0.9 },
        { entity: 'c', similarity: 0.7 },
      ];

      expect(enhancer.sortAndTruncateSimilarities(entries)).toEqual([
        { entity: 'b', similarity: 0.9 },
        { entity: 'c', similarity: 0.7 },
        { entity: 'a', similarity: 0.2 },
      ]);
    });
  });

  describe('peopleSimilarities', () => {
    it('should emit sorted similarities with transformer and truncate by maxSimilarities', async() => {
      const stdoutSpy = jest.spyOn(process.stdout, 'write').mockReturnValue(true);
      const personTransformer = new TransformerReplaceIri('^', 'transformed:');
      const transformSpy = jest.spyOn(personTransformer, 'transformTerm');

      enhancer = new EnhancerSimilarity({
        personsPath: 'source-persons.ttl',
        activitiesPath: 'source-activities.ttl',
        destinationPathData: 'destination.ttl',
        dataSelector: new DataSelectorSequential(),
        maxSimilarities: 1,
        personTransformer,
        parameterEmitterSimilaritiesPeople: emitterSimilarityPeople,
        parameterEmitterSimilaritiesPosts: emitterSimilarityPosts,
        parameterEmitterSimilaritiesComments: emitterSimilarityComments,
      });

      await enhancer.peopleSimilarities(
        {
          person1: [ 1, 0 ],
          person2: [ 1, 0 ],
          person3: [ 0, 1 ],
        },
        {
          person2: [ 'post2' ],
          person3: [ 'post3' ],
        },
        {
          person2: [ 'comment2' ],
          person3: [ 'comment3' ],
        },
      );

      expect(stdoutSpy).toHaveBeenCalledWith(expect.stringContaining('Similarities calculated: 0'));
      expect(transformSpy).toHaveBeenCalledWith('person1');
      expect(transformSpy).toHaveBeenCalledWith('person2');

      expect(emitterSimilarityPeople.emitRow).toHaveBeenCalledTimes(3);
      expect(emitterSimilarityPosts.emitRow).toHaveBeenCalledTimes(3);
      expect(emitterSimilarityComments.emitRow).toHaveBeenCalledTimes(3);

      const similaritiesPeopleJson = (<jest.Mock> emitterSimilarityPeople.emitRow).mock.calls[0][0][1];
      const similaritiesPostsJson = (<jest.Mock> emitterSimilarityPosts.emitRow).mock.calls[0][0][1];
      const similaritiesCommentsJson = (<jest.Mock> emitterSimilarityComments.emitRow).mock.calls[0][0][1];

      expect(JSON.parse(similaritiesPeopleJson)).toHaveLength(1);
      expect(JSON.parse(similaritiesPostsJson)).toHaveLength(1);
      expect(JSON.parse(similaritiesCommentsJson)).toHaveLength(1);

      expect(emitterSimilarityPeople.flush).toHaveBeenCalledTimes(1);
      expect(emitterSimilarityPosts.flush).toHaveBeenCalledTimes(1);
      expect(emitterSimilarityComments.flush).toHaveBeenCalledTimes(1);

      stdoutSpy.mockRestore();
    });

    it('should handle a single person without transformer', async() => {
      await enhancer.peopleSimilarities(
        {
          person1: [ 1, 0 ],
        },
        {},
        {},
      );

      expect(emitterSimilarityPeople.emitRow).toHaveBeenCalledWith([ 'person1', '[]' ]);
      expect(emitterSimilarityPosts.emitRow).toHaveBeenCalledWith([ 'person1', '[]' ]);
      expect(emitterSimilarityComments.emitRow).toHaveBeenCalledWith([ 'person1', '[]' ]);
    });
  });
});
