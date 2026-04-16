import * as fs from 'node:fs';
import type { Writable } from 'node:stream';
import { PassThrough } from 'node:stream';
import type * as RDF from '@rdfjs/types';
import { DataFactory } from 'rdf-data-factory';
import type { IRdfClassLoaderArgs } from 'rdf-object';
import { RdfObjectLoader } from 'rdf-object';
import { rdfParser } from 'rdf-parse';
import { rdfSerializer } from 'rdf-serialize';
import type { IEnhancementContextSimilarity } from './handlers/IEnhancementContext';
import type { IEnhancementHandlerSimilarity } from './handlers/IEnhancementHandler';
import type { ILogger } from './logging/ILogger';
import type { IParameterEmitter } from './parameters/IParameterEmitter';
import type { IDataSelector } from './selector/IDataSelector';
import type { TransformerReplaceIri } from './transformers/TransformerReplaceIri';

const DF = new DataFactory();

/**
 * Enhances a given dataset.
 */
export class EnhancerSimilarity {
  // eslint-disable-next-line ts/no-var-requires, ts/no-require-imports, import/extensions, ts/naming-convention
  public static readonly CONTEXT_LDBC_SNB = <IRdfClassLoaderArgs>require('./context-ldbc-snb.json');

  private readonly personsPath: string;
  private readonly activitiesPath: string;
  private readonly destinationPathData: string;
  private readonly dataSelector: IDataSelector;
  private readonly handlers: IEnhancementHandlerSimilarity[];
  private readonly logger?: ILogger;
  private readonly parameterEmitterSimilaritiesPeople: IParameterEmitter;
  private readonly parameterEmitterSimilaritiesPosts: IParameterEmitter;
  private readonly parameterEmitterSimilaritiesComments: IParameterEmitter;

  private readonly parameterEmitterPosts?: IParameterEmitter;
  private readonly parameterEmitterComments?: IParameterEmitter;
  private readonly maxSimilarities: number = 200;
  private readonly personTransformer?: TransformerReplaceIri;
  private readonly rdfObjectLoader: RdfObjectLoader;

  public constructor(options: IEnhancerSimilarityOptions) {
    this.personsPath = options.personsPath;
    this.activitiesPath = options.activitiesPath;

    this.destinationPathData = options.destinationPathData;
    this.dataSelector = options.dataSelector;
    // eslint-disable-next-line ts/prefer-nullish-coalescing
    this.handlers = options.handlers || [];
    this.logger = options.logger;
    this.parameterEmitterPosts = options.parameterEmitterPosts;
    this.parameterEmitterComments = options.parameterEmitterComments;
    this.parameterEmitterSimilaritiesPeople = options.parameterEmitterSimilaritiesPeople;
    this.parameterEmitterSimilaritiesPosts = options.parameterEmitterSimilaritiesPosts;
    this.parameterEmitterSimilaritiesComments = options.parameterEmitterSimilaritiesComments;

    if (options.maxSimilarities) {
      this.maxSimilarities = options.maxSimilarities;
    }
    this.personTransformer = options.personTransformer;

    this.rdfObjectLoader = new RdfObjectLoader({ context: EnhancerSimilarity.CONTEXT_LDBC_SNB });

    this.parameterEmitterPosts?.emitHeader([ 'post' ]);
    this.parameterEmitterComments?.emitHeader([ 'comment' ]);
    this.parameterEmitterSimilaritiesPeople.emitHeader([ 'person', 'similarities' ]);
    this.parameterEmitterSimilaritiesPosts.emitHeader([ 'person', 'similarities' ]);
    this.parameterEmitterSimilaritiesComments.emitHeader([ 'person', 'similarities' ]);
  }

  /**
   * Generates an auxiliary dataset.
   */
  public async generate(): Promise<void> {
    // Make sure our object loader is initialized
    this.logger?.log('Loading context');
    await this.rdfObjectLoader.context;

    // Prepare output stream
    this.logger?.log('Preparing output stream');
    const writeStream: RDF.Stream & Writable = <any> new PassThrough({ objectMode: true });
    const fileStream = fs.createWriteStream(this.destinationPathData);
    rdfSerializer.serialize(writeStream, { contentType: 'text/turtle' }).pipe(fileStream);

    this.logger?.log('Reading background data: activities');
    const {
      postsCreator,
      commentsCreator,
    } = await this.extractActivities();
    const personToPost = this.personToActivities(postsCreator);
    const personToComment = this.personToActivities(commentsCreator);

    // Prepare context
    this.logger?.log('Reading background data: people');
    const {
      people,
      interests,
      universities,
      peopleHasInterest,
      peopleStudyAt,
      predicates,
      personClasses,
    } = await this.extractPeopleInterests();

    const peopleSemanticVectors = this.interestsToVector(
      peopleHasInterest,
      peopleStudyAt,
      interests,
      universities,
    );
    await this.peopleSimilarities(peopleSemanticVectors, personToPost, personToComment);

    const classes: RDF.NamedNode[] = [ ...personClasses ];
    const context: IEnhancementContextSimilarity = {
      rdfObjectLoader: this.rdfObjectLoader,
      dataSelector: this.dataSelector,
      people,
      interests,
      universities,
      peopleHasInterest,
      peopleStudyAt,
      predicates,
      classes,
    };

    // Generate data
    for (const handler of this.handlers) {
      this.logger?.log(`Running ${handler.constructor.name}`);
      await handler.generate(writeStream, context);
    }

    // Close output stream
    this.logger?.log('Ending');
    writeStream.end();
  }

  public extractPeopleInterests(): Promise<{
    people: RDF.NamedNode[];
    interests: RDF.NamedNode[];
    universities: RDF.NamedNode[];
    peopleHasInterest: Record<string, RDF.NamedNode[]>;
    peopleStudyAt: Record<string, RDF.NamedNode[]>;
    predicates: RDF.NamedNode[];
    personClasses: RDF.NamedNode[];
  }> {
    return new Promise((resolve, reject) => {
      // Prepare RDF terms to compare with
      const termType = this.rdfObjectLoader.createCompactedResource('rdf:type').term;
      const termPerson = this.rdfObjectLoader.createCompactedResource('snvoc:Person').term;
      const termStudyAt = this.rdfObjectLoader.createCompactedResource('snvoc:studyAt').term;
      const termhasOrganisation = this.rdfObjectLoader.createCompactedResource('snvoc:hasOrganisation').term;
      const termHasInterest = this.rdfObjectLoader.createCompactedResource('snvoc:hasInterest').term;

      const people: RDF.NamedNode[] = [];
      const interests: RDF.NamedNode[] = [];
      const universities: RDF.NamedNode[] = [];

      const peopleHasInterest: Record<string, RDF.NamedNode[]> = {};
      const peopleStudyAt: Record<string, RDF.NamedNode[]> = {};
      const predicates: Set<string> = new Set<string>();
      const classes: Set<string> = new Set<string>();
      const stream = rdfParser.parse(fs.createReadStream(this.personsPath), { path: this.personsPath });

      // Temporary variables to determine knows relationships
      let currentStudyAtPerson: RDF.NamedNode | undefined;
      let currentStudyAtNode: RDF.BlankNode | undefined;

      stream.on('error', reject);
      stream.on('data', (quad: RDF.Quad) => {
        // Extract people
        if (quad.subject.termType === 'NamedNode' &&
          quad.predicate.equals(termType) &&
          quad.object.equals(termPerson)) {
          people.push(quad.subject);
        }

        // Extract interests of people
        if (quad.subject.termType === 'NamedNode' &&
            quad.object.termType === 'NamedNode' &&
            quad.predicate.equals(termHasInterest)
        ) {
          interests.push(quad.object);
          if (!peopleHasInterest[quad.subject.value]) {
            peopleHasInterest[quad.subject.value] = [];
          }
          peopleHasInterest[quad.subject.value].push(quad.object);
        }

        // Extract people studyAt relationships
        // 1. Determine reified blank node identifying the relationships
        if (quad.subject.termType === 'NamedNode' &&
          quad.predicate.equals(termStudyAt) &&
          quad.object.termType === 'BlankNode') {
          currentStudyAtPerson = quad.subject;
          currentStudyAtNode = quad.object;
        }
        // 2. Determine the person linked to the relationships
        if (currentStudyAtPerson &&
          quad.subject.equals(currentStudyAtNode) &&
          quad.predicate.equals(termhasOrganisation) &&
          quad.object.termType === 'NamedNode') {
          if (!peopleStudyAt[currentStudyAtPerson.value]) {
            peopleStudyAt[currentStudyAtPerson.value] = [];
          }
          peopleStudyAt[currentStudyAtPerson.value].push(quad.object);
          universities.push(quad.object);

          currentStudyAtPerson = undefined;
          currentStudyAtNode = undefined;
        }

        // Determine predicates
        predicates.add(quad.predicate.value);

        // Determine classes
        if (quad.predicate.equals(termType)) {
          classes.add(quad.object.value);
        }
      });
      stream.on('end', () => {
        resolve({
          people,
          interests,
          universities,
          peopleHasInterest,
          peopleStudyAt,
          predicates: [ ...predicates ].map(value => DF.namedNode(value)),
          personClasses: [ ...classes ].map(value => DF.namedNode(value)),
        });
      });
    });
  }

  public extractActivities(): Promise<{
    posts: RDF.NamedNode[];
    postsCreator: Record<string, RDF.Term[]>;
    comments: RDF.NamedNode[];
    commentsCreator: Record<string, RDF.Term[]>;
    activityClasses: RDF.NamedNode[];
  }> {
    return new Promise<{
      posts: RDF.NamedNode[];
      postsCreator: Record<string, RDF.Term[]>;
      comments: RDF.NamedNode[];
      commentsCreator: Record<string, RDF.Term[]>;
      activityClasses: RDF.NamedNode[];
    }>((resolve, reject) => {
      // Prepare RDF terms to compare with
      const termType = this.rdfObjectLoader.createCompactedResource('rdf:type').term;
      const termPost = this.rdfObjectLoader.createCompactedResource('snvoc:Post').term;
      const termComment = this.rdfObjectLoader.createCompactedResource('snvoc:Comment').term;
      const termHasCreator = this.rdfObjectLoader.createCompactedResource('snvoc:hasCreator').term;

      const posts: RDF.NamedNode[] = [];
      const postsCreator: Record<string, RDF.Term[]> = {};
      const comments: RDF.NamedNode[] = [];
      const commentsCreator: Record<string, RDF.Term[]> = {};
      const stream = rdfParser.parse(fs.createReadStream(this.activitiesPath), { path: this.activitiesPath });
      stream.on('error', reject);
      stream.on('data', (quad: RDF.Quad) => {
        if (quad.subject.termType === 'NamedNode' &&
            quad.predicate.equals(termType)) {
          if (quad.object.equals(termPost)) {
            posts.push(quad.subject);
            // Emit parameters
            this.parameterEmitterPosts?.emitRow([ quad.subject.value ]);
            postsCreator[quad.subject.value] = [];
          }
          if (quad.object.equals(termComment)) {
            comments.push(quad.subject);
            commentsCreator[quad.subject.value] = [];
            this.parameterEmitterComments?.emitRow([ quad.subject.value ]);
          }
        }
        if (quad.subject.termType === 'NamedNode' && quad.predicate.equals(termHasCreator)) {
          const postDetails = postsCreator[quad.subject.value];
          const commentDetails = commentsCreator[quad.subject.value];
          if (postDetails) {
            postDetails.push(quad.object);
          } else {
            commentDetails.push(quad.object);
          }
        }
      });
      stream.on('end', () => {
        this.parameterEmitterPosts?.flush();
        this.parameterEmitterComments?.flush();
        resolve({
          posts,
          postsCreator,
          comments,
          commentsCreator,
          activityClasses: [ DF.namedNode(termPost.value), DF.namedNode(termComment.value) ],
        });
      });
    });
  }

  public personToActivities(activityToPerson: Record<string, RDF.Term[]>): Record<string, string[]> {
    const personToActivity: Record<string, string[]> = {};
    for (const activity of Object.keys(activityToPerson)) {
      // The first value is always the creator of the activity, could
      // probably do with removing the array and just setting directly.
      const person = activityToPerson[activity][0].value;
      if (!personToActivity[person]) {
        personToActivity[person] = [];
      }
      personToActivity[person].push(activity);
    }
    return personToActivity;
  }

  /**
   * Encode interests and university of a person to a semantic one-hot encoded vector
   * @param peopleHasInterest
   * @param peopleStudyAt
   * @param interests
   * @param universities
   * @returns Record mapping person to vector representing their interests
   */
  public interestsToVector(
    peopleHasInterest: Record<string, RDF.NamedNode[]>,
    peopleStudyAt: Record<string, RDF.NamedNode[]>,
    interests: RDF.NamedNode[],
    universities: RDF.NamedNode[],
  ): Record<string, number[]> {
    const indexMapInterests: Record<string, number> = Object.fromEntries(interests.map((s, i) => [ s.value, i ]));
    const indexMapUniversities: Record<string, number> = Object.fromEntries(universities.map((s, i) => [ s.value, i ]));

    const peopleSemanticVectors: Record<string, number[]> = {};
    for (const person of Object.keys(peopleHasInterest)) {
      const semanticVector = <number[]> Array.from({ length: interests.length + universities.length }).fill(0);
      // First n elements semantic vector represent person's interests
      for (const interest of peopleHasInterest[person]) {
        semanticVector[indexMapInterests[interest.value]] = 1;
      }
      // Second m elements semantic vector represent university person studied at
      if (peopleStudyAt[person]) {
        for (const university of peopleStudyAt[person]) {
          semanticVector[indexMapUniversities[university.value] + interests.length] = 1;
        }
      }
      peopleSemanticVectors[person] = semanticVector;
    }
    return peopleSemanticVectors;
  }

  public async peopleSimilarities(
    peopleSemanticVectors: Record<string, number[]>,
    personToPost: Record<string, string[]>,
    personToComment: Record<string, string[]>,
  ): Promise<void> {
    const persons = Object.keys(peopleSemanticVectors);
    let i = 0;
    for (const person of persons) {
      // Transform the person IRI if a transformer is provided
      let personTransformed = person;
      if (this.personTransformer) {
        personTransformed = this.personTransformer.transformTerm(person);
      }
      if (i % 10 === 0) {
        process.stdout.write(`\rSimilarities calculated: ${i}`);
      }
      const similaritiesPeople: IEntitySimilarity[] = [];
      const similaritiesPosts: IEntitySimilarity[] = [];
      const similaritiesComments: IEntitySimilarity[] = [];
      for (const pair of persons) {
        let pairTransformed = pair;
        if (this.personTransformer) {
          pairTransformed = this.personTransformer.transformTerm(pair);
        }
        if (personTransformed !== pairTransformed) {
          const similarityPerson = this.cosineSimilarity(
            peopleSemanticVectors[person],
            peopleSemanticVectors[pair],
          );
          similaritiesPeople.push(
            {
              entity: pairTransformed,
              similarity: similarityPerson,
            },
          );
          // Some people have no posts
          if (personToPost[pair]) {
            for (const post of personToPost[pair]) {
              similaritiesPosts.push(
                { entity: post, similarity: similarityPerson },
              );
            }
          }
          if (personToComment[pair]) {
            for (const comment of personToComment[pair]) {
              similaritiesComments.push(
                { entity: comment, similarity: similarityPerson },
              );
            }
          }
        }
      }
      const similaritiesPeopleSorted = this.sortAndTruncateSimilarities(similaritiesPeople);
      const similaritiesPostsSorted = this.sortAndTruncateSimilarities(similaritiesPosts);
      const similaritiesCommentsSorted = this.sortAndTruncateSimilarities(similaritiesComments);

      await this.parameterEmitterSimilaritiesPeople.waitForDrain(
        this.parameterEmitterSimilaritiesPeople.emitRow(
          [ personTransformed, JSON.stringify(similaritiesPeopleSorted) ],
        ),
      );
      await this.parameterEmitterSimilaritiesPosts.waitForDrain(
        this.parameterEmitterSimilaritiesPosts.emitRow([ personTransformed, JSON.stringify(similaritiesPostsSorted) ]),
      );
      await this.parameterEmitterSimilaritiesComments.waitForDrain(
        this.parameterEmitterSimilaritiesComments.emitRow(
          [ personTransformed, JSON.stringify(similaritiesCommentsSorted) ],
        ),
      );
      i++;
    }
    this.parameterEmitterSimilaritiesPeople.flush();
    this.parameterEmitterSimilaritiesPosts.flush();
    this.parameterEmitterSimilaritiesComments.flush();
  }

  public cosineSimilarity(a: number[], b: number[]): number {
    const dot = a.reduce((sum, ai, i) => sum + ai * b[i], 0);
    const normA = Math.sqrt(a.reduce((sum, ai) => sum + ai * ai, 0));
    const normB = Math.sqrt(b.reduce((sum, bi) => sum + bi * bi, 0));
    return dot / (normA * normB);
  }

  public sortAndTruncateSimilarities(similarityArray: IEntitySimilarity[]): IEntitySimilarity[] {
    // Sort in descending order in terms of similarities
    similarityArray.sort((a, b) => b.similarity - a.similarity);
    // Ensure a maximum number of similarities is stored, as storage is N^2 with
    // N the number of entities.
    return similarityArray.slice(0, this.maxSimilarities);
  }
}

export interface IEnhancerSimilarityOptions {
  /**
   * Path to an LDBC SNB RDF persons dataset file.
   */
  personsPath: string;
  /**
   * Path to an LDBC SNB RDF activities dataset file.
   */
  activitiesPath: string;
  /**
   * Path to the output destination file.
   */
  destinationPathData: string;
  /**
   * Data selector.
   */
  dataSelector: IDataSelector;
  /**
   * Parameter emitter that emits the calculated similarities for people
   */
  parameterEmitterSimilaritiesPeople: IParameterEmitter;
  /**
   * Parameter emitter that emits the calculated similarities for posts
   */
  parameterEmitterSimilaritiesPosts: IParameterEmitter;
  /**
   * Parameter emitter that emits the calculated similarities for comments
   */
  parameterEmitterSimilaritiesComments: IParameterEmitter;

  /**
   * Enhancement handlers.
   */
  handlers?: IEnhancementHandlerSimilarity[];
  /**
   * Logger.
   */
  logger?: ILogger;
  /**
   * An optional parameter emitter for all available posts.
   */
  parameterEmitterPosts?: IParameterEmitter;
  /**
   * An optional parameter emitter for all available comments.
   */
  parameterEmitterComments?: IParameterEmitter;
  /**
   * Maximum number of similarities to save.
   * This will safe the highest similarities
   */
  maxSimilarities?: number;
  /**
   * Transformer to replace IRIs in for all people.
   */
  personTransformer?: TransformerReplaceIri;
}

export interface IEntitySimilarity {
  entity: string;
  similarity: number;
}
