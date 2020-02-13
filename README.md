- Badges

 [![Conventional Commits](https://img.shields.io/badge/Conventional%20Commits-1.0.0-yellow.svg)](https://conventionalcommits.org)

- Build

Drone:

[![Build Status](https://cloud.drone.io/api/badges/astubbs/ks-tributary/status.svg)](https://cloud.drone.io/astubbs/ks-tributary)

CodeCov.io:

[![codecov](https://codecov.io/gh/astubbs/ks-tributary/branch/master/graph/badge.svg)](https://codecov.io/gh/astubbs/ks-tributary)

# KS-Tributary; A Kafka Streams Reference App

Realistic implementation of a full real Kafka Streams application and stream processing logic.

  * [How to run](#how-to-run)
     * [Maven](#maven)
  * [How to build](#how-to-build)
  * [Features](#features)
     * [Stream Processing](#stream-processing)
        * [Generic](#generic)
        * [Finance](#finance)
        * [Chat](#chat)
     * [Unit testing](#unit-testing)
     * [Integration testing](#integration-testing)
     * [Software Structure](#software-structure)
  * [Features on their way...](#features-on-their-way)
     * [Stream Processing](#stream-processing-1)
     * [Software Structure](#software-structure-1)
  * [Development](#development)
     * [Building](#building)
     * [Contributing](#contributing)

## How to run

Integration tests require a running docker daemon.

Intellij - use provided run targets to run with or without integration tests

### Maven

Disable integration tests

    mvn clean verify -DskipITs

## How to build

## Features

### Stream Processing

#### Generic
- Realtime delta calculation (e.g. produce derived delta messages from database CDC full state messages)
- Cached system configuration lookup with realtime updates
- Realtime fan out of topic data onto _communication channels_ like web sockets
- Dynamic Avro wrapper system to extend Avro objects with custom code without modifying generated code, integrated with Avro Serdes 

#### Finance
- Realtime calculation of windows 
 - Three topologies
   - Financial snap set precomputation
   - Simpler approach that's centralised performance wise
     - More sophisticated fully distributed
     - Custom state store intermediate level demonstration
- Simple high low bar calculation
- Back loading of data from previous buckets
 

#### Chat
- Transcript generation


### Unit testing
- Demonstration of Time model decoupling for test
- Demonstration of AssertJ powerful assertions
  - here
  - here

### Integration testing
- Model hydration using human readable data from JavaFaker[] project
  - Deterministic generation of series of random data, useful for larger integration tests
- TestContainers for Integration Testing with real Kafka brokers.
- CI service integration: Travis, CircleCI, Drone (drone not working with Docker in Docker (i.e. TestContainers not working))
- JSR Money integration
- Avro message sizing analysis - i.e. how big is my data when it's bin packed and compressed? Compared with Gzipped vs Zstd vs Json?
- Demonstration of asynchronous testing using Awaitility[]


### Software Structure
- Guice (dynamic) and Dagger (static) dependency injection examples
- Dagger
- Java Money library demonstration
- Java Units of Measure library demonstration (statically typed Scientific units combination library)
- Lombok use (make Java more tolerable)
- Avro IDL vs AVSC model generation demonstration


## Features on their way...
### Stream Processing
- Realtime out of order recalculation of data aggregation in state dependent buckets
- Massive scale question and answer quiz show of just in time answer discovery
  - Massive skew avoidance techniques
- Compound key query demonstration / data demoralisation (e.g. Invoice reconstruction from Orders and Order Items)
- Yearly aggregator by date (think April 5th Financial years)
  - Normal window computations can only run against fixed size windows (e.g. not calendar dates)
  - Extending to also do monthly dates is an easy modification 

### Software Structure
- External Interactive Query example on Qurkus[] and GraalVM[]

[0.7]
Dagger

[0.6]
0.5
0.4
0.3
0.2
[0.1]
- Three topologies
  - Financial snap set precomputation
  - Simpler approach that's centralised performance wise

    - More sophisticated fully distributed
        - Custom state store intermediate level demonstration
- Unit testing coverage
  - Guice test wiring 
- Separated Integration testing
  - Kafka ContainerTests
- Avro message model sizing analysis
  - Raw bin-packed vs GZipped vs Json
- Java Science Units and Quantities use

## Development

### Building

- Requires Lombok plugin for your IDE

### Contributing
Submit a PR for review :)

https://www.conventionalcommits.org/en/v1.0.0/
 [the Angular convention](https://github.com/angular/angular/blob/22b96b9/CONTRIBUTING.md#-commit-message-guidelines)
 
 
[val-lombok]: https://www.projectlombok.org/features/val

[rbenv]: https://github.com/rbenv/rbenv
[ruby-version]: .ruby-version
[source]: source/
[pull-request]: https://help.github.com/articles/creating-a-pull-request/
[fork]: https://help.github.com/articles/fork-a-repo/
[version-badge]: https://img.shields.io/badge/version-1.1.0-blue.svg
[license-badge]: https://img.shields.io/badge/license-MIT-blue.svg
