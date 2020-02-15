# GitDetective
> Find the use and users of open-source code

[![Release](https://img.shields.io/github/release/CodeBrig/GitDetective.svg)](https://github.com/CodeBrig/GitDetective/releases/latest)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)

![gitdetective](https://i.imgur.com/gSdJbZH.png)

GitDetective is an open-source web application for finding the use and users of open-source code.
GitDetective achieves this by compiling open-source code with Kythe and indexing references found in the compiliation units. GitDetective then supresses internal references (referring to source code within the project) and extracts external references (refering to source code outside the project). This data is then collected and organized into a knowledge graph powered by Grakn.

GitDetective consists of two separate modules:

## Indexer

The indexer module contains source code builders and integration with Kythe to produce the external references to be imported into Grakn. The indexer scales horizontally, are deployed in a cluster, and are responsible for:
- cloning projects
- building projects with Kythe
- extracting Kythe data related to external references
- augmenting Kythe data with additional information
- filtering out definition/reference data already imported
- sending indexed project to web module for importing

### Code Structure

- [`indexer`](https://github.com/CodeBrig/GitDetective/tree/master/indexer/src/main/groovy/io/gitdetective/indexer) -- Build/extract source code data
  - [`stage`](https://github.com/CodeBrig/GitDetective/tree/master/indexer/src/main/groovy/io/gitdetective/indexer/stage) -- Indexer work stages
  - [`support`](https://github.com/CodeBrig/GitDetective/tree/master/indexer/src/main/groovy/io/gitdetective/indexer/support) -- Source code builder support

## Web

The web module contains a Bootstrap website which is generated by the Handlebars template engine and provides a way to view data collected by the indexer module. The web is deployed as a single instance and is responsible for:
- creating indexing jobs (based on user activity)
- importing indexed projects into Grakn
- caching imported definitions/references and unique Kythe paths
- displaying external references between projects

### Code Structure

- [`web`](https://github.com/CodeBrig/GitDetective/tree/master/web/src/main/groovy/io/gitdetective/web) -- Website/backend service functionality
  - [`dao`](https://github.com/CodeBrig/GitDetective/tree/master/web/src/main/groovy/io/gitdetective/web/dao) -- Data access
  - [`work`](https://github.com/CodeBrig/GitDetective/tree/master/web/src/main/groovy/io/gitdetective/web/work) -- Import source code reference data
- [`resources`](https://github.com/CodeBrig/GitDetective/tree/master/web/src/main/resources) -- Website/service resources
  - [`queries`](https://github.com/CodeBrig/GitDetective/tree/master/web/src/main/resources/queries) -- Graql/SQL queries
  - [`webroot`](https://github.com/CodeBrig/GitDetective/tree/master/web/src/main/resources/webroot) -- Bootstrap + Handlebars (Mustache) website

## Development Setup

Coming soon

## Contributing

1. Fork it (<https://github.com/yourname/yourproject/fork>)
2. Create your feature branch (`git checkout -b feature/fooBar`)
3. Commit your changes (`git commit -am 'Add some fooBar'`)
4. Push to the branch (`git push origin feature/fooBar`)
5. Create a new Pull Request

## Meta

Brandon Fergerson – [@fergerson92](https://twitter.com/fergerson92) – brandon.fergerson@codebrig.com

This project was created to test Grakn and Kythe in the context of processing large amounts of source code.
The idea came from the thought that it would be nice to **really know** how your source code is being used in the wild. Who uses function/feature X? Who's the biggest users/companies of function/feature X? Who was the first to use function/feature x? Etc.


If you would like to see GitDetective continue to comb the desert of open source code please consider sponsoring continued work on this project.

## License
[Apache 2.0](https://github.com/CodeBrig/GitDetective/LICENSE)
