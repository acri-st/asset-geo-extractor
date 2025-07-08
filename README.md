# Asset Geo Extractor


## Table of Contents

- [Introduction](#Introduction)
- [Prerequisites](#prerequisites)
- [Installation](#installation)
- [Development](#development)
- [Contributing](#contributing)

## Introduction

### What is the Asset Geo Extractor microservice?

The Asset Geo Extractor is a microservice designed to extract and process geographical data from various asset sources. It provides functionality to:

- **Extract geographical coordinates** from asset metadata and content
- **Process and validate** geographic data using GeoJSON standards
- **Integrate with Elasticsearch** for efficient data storage and retrieval
- **Support async operations** for high-performance data processing
- **Handle multiple asset formats** and data sources

This microservice is part of a larger system architecture and works in conjunction with other services like the Search microservice to interact with elasticsearch, and an asset manager to provide comprehensive asset management capabilities.


## Prerequisites

Before you begin, ensure you have the following installed:
- **Git** 
- **Docker** Docker is mainly used for the test suite, but can also be used to deploy the project via docker compose
- **ElasticSearch** So it can use it to operate the searchs

## Installation

1. Clone the repository:
```bash
git clone https://github.com/acri-st/asset-geo-extractor.git asset-geo-extractor
cd asset-geo-extractor
```

## Development

## Development Mode

### Standard local development

Setup environment
```bash
make setup
```

To clean the project and remove node_modules and other generated files, use:
```bash
make clean
```

## Contributing

Check out the **CONTRIBUTING.md** for more details on how to contribute.