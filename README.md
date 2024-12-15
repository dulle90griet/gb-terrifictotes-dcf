# Data Warehouse Pipeline for TerrificTotes

![coverage](./docs/images/coverage-badge.svg)

An automated, monitored ETL (Extract, Transform, Load) pipeline for fictional tote manufacturer TerrificTotes.

## Overview

TerrificTotes's existing commercial and production systems store data in a write-optimized PostgreSQL database, unsuited to querying by analysts. `gb-terrifictotes-dcf` spins up a complete ecosystem of AWS cloud services in order to process new data from this database at regular intervals. Data is: 

- ingested for storage in JSON files; 
- denormalized and transformed into a new, OLAP-friendly schema, saved in parquet format; and finally 
- loaded into a data warehouse, ready for querying and integration into BI dashboards.

Change history is maintained from the moment of the pipeline's first operation. All stages are monitored, and basic error reporting triggers email notifications on system failure.

## Team GreenBeans

[`gb-terrifictotes-solutions`](https://github.com/dulle90griet/gb-terrifictotes-solutions) (🔒) was developed in November 2024 by [@Rmbkh](https://github.com/Rmkbh), [@dulle90griet](https://github.com/dulle90griet), [@contiele1](https://github.com/contiele1), [@ali-shep](https://github.com/ali-shep) and [@Minalpatil3](https://github.com/Minalpatil3).

[`gb-terrifictotes-dcf`](https://github.com/dulle90griet/gb-terrifictotes-dcf) is a comprehensive refactoring of that project by [@dulle90griet](https://github.com/dulle90griet). For an overview of current progress, [see below](#refactor-goals).

## Refactor Roadmap

- 🚛 Create S3 bucket backup tool for pipeline migration | ✔️ Dec 3 2024
- 🔧 Create SQL script to initialize data warehouse | ✔️ Dec 4 2024
- 💚 Fix CI build making unusable layer zip | ✔️ Dec 4 2024
- ✅ Add missing tests on ingestion functions | ✔️ Dec 11 2024
- ♻️ Refactor and reorganise ingestion Lambda | ✔️ Dec 11 2024
- ✅ Add missing tests on processing functions | ✔️ Dec 14 2024
- ♻️ Refactor and reorganise processing Lambda | ✔️ Dec 14 2024
- 🚧 Add missing tests on uploading functions | 👷‍♂️ In progress
- 🚧 Refactor and reorganise uploading Lambda | 👷‍♂️ In progress
- Establish consistency of logging
- Rationalize nomenclature
- Remove all deprecated code and modules

## Prequisites

This project requires:

1. Python (3.9 <= version <= 3.12.4)

2. [Terraform](https://developer.hashicorp.com/terraform/tutorials/aws-get-started/install-cli) (developed using version 1.10.2)

3. An [AWS account](https://aws.amazon.com/free/)

4. [AWS credentials configured locally](https://docs.aws.amazon.com/cli/latest/userguide/cli-chap-configure.html), including access keys and default region

5. An [S3 bucket](https://aws.amazon.com/s3/) for remote storage of your Terraform state files

6. The [git CLI](https://git-scm.com/downloads)
