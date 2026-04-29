# Test Task

This repository contains two parts of the test task.

## Contents

- [Part 1 - Mini Marketing Pipeline](part1/README.md)
  - Python ETL pipeline for mocked Facebook Ads Insights data.
  - Reads local JSON, normalizes nested action arrays, writes JSONL, and includes
    a BigQuery schema and loader stub.
- [Part 2 - GA4 Raw Export Transformation Design](part2/README.md)
  - Lightweight transformation design for raw GA4 BigQuery export data.
  - Includes a reporting table structure, assumptions, tradeoffs, and
    representative BigQuery SQL.

## Notes

- Part 1 can be run locally. See the Part 1 README for setup and execution
  commands.
- Part 2 is a design-oriented deliverable. The SQL files use placeholder
  BigQuery table names and are intended to show the transformation approach.
