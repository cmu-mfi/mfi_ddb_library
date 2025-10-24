# Metadata Store
## Overview
The Metadata Store is the central, authoritative database for tracking the complete lifecycle of our experiments (referred to as "trials") and the projects they belong to. Its primary purpose is to provide a durable, queryable, and auditable record of every trial from its creation to its completion.

## Core concepts
The data model is built around two core entities:
1. `Project`: Represents a high-level research initiative. Each project has its own set of details and can contain multiple trials.
2. `Trial`: Represents a single, unique experiment that belongs to a project. A trial's lifecycle is captured through birth and death timestamps, and its status is tracked with a clean_exit flag.

Each project and trial contains a flexible JSON payload, allowing us to record rich, semi-structured data without being constrained by a rigid schema.

## Architecture
The metadata store is implemented using a hybrid model in PostgreSQL, combining the reliability of a relational database with the flexibility of a document store.

### Technology Stack
Database: PostgreSQL

Data Type for Metadata: JSONB

### Database Schema

The schema consists of two primary tables, `project_detail` and `trial_detail`, to ensure data integrity for the core entities. Flexibility is provided by JSONB columns in both tables, which store semi-structured metadata for projects and trials. For ER Diagram [click here](https://editor.plantuml.com/uml/lPBVIyCm4CVVyrSSVTgA6ohiPOonJZhyqRLZhGg-bDYS64swabx1CVQ_cxPqu6GeFfY71Ew-b-JplPkLn0rLMh7oNUO5Drn3ILk5TZSo8yOm9qbRACpc3JDA1HAN2dOCx7B1TRk45AuBOtZmrbVNthftEHhrOJ9PtKsdZNGmQ8wSQpnIDV7C82SKAIURJMwMVfnuorNo1BimIY2y3u8p4FZ2AqKGHe-z_hufgmhnbx8MehGrjt4Kpjd-W6cXkVeEsOP_X_YJ9OjE_omDlQOaDTecwE8KGVTbVbhSMgYGvob-oDgBUHG5lXV2hgDVU47ijrTfIsTjumVy_ss0DVjec9mBnve7plbmw3fVMp06Wwf-0MZ3PfYBUbG_0G00)

#### Table `project_detail`
```
CREATE TABLE project_detail (
    project_id       VARCHAR(255) PRIMARY KEY,
    name      	     VARCHAR(255),
    created_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    details	         JSONB
);
```

#### Table `trial_detail`
```
CREATE TABLE trial_detail (
    trial_id         VARCHAR(255) PRIMARY KEY,
    project_id       VARCHAR(255) REFERENCES project_detail(project_id),
    birth_timestamp  TIMESTAMPTZ  NOT NULL,
    death_timestamp  TIMESTAMPTZ,
    clean_exit	     BOOLEAN DEFAULT FALSE,
    metadata         JSONB,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at       TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
```

#### Function `set_updated_at_timestamp` to be used in triggers for updating the column `updated_at` in different tables
```
CREATE OR REPLACE FUNCTION set_updated_at_timestamp()
 RETURNS TRIGGER AS $$
 BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
 END;
$$ LANGUAGE plpgsql;
```

#### Trigger `project_detail_set_updated_at` to update `updated_at` column in `project_detail` table
```
DROP TRIGGER IF EXISTS project_detail_set_updated_at ON project_detail;
CREATE TRIGGER project_detail_set_updated_at
BEFORE UPDATE ON project_detail
FOR EACH ROW
EXECUTE PROCEDURE set_updated_at_timestamp();
```

#### Trigger `trial_detail_set_updated_at` to update `updated_at` column in `trial_detail` table
```
DROP TRIGGER IF EXISTS trial_detail_set_updated_at ON trial_detail;
CREATE TRIGGER trial_detail_set_updated_at
BEFORE UPDATE ON trial_detail
FOR EACH ROW
EXECUTE PROCEDURE set_updated_at_timestamp();
```

## Dynamic Graph Layer
A key feature of the metadata store is its ability to represent a graph of interconnected data without requiring a separate graph database. This is achieved through a simple tagging syntax and a "Connector" process.
- **Tagging**: Users can create relationships by embedding tags directly within the metadata or details JSON fields.
  - **Categorical Tags**: `#project-phoenix`, `#calibration-run`
  - **Entity Links**: `trial:trial-abc-123` (links to another trial)
- **Connector**: A background process parses the JSON of new or updated records, identifies these tags, and populates the `graph_edges` table.

This approach transforms implicit references into explicit, queryable relationships, allowing us to perform graph-style queries using standard SQL.

### Table `graph_edges`
```
CREATE TABLE graph_edges (
    edge_id            UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    source_trial_id    VARCHAR(255) NOT NULL REFERENCES trial_detail(trial_id),
    target_entity_id   VARCHAR(255) NOT NULL,
    target_entity_type VARCHAR(50) NOT NULL --  ('trial', 'project', 'tag')
);
```
