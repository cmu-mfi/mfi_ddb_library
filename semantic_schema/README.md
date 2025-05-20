# Semantic Metadata Schema

This repository defines a semantic metadata model using JSON-LD and JSON Schema. It enables structured metadata exchange via a knowledge graph-compatible format.

## Core Requirements

- `@context` (required): Declares the JSON-LD context.
- `project_id` (required): Unique identifier for the metadata payload project, to link all trials to this.
- `trial_id` (required): Primary identifier for the metadata payload.
- Optional:  `extension` pointer to domain-specific JSON-LD schemas.

## Usage

Use `core/schema.json` for validation and `core/context.jsonld` to interpret metadata semantically. Extensions can be loaded as needed per domain (e.g., robotics, AM).

Creation of new project UID, can be done in SQL, then returned as response?
mfi-1.0-kv/cmu/mill19/mezzanine-lab/yk_destoryer/projects/{urn:uuid}/metadata
Once Project_ID is returned, then it is added to the json-ld file that can then continously be reused
The reason for the Project_ID linking here, is that makes it easier for the user and does not have to do it later.
mfi-1.0-kv/cmu/mill19/mezzanine-lab/yk_destoryer/projects/{urn:uuid}/trials/metadata

