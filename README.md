# Certificate Integrity Pipeline

This pipeline detects discrepancies between issued calibration certificates and production records. It extracts structured data from PDFs using an LLM, compares it against the operational database, and flags non-conformities using dbt compliance rules, replacing manual spot-checks with automated daily coverage.

### Overview

  * **Problem:** ISO 17025 accreditation requires strict coherence between the operational database and the final printed PDF certificate delivered to the client. Currently, human auditors verify a limited sample. This leaves the vast majority of the population unaudited, exposing the laboratory to regulatory risk, invalid traceability claims, and compromised certification during external audits.

<br>

  * **Solution:** A daily pipeline that compares database records against the signed certificate content executes full population testing. It ingests the database state and the PDF files, **extracts the PDF text via a Python UDF, parses it into JSON using an LLM, and runs compliance rules against both datasets using dbt**.

<br>

  * **Impact:** The laboratory previously audited a sample before each external assessment. This pipeline runs the same checks daily against every issued certificate, flagging non-conformities as they appear instead of discovering them under audit pressure.

<br>

  * **Technical Detail:** The infrastructure requires Snowflake to execute Python and LLM inference directly where the data resides. The architecture strictly separates ingestion, LLM execution, and business logic to guarantee auditability, control API costs, and measure AI hallucinations.

  >**Architecture Note:** This project builds on the operational analytics platform documented [here](https://github.com/andreslarrahona/operational-analytics-platform). The stack moved from PostgreSQL on-premise to Snowflake to run LLM inference in-warehouse via Cortex AI, avoiding the need to move raw PDF content to an external API.



### AI Extraction Results

The pipeline uses Snowflake Cortex to extract structured data from certificate PDFs. Three models were evaluated against a manually verified sample of 30 certificates:

| Model | Serial # | Temperature | Humidity |
| :--- | :--- | :--- | :--- |
| llama3.1-8b | 76.7% | 76.7% | 73.3% |
| llama3.1-70b | 80.0% | 80.0% | 80.0% |
| mistral-large2 | 80.0% | 80.0% | 76.7% |

The pipeline runs on `llama3.1-70b`. It improves consistently over the 8b model across all three fields. `mistral-large2` matches on serial number but underperforms on humidity, making `llama3.1-70b` the more consistent choice.

---


## Architecture and Pipeline Breakdown

```mermaid
flowchart TD
    classDef source fill:#f8f9fa,stroke:#ced4da,stroke-width:1px,color:#495057;
    classDef platform fill:#ffffff,stroke:#343a40,stroke-width:2px,color:#212529;
    classDef logic fill:#e9ecef,stroke:#495057,stroke-width:1px,color:#212529;
    classDef audit fill:#ffffff,stroke:#000000,stroke-width:2px,color:#000000,stroke-dasharray: 4 4;
    classDef seed fill:#e2e3e5,stroke:#adb5bd,stroke-width:1px,color:#212529;

    subgraph Sources
        DB[(MySQL)]:::source
        PDF[PDF Server]:::source
    end

    subgraph SF_RAW[Snowflake RAW]
        RAW[(RAW Data)]:::platform
        STAGE[(Internal Stage)]:::platform
        TEXT[(Raw Text)]:::platform
    end

    subgraph DBT[dbt Staging]
        STG[stg_ordenes]:::logic
        EXTRACT[stg_pdf__extracted]:::logic
        PARSED[stg_pdf__parsed]:::logic
    end

    subgraph SEEDS[Seeds]
        LIMITS[\ambiental specs\]:::seed
        GOLDEN[\golden_dataset\]:::seed
    end

    subgraph ANALYTICS[Analytics]
        AUDIT{{fct_audit}}:::audit
        ACCURACY{{fct_ai_accuracy}}:::audit
    end

    DB -->|Airflow| RAW
    PDF -->|Airflow| STAGE
    STAGE -->|Python UDF| TEXT
    TEXT -->|Cortex AI| EXTRACT
    EXTRACT -->|Regex & Cast| PARSED
    RAW -->|dbt| STG
    STG --> AUDIT
    PARSED --> AUDIT
    PARSED --> ACCURACY
    LIMITS --> AUDIT
    GOLDEN --> ACCURACY

    style Sources fill:#f1f3f5,stroke:#ced4da,color:#495057
    style SF_RAW fill:#f1f3f5,stroke:#ced4da,color:#495057
    style DBT fill:#f1f3f5,stroke:#ced4da,color:#495057
    style SEEDS fill:#f1f3f5,stroke:#ced4da,color:#495057
    style ANALYTICS fill:#f1f3f5,stroke:#ced4da,color:#495057
```



### 1\. Infrastructure as Code (Terraform)

The Terraform configuration is split into three files by concern: `main.tf` provisions the database, schemas, stage, and UDF. `resources.tf` defines the raw text table where extracted PDF content lands. `security.tf` creates two service roles — `AIRFLOW_ROLE` and `DBT_ROLE` — and grants them the minimum privileges needed for each layer.

The schema separation (`RAW_DATA`, `STAGING`, `INTERMEDIATE`, `ANALYTICS`) is what makes the RBAC meaningful: Airflow can write to `RAW_DATA` but has no access to the transformation schemas, and dbt can read and write across all schemas but cannot touch the internal stage directly.


<details>
<summary style="font-weight:bold; cursor:pointer">View security.tf: role grants per schema</summary>

```terraform
resource "snowflake_grant_privileges_to_account_role" "airflow_schema_raw" {
  privileges        = ["USAGE", "CREATE TABLE"]
  account_role_name = snowflake_account_role.airflow_role.name

  on_schema {
    schema_name = "${snowflake_database.audit_db.name}.${local.schemas["raw_schema"]}"
  }
}

resource "snowflake_grant_privileges_to_account_role" "dbt_schema_usage" {
  for_each          = local.schemas
  privileges        = ["USAGE", "CREATE TABLE", "CREATE VIEW"]
  account_role_name = snowflake_account_role.dbt_role.name

  on_schema {
    schema_name = "${snowflake_database.audit_db.name}.${each.value}"
  }
}

```

</details>
<br>


### 2\. Data Ingestion (Airflow)

Data movement is orchestrated via Airflow DAGs.

  * **`dag_certificates.py`:** Connects via SSH to the server, diffs against the Snowflake stage, and runs an incremental SFTP upload to avoid reprocessing historical files.
  * **`dag_olap.py`:** Pulls operational tables from MySQL and writes to `RAW_DATA` via `write_pandas`.

### 3\. Unstructured Data Processing (Snowpark & Cortex AI)

The pipeline converts PDF text into structured relational data in two steps: LLM inference and format validation.

  * **`stg_pdf__extracted.sql` (LLM Inference):** Uses `SNOWFLAKE.CORTEX.COMPLETE` (Llama 3.1) with Few-Shot prompting to generate a strict JSON object from the raw text. This model is materialized as `incremental` with batch limits to prevent runaway API costs during daily dbt runs.
  * **`stg_pdf__parsed.sql` (Quality Triage):** A declarative view that unpacks the JSON. It uses safe casting (`TRY_TO_DOUBLE`) to handle VARIANT nulls and strict Regex to enforce business formats. It generates a `status_ia` flag (`OK`, `JSON_NULL`,`MISSING_KEY_DATA`, `WARN_TRUNCATED`, `INVALID_FORMAT`), acting as a bouncer that prevents hallucinated data from breaking downstream joins.

### 4\. Master Data Management (dbt Seeds)

Procedural thresholds and ground truth data are managed as dbt Seeds. `ambiental_limits.csv` and `ambiental_u.csv` store environmental limits and historical uncertainty authorizations.

`golden_dataset.csv` provides a manually verified sample of 30 certificates used to benchmark the AI's extraction accuracy.


### 5. The Bifurcation: Compliance vs. AI Accuracy

The `ANALYTICS` layer splits into two fact tables, separating business auditing from model validation.

* **Path A: MLOps Validation (`fct_ai_accuracy.sql`)**
  LLMs hallucinate, and this pipeline is no exception. In practice, the observed failure mode is serial numbers printed in non-linear layouts — columns, tables, rotated text — which Cortex tends to misread or skip. `fct_ai_accuracy` quantifies this against the golden dataset before any compliance result is trusted.

  </br>
  <details>
  <summary style="cursor:pointer">Benchmark results: Llama 3.1-70b against golden dataset (n=30)</summary>

  | Model | Serial # | Temperature | Humidity |
  | :--- | :--- | :--- | :--- |
  | llama3.1-8b | 76.7% | 76.7% | 73.3% |
  | llama3.1-70b | 80.0% | 80.0% | 80.0% |
  | mistral-large2 | 80.0% | 80.0% | 76.7% |

  **Error Analysis (llama3.1-70b, n=6 failures):**

  * **Total extraction failure (4 cases):** `certificado_2526`, `certificado_3999`, `certificado_2615`, `certificado_8424` — the model returned NULL across all fields. Likely caused by non-standard PDF layouts where text extraction via PyPDF2 produces malformed or unordered raw text, leaving the LLM without parseable input.
  * **Wrong value extracted (2 cases):** `certificado_4058` returned `3826417` instead of `E0583` — a numeric string present elsewhere in the document was substituted for the alphanumeric serial. `certificado_4898` returned `-6.7` for temperature instead of `10` — likely a delta or correction value printed adjacent to the nominal reading.
  * **Partial failure (1 case):** `certificado_6666` has no expected temperature or humidity in the golden dataset, so only serial number accuracy applies — the model returned NULL for a non-standard alphanumeric serial (`23JUL117124`).
  * **Rounding edge case (1 case):** `certificado_3333` humidity expected `28.9`, predicted NULL — the model likely extracted `29` and discarded the decimal, which TRY_TO_DOUBLE then failed to match.

  **Guardrail:** Records flagged as `ERR_JSON_NULL`, `ERR_MISSING_ID`, or `ERR_INVALID_FORMAT` by the `status_ia` triage are excluded from `fct_audit` and routed to manual review.

  </details>

<br>

* **Path B: ISO 17025 Compliance (`fct_audit.sql`)**
  Taking only certificates that passed the parsing triage, this table runs the actual audit against the MySQL operational records. It flags:
  * **Segregation of duties:** Did the same user calibrate and approve the certificate?
  * **Process violations:** Are there orders without an entry note that aren't on-site calibrations?
  * **Data mismatches:** Do dates and serial numbers match between PDF and database?
  * **Procedural limits:** Are environmental conditions outside the authorized limits defined in the seeds?

</br>

![dbt Lineage Graph](assets/lineage.png)
*dbt docs lineage graph — two independent paths converging on `fct_audit`, 
with `fct_ai_accuracy` isolated from the compliance output.*

## Repository Structure

```text
├── README.md
├── dags/
│   ├── dag_certificates.py
│   └── dag_olap.py
├── dbt_project/
|   ├── dbt_project.yml
|   ├── profiles.yml
│   ├── seeds/
│   │   ├── ambiental_limits.csv       # ISO procedure environmental limits
│   │   ├── ambiental_u.csv            # Historical uncertainty thresholds
│   │   └── golden_dataset.csv         # Ground truth sample for AI evaluation
│   ├── models/
│   |   ├── staging/
│   |   │   ├── stg_mysql/
|   |   |   |      ├── stg_brands.sql
|   |   |   |      ├── stg_customer_instruments.sql
|   |   |   |      ├── stg_models.sql
|   |   |   |      ├── stg_order_state_changes.sql
|   |   |   |      └── stg_orders.sql
│   |   │   └── stg_pdf/
│   |   │       ├── stg_pdf__extracted.sql  # Incremental LLM batching
│   |   │       └── stg_pdf__parsed.sql     # JSON flattening & Quality Triage
│   |   ├── intermediate/
|   |   |   └── int_orders_pivoted.sql
│   |   ├── analytics/
│   |   |   ├── dim_instruments.sql
│   |   |   ├── fct_audit.sql               # ISO 17025 Compliance Matrix
│   |   |   └── fct_ai_accuracy.sql         # MLOps Accuracy Measurement
│   |   └── sources.yml
│   └── tests/
└── terraform/
    ├── main.tf
    ├── resources.tf
    └── security.tf


```
