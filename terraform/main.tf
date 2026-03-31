terraform {
  required_providers {
    snowflake = {
      source  = "snowflakedb/snowflake"
      version = "~> 0.90"
    }
  }
}

provider "snowflake" {}

# DATABASE
resource "snowflake_database" "audit_db" {
  name         = "ISO17025_AUDIT_SYSTEM"
  is_transient = false
}

# SCHEMAS
locals {
  schemas = {
    staging = "STAGING"
    intermediate = "INTERMEDIATE"
    analytics = "ANALYTICS"
    raw_schema = "RAW_DATA"
  }
}

resource "snowflake_schema" "schemas" {
  for_each = local.schemas

  database = snowflake_database.audit_db.name
  name = each.value
}

# STAGE

resource "snowflake_stage" "pdf_stage" {
  name        = "CERTIFICATES_STAGE"
  database    = snowflake_database.audit_db.name
  schema      = snowflake_schema.schemas["raw_schema"].name
  directory   = "ENABLE = TRUE"
  encryption  = "TYPE = 'SNOWFLAKE_SSE'"
}
