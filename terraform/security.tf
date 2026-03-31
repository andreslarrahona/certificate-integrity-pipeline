resource "snowflake_account_role" "airflow_role" { 
  name = "AIRFLOW_ROLE"
}

resource "snowflake_account_role" "dbt_role" { 
  name = "DBT_ROLE"
}

variable "snowflake_user" {
  description = "Primary Snowflake user account authorized to manage infrastructure and execute data pipelines."
  type        = string
}

resource "snowflake_grant_account_role" "user_grants" {
  for_each  = toset([snowflake_account_role.airflow_role.name, snowflake_account_role.dbt_role.name])
  role_name = each.value
  user_name = var.snowflake_user
}

resource "snowflake_grant_privileges_to_account_role" "db_usage" {
  for_each          = toset([snowflake_account_role.airflow_role.name, snowflake_account_role.dbt_role.name])
  privileges        = ["USAGE"]
  account_role_name = each.value
  
  on_account_object {
    object_type = "DATABASE"
    object_name = snowflake_database.audit_db.name
  }
  depends_on = [snowflake_database.audit_db]
}


resource "snowflake_grant_privileges_to_account_role" "airflow_schema_raw" {
  privileges        = ["USAGE", "CREATE TABLE"]
  account_role_name = snowflake_account_role.airflow_role.name
  
  on_schema {
    schema_name = "${snowflake_database.audit_db.name}.${local.schemas["raw_schema"]}"
  }
  depends_on = [snowflake_schema.schemas]
}

resource "snowflake_grant_privileges_to_account_role" "airflow_stage" {
  privileges        = ["READ", "WRITE"]
  account_role_name = snowflake_account_role.airflow_role.name
  
  on_schema_object {
    object_type = "STAGE"
    object_name = "${snowflake_database.audit_db.name}.${local.schemas["raw_schema"]}.${snowflake_stage.pdf_stage.name}"
  }
  depends_on = [
    snowflake_schema.schemas,
    snowflake_stage.pdf_stage
  ]
}

resource "snowflake_grant_privileges_to_account_role" "airflow_function" {
  privileges        = ["USAGE"]
  account_role_name = snowflake_account_role.airflow_role.name
  
  on_schema_object {
    object_type = "FUNCTION"
    object_name = "${snowflake_database.audit_db.name}.${local.schemas["raw_schema"]}.${snowflake_function.pdf_to_text.name}(STRING)"
  }
  depends_on = [
    snowflake_schema.schemas,
    snowflake_function.pdf_to_text
  ]
}



resource "snowflake_grant_privileges_to_account_role" "dbt_schema_usage" {
  for_each = local.schemas
  
  privileges        = ["USAGE", "CREATE TABLE", "CREATE VIEW"]
  account_role_name = snowflake_account_role.dbt_role.name
  
  on_schema {
    schema_name = "${snowflake_database.audit_db.name}.${each.value}"
  }
  depends_on = [snowflake_schema.schemas]
}


resource "snowflake_grant_privileges_to_account_role" "dbt_tables_select_all" {
  for_each          = local.schemas
  privileges        = ["SELECT"]
  account_role_name = snowflake_account_role.dbt_role.name
  
  on_schema_object {
    all {
      object_type_plural = "TABLES"
      in_schema          = "${snowflake_database.audit_db.name}.${each.value}"
    }
  }
  depends_on = [snowflake_schema.schemas]
}

resource "snowflake_grant_privileges_to_account_role" "dbt_tables_select_future" {
  for_each          = local.schemas
  privileges        = ["SELECT"]
  account_role_name = snowflake_account_role.dbt_role.name
  
  on_schema_object {
    future {
      object_type_plural = "TABLES"
      in_schema          = "${snowflake_database.audit_db.name}.${each.value}"
    }
  }
  depends_on = [snowflake_schema.schemas]
}

resource "snowflake_grant_privileges_to_account_role" "dbt_views_select_all" {
  for_each          = local.schemas
  privileges        = ["SELECT"]
  account_role_name = snowflake_account_role.dbt_role.name
  
  on_schema_object {
    all {
      object_type_plural = "VIEWS"
      in_schema          = "${snowflake_database.audit_db.name}.${each.value}"
    }
  }
  depends_on = [snowflake_schema.schemas]
}

resource "snowflake_grant_privileges_to_account_role" "dbt_views_select_future" {
  for_each          = local.schemas
  privileges        = ["SELECT"]
  account_role_name = snowflake_account_role.dbt_role.name
  
  on_schema_object {
    future {
      object_type_plural = "VIEWS"
      in_schema          = "${snowflake_database.audit_db.name}.${each.value}"
    }
  }
  depends_on = [snowflake_schema.schemas]
}
