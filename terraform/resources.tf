# USER DEFINED FUNCTION FOR PDF_TO_TEXT

resource "snowflake_function" "pdf_to_text" {
  name     = "EXTRACT_TEXT_PDF"
  database = snowflake_database.audit_db.name
  schema   = snowflake_schema.schemas["raw_schema"].name
  
  arguments {
    name = "file_url"
    type = "STRING"
  }
  
  return_type     = "STRING"
  language        = "python"
  runtime_version = "3.10"
  packages        = ["snowflake-snowpark-python", "pypdf2"]
  handler         = "extraer"
  
  statement = <<-EOT
    import PyPDF2
    from snowflake.snowpark.files import SnowflakeFile

    def extraer(file_url):
        try:
            with SnowflakeFile.open(file_url, 'rb') as f:
                reader = PyPDF2.PdfReader(f)
                texto_completo = ""
                for page in reader.pages:
                    texto = page.extract_text()
                    if texto:
                        texto_completo += texto + "\n"
                return texto_completo
        except Exception as e:
            return f"ERROR_PDF: {str(e)}"
  EOT
}

# CERTIFICATES TABLE CREATION

resource "snowflake_table" "certificates_text_base" {
  database = snowflake_database.audit_db.name
  schema   = snowflake_schema.schemas["raw_schema"].name
  name     = "CERTIFICATES_TEXT_BASE"
  comment  = "Base raw table with extracted text from certificates"

  column { 
    name = "pdf_filename"
    type = "STRING" 
    }
  column { 
    name = "file_date"
    type = "TIMESTAMP_LTZ" 
    }
  column { 
    name = "raw_text"
    type = "STRING" 
    }
}