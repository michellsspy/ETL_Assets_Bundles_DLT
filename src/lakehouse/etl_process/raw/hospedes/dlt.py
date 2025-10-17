import dlt
from pyspark.sql import functions as F
from schema import schema_raw_hospedes

SOURCE_TABLE = "dev.transient.source_hospedes"
TARGET_TABLE = "dev.raw.hospedes"
TARGET_TABLE_DLT_NAME = "raw_hospedes"

@dlt.table(
    name="hospedes_transient_stream",
    comment=f"Lê a tabela {SOURCE_TABLE}, prepara colunas de controle e aplica o schema explícito.",
    temporary=True,
    schema=schema_raw_hospedes
)
def hospedes_transient_stream():
    processing_time = F.current_timestamp()
    return (
        spark.readStream.table(SOURCE_TABLE)
        .withColumn("insert_date", processing_time)
        .withColumn("update_date", processing_time)
    )

@dlt.table(
    name = TARGET_TABLE_DLT_NAME,
    comment = f"Tabela Raw de Hóspedes (SCD Type 1) ingerida da {SOURCE_TABLE}",
    table_properties = {"layer": "raw"}
)
def raw_hospedes_target():
    return dlt.apply_changes(
        target = TARGET_TABLE,
        source = "hospedes_transient_stream",
        keys = ["hospede_id"],
        sequence_by = "update_date", # <-- CORREÇÃO ADICIONADA
        except_column_list = ["insert_date"]
    )