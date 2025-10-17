import dlt
from pyspark.sql import functions as F
from schema import schema_raw_quartos

SOURCE_TABLE = "dev.transient.source_quartos"
TARGET_TABLE = "dev.raw.quartos"
TARGET_TABLE_DLT_NAME = "raw_quartos"

@dlt.table(
    name="quartos_transient_stream",
    comment=f"Lê a tabela {SOURCE_TABLE}, prepara colunas de controle e aplica o schema explícito.",
    temporary=True,
    schema=schema_raw_quartos
)
def quartos_transient_stream():
    processing_time = F.current_timestamp()
    return (
        spark.readStream.table(SOURCE_TABLE)
        .withColumn("insert_date", processing_time)
        .withColumn("update_date", processing_time)
    )

@dlt.table(
    name = TARGET_TABLE_DLT_NAME,
    comment = f"Tabela Raw de Quartos (SCD Type 1) ingerida da {SOURCE_TABLE}",
    table_properties = {"layer": "raw"}
)
def raw_quartos_target():
    return dlt.apply_changes(
        target = TARGET_TABLE,
        source = "quartos_transient_stream",
        keys = ["quarto_id"],
        sequence_by = "update_date", # <-- CORREÇÃO ADICIONADA
        except_column_list = ["insert_date"]
    )