import dlt
from pyspark.sql import functions as F
from schema import schema_raw_consumos

SOURCE_TABLE = "dev.transient.source_consumos"
TARGET_TABLE = "dev.raw.consumos"
TARGET_TABLE_DLT_NAME = "raw_consumos"

@dlt.table(
    name="consumos_transient_stream",
    comment=f"Lê a tabela {SOURCE_TABLE}, prepara colunas de controle e aplica o schema explícito.",
    temporary=True,
    schema=schema_raw_consumos
)
def consumos_transient_stream():
    processing_time = F.current_timestamp()
    return (
        spark.readStream.table(SOURCE_TABLE)
        .withColumn("insert_date", processing_time)
        .withColumn("update_date", processing_time)
    )

@dlt.table(
    name = TARGET_TABLE_DLT_NAME,
    comment = f"Tabela Raw de Consumos (SCD Type 1) ingerida da {SOURCE_TABLE}",
    table_properties = {"layer": "raw"}
)
def raw_consumos_target():
    return dlt.apply_changes(
        target = TARGET_TABLE,
        source = "consumos_transient_stream",
        keys = ["consumo_id"],
        sequence_by = "update_date", # <-- CORREÇÃO ADICIONADA
        except_column_list = ["insert_date"]
    )