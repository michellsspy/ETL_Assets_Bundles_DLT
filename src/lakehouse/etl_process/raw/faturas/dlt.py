import dlt
from pyspark.sql import functions as F
from schema import schema_raw_faturas

SOURCE_TABLE = "dev.transient.source_faturas"
TARGET_TABLE = "dev.raw.faturas"
TARGET_TABLE_DLT_NAME = "raw_faturas"

@dlt.table(
    name="faturas_transient_stream",
    comment=f"Lê a tabela {SOURCE_TABLE}, prepara colunas de controle e aplica o schema explícito.",
    temporary=True,
    schema=schema_raw_faturas
)
def faturas_transient_stream():
    processing_time = F.current_timestamp()
    return (
        spark.readStream.table(SOURCE_TABLE)
        .withColumn("insert_date", processing_time)
        .withColumn("update_date", processing_time)
    )

@dlt.table(
    name = TARGET_TABLE_DLT_NAME,
    comment = f"Tabela Raw de Faturas (SCD Type 1) ingerida da {SOURCE_TABLE}",
    tags = {"layer": "raw"}
)
def raw_faturas_target():
    return dlt.apply_changes(
        target = TARGET_TABLE,
        source = "faturas_transient_stream",
        keys = ["fatura_id"],
        except_column_list = ["insert_date"]
    )