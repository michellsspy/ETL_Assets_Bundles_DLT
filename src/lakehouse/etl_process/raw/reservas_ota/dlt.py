import dlt
from pyspark.sql import functions as F
from schema import schema_raw_reserva_ota

SOURCE_TABLE = "dev.transient.source_reserva_ota"
TARGET_TABLE = "dev.raw.reserva_ota"
TARGET_TABLE_DLT_NAME = "raw_reserva_ota"

@dlt.table(
    name="reserva_ota_transient_stream",
    comment=f"Lê a tabela {SOURCE_TABLE}, prepara colunas de controle e aplica o schema explícito.",
    temporary=True,
    schema=schema_raw_reserva_ota
)
def reserva_ota_transient_stream():
    processing_time = F.current_timestamp()
    return (
        spark.readStream.table(SOURCE_TABLE)
        .withColumn("insert_date", processing_time)
        .withColumn("update_date", processing_time)
    )

@dlt.table(
    name = TARGET_TABLE_DLT_NAME,
    comment = f"Tabela Raw de Reservas OTA (SCD Type 1) ingerida da {SOURCE_TABLE}",
    tags = {"layer": "raw"}
)
def raw_reserva_ota_target():
    return dlt.apply_changes(
        target = TARGET_TABLE,
        source = "reserva_ota_transient_stream",
        keys = ["ota_reserva_id"],
        except_column_list = ["insert_date"]
    )