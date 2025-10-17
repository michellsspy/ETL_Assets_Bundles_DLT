import dlt
from pyspark.sql import functions as F

# Esta linha deve funcionar agora
from schema import schema_raw_reservas_ota

# Define os nomes das tabelas de origem (transient) e destino (raw)
# ATENÇÃO: Assumindo que a tabela de origem se chama 'source_reserva_ota'
SOURCE_TABLE = "dev.transient.source_reserva_ota"
TARGET_TABLE = "dev.raw.reserva_ota"
TARGET_TABLE_DLT_NAME = "raw_reserva_ota" # Nome lógico usado no DLT

@dlt.table(
    name="reserva_ota_transient_stream",
    comment=f"Lê a tabela {SOURCE_TABLE} e prepara colunas de controle para o MERGE.",
    temporary=True
)
def reserva_ota_transient_stream():
    """
    Prepara o stream de dados da origem (source_reserva_ota).

    Esta função lê a tabela de origem e adiciona as colunas de auditoria
    com o timestamp atual.

    - 'insert_date': Recebe o timestamp atual (será usado APENAS no INSERT).
    - 'update_date': Recebe o timestamp atual (será usado no INSERT e no UPDATE).
    """
    processing_time = F.current_timestamp()
    
    return (
        spark.readStream.table(SOURCE_TABLE)
        .withColumn("insert_date", processing_time)
        .withColumn("update_date", processing_time)
    )

# Aplica as mudanças na tabela Raw (SCD Type 1)
dlt.apply_changes(
    target = TARGET_TABLE, # O nome completo da tabela final (ex: dev.raw.reserva_ota)
    source = "reserva_ota_transient_stream", # O nome da tabela DLT temporária acima
    keys = ["ota_reserva_id"], # A Chave Primária para o MERGE
    
    # Garante que o 'insert_date' original seja mantido em atualizações
    except_column_list = ["insert_date"],
    
    # Garantimos que a tabela seja criada com o schema correto
    schema = schema_raw_reserva_ota, 
    
    # Metadados da tabela no DLT
    name = TARGET_TABLE_DLT_NAME, # O nome que aparecerá no grafo do DLT
    comment = f"Tabela Raw de Reservas OTA (SCD Type 1) ingerida da {SOURCE_TABLE}"
)