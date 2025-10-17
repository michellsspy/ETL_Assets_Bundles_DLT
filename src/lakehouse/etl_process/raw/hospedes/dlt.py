import dlt
from pyspark.sql import functions as F
# Importa o schema que acabamos de definir
from etl_assets_bundles_dlt.lakehouse.etl_process.raw.hospedes.schema import schema_raw_hospedes

# Define os nomes das tabelas de origem (transient) e destino (raw)
SOURCE_TABLE = "dev.transient.source_hospedes"
TARGET_TABLE = "dev.raw.hospedes"
TARGET_TABLE_DLT_NAME = "raw_hospedes" # Nome lógico usado no DLT

@dlt.table(
    name="hospedes_transient_stream",
    comment=f"Lê a tabela {SOURCE_TABLE} e prepara colunas de controle para o MERGE.",
    temporary=True
)
def hospedes_transient_stream():
    """
    Prepara o stream de dados da origem (source_hospedes).

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
    target = TARGET_TABLE, # O nome completo da tabela final (ex: dev.raw.hospedes)
    source = "hospedes_transient_stream", # O nome da tabela DLT temporária acima
    keys = ["hospede_id"], # A Chave Primária para o MERGE
    
    # Garante que o 'insert_date' original seja mantido em atualizações
    except_column_list = ["insert_date"],
    
    # Garantimos que a tabela seja criada com o schema correto
    schema = schema_raw_hospedes, 
    
    # Metadados da tabela no DLT
    name = TARGET_TABLE_DLT_NAME, # O nome que aparecerá no grafo do DLT
    comment = f"Tabela Raw de Hospedes (SCD Type 1) ingerida da {SOURCE_TABLE}"
)