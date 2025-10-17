import dlt
from pyspark.sql import functions as F

# Esta linha deve funcionar agora
from schema import schema_raw_faturas

# Define os nomes das tabelas de origem (transient) e destino (raw)
SOURCE_TABLE = "dev.transient.source_faturas"
TARGET_TABLE = "dev.raw.faturas"
TARGET_TABLE_DLT_NAME = "raw_faturas" # Nome lógico usado no DLT

@dlt.table(
    name="faturas_transient_stream",
    comment=f"Lê a tabela {SOURCE_TABLE} e prepara colunas de controle para o MERGE.",
    temporary=True
)
def faturas_transient_stream():
    """
    Prepara o stream de dados da origem (source_faturas).

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
    target = TARGET_TABLE, # O nome completo da tabela final (ex: dev.raw.faturas)
    source = "faturas_transient_stream", # O nome da tabela DLT temporária acima
    keys = ["fatura_id"], # A Chave Primária para o MERGE
    
    # Garante que o 'insert_date' original seja mantido em atualizações
    except_column_list = ["insert_date"],
    
    # Garantimos que a tabela seja criada com o schema correto
    schema = schema_raw_faturas, 
    
    # Metadados da tabela no DLT
    name = TARGET_TABLE_DLT_NAME, # O nome que aparecerá no grafo do DLT
    comment = f"Tabela Raw de Faturas (SCD Type 1) ingerida da {SOURCE_TABLE}"
)