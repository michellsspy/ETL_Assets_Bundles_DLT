import dlt
from pyspark.sql import functions as F

# Importa o schema (usando o import local que funcionou)
from schema import schema_raw_quartos

SOURCE_TABLE = "dev.transient.source_quartos"
TARGET_TABLE = "dev.raw.quartos"
TARGET_TABLE_DLT_NAME = "raw_quartos" # Nome lógico no grafo DLT


# 1. FUNÇÃO FONTE (TEMPORÁRIA)
# Lê a origem, adiciona auditoria e aplica o schema
@dlt.table(
    name="quartos_transient_stream",
    comment=f"Lê a tabela {SOURCE_TABLE}, prepara colunas de controle e aplica o schema explícito.",
    temporary=True,
    schema=schema_raw_quartos  # Aplica o schema aqui para garantir os tipos
)
def quartos_transient_stream():
    """
    Prepara o stream de dados da origem (source_quartos).
    """
    processing_time = F.current_timestamp()
    
    return (
        spark.readStream.table(SOURCE_TABLE)
        .withColumn("insert_date", processing_time)
        .withColumn("update_date", processing_time)
    )


# 2. FUNÇÃO ALVO (FINAL)
# Aplica o SCD Type 1 (MERGE) usando a fonte acima
@dlt.table(
    name = TARGET_TABLE_DLT_NAME,
    comment = f"Tabela Raw de Quartos (SCD Type 1) ingerida da {SOURCE_TABLE}",
    table_properties = {"layer": "raw"} # <<< CORREÇÃO AQUI (de 'tags' para 'table_properties')
)
def raw_quartos_target():
    """
    Função que aplica o SCD Type 1 (apply_changes) na tabela alvo.
    """
    return dlt.apply_changes(
        target = TARGET_TABLE,
        source = "quartos_transient_stream", # O nome da função fonte
        keys = ["quarto_id"], # Chave Primária
        
        # Mantém o insert_date original no UPDATE
        except_column_list = ["insert_date"]
    )