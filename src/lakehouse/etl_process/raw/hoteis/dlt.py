import dlt
from pyspark.sql import functions as F

# Importa o schema (usando o import local que funcionou)
from schema import schema_raw_hoteis

SOURCE_TABLE = "dev.transient.source_hoteis"
TARGET_TABLE = "dev.raw.hoteis"
TARGET_TABLE_DLT_NAME = "raw_hoteis" # Nome lógico no grafo DLT

@dlt.table(
    name="hoteis_transient_stream",
    comment=f"Lê a tabela {SOURCE_TABLE}, prepara colunas de controle e aplica o schema explícito.",
    temporary=True,
    schema=schema_raw_hoteis,  # Aplica o schema aqui para garantir os tipos
)

def hoteis_transient_stream():
    """
    Prepara o stream de dados da origem.
    O schema explícito definido no decorador @dlt.table garantirá
    que o DLT faça o cast correto dos tipos de dados.
    """
    processing_time = F.current_timestamp()
    
    return (
        spark.readStream.table(SOURCE_TABLE)
        .withColumn("insert_date", processing_time)
        .withColumn("update_date", processing_time)
    )

# Aplica as mudanças na tabela Raw (SCD Type 1)
dlt.apply_changes(
    target = TARGET_TABLE,
    source = "hoteis_transient_stream", # Fonte (com schema já aplicado)
    keys = ["hotel_id"], # Chave Primária
    
    # Mantém o insert_date original no UPDATE
    except_column_list = ["insert_date"],
    
    # Metadados da Tabela Final
    name = TARGET_TABLE_DLT_NAME,
    comment = f"Tabela Raw de Hoteis (SCD Type 1) ingerida da {SOURCE_TABLE}",
    tags = {"layer": "raw"} # Tag para a tabela final
)