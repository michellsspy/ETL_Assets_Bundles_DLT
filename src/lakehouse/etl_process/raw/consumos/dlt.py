import dlt
from pyspark.sql import functions as F
from .schema import schema_raw_consumos

SOURCE_TABLE = "dev.transient.source_consumos"
TARGET_TABLE = "dev.raw.consumos"
TARGET_TABLE_DLT_NAME = "raw_consumos"

@dlt.table(
    name="consumos_transient_stream",
    comment=f"Lê a tabela {SOURCE_TABLE} e prepara colunas de controle para o MERGE.",
    temporary=True
)
def consumos_transient_stream():
    """
    Prepara o stream de dados da origem.

    Esta função lê a tabela de origem e adiciona as colunas de auditoria
    com o timestamp atual.

    - 'insert_date': Recebe o timestamp atual. Será usado APENAS no INSERT.
    - 'update_date': Recebe o timestamp atual. Será usado tanto no INSERT 
                     quanto no UPDATE.

    No DLT, é o comportamento padrão que 'insert_date' e 'update_date'
    sejam iguais no momento da criação do registro.
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
    source = "consumos_transient_stream",
    keys = ["consumo_id"], # Chave Primária
    
    # Diz ao DLT para NUNCA atualizar o 'insert_date' se o registro já existir.
    except_column_list = ["insert_date"],
    
    schema = schema_raw_consumos,
    name = TARGET_TABLE_DLT_NAME,
    comment = f"Tabela Raw de Consumos (SCD Type 1) ingerida da {SOURCE_TABLE}"
)
