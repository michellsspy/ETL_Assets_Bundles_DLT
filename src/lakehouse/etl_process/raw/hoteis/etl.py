import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType
import sys

# Importa o schema local
try:
    from schema import schema_raw_hoteis
except ImportError:
    print("Erro: Não foi possível encontrar o schema.py. Verifique o sys.path.")
    sys.exit(1)

# --- Funções Genéricas de Ingestão ---
# (As funções 'create_table_if_not_exists' e 'run_scd1_merge' são coladas aqui)

def create_table_if_not_exists(spark: SparkSession, table_name: str, schema: StructType):
    print(f"Verificando/Criando tabela de destino: {table_name}...")
    ddl_schema = ", ".join([f"`{field.name}` {field.dataType.simpleString()}" for field in schema.fields])
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            {ddl_schema}
        )
        USING DELTA
        COMMENT 'Tabela da camada Raw (hoteis) criada via job PySpark.'
    """)
    print(f"Tabela {table_name} pronta.")

def run_scd1_merge(spark: SparkSession, source_table_name: str, target_table_name: str, target_schema: StructType, primary_keys: list[str]):
    print(f"Iniciando MERGE para a tabela: {target_table_name}...")
    try:
        df_source = spark.read.table(source_table_name)
        target_cols = [f.name for f in target_schema.fields if f.name not in ("insert_date", "update_date")]
        select_exprs = []
        for col_name in target_cols:
            if col_name in df_source.columns:
                field_type = target_schema[col_name].dataType
                select_exprs.append(F.col(col_name).cast(field_type).alias(col_name))
            else:
                field_type = target_schema[col_name].dataType
                select_exprs.append(F.lit(None).cast(field_type).alias(col_name))
        df_source.select(select_exprs).createOrReplaceTempView("source_for_merge")

        merge_condition = " AND ".join([f"target.{k} = source.{k}" for k in primary_keys])
        update_set_clause = ", ".join(
            [f"target.{col} = source.{col}" for col in target_cols if col not in primary_keys] +
            ["target.update_date = current_timestamp()"]
        )
        all_cols_str = ", ".join([f"`{f.name}`" for f in target_schema.fields])
        values_str = ", ".join(
            [f"source.{f.name}" if f.name in target_cols else 
             "current_timestamp()" if f.name == "insert_date" else 
             "NULL" if f.name == "update_date" else 
             "NULL"
             for f in target_schema.fields]
        )
        merge_sql = f"""
            MERGE INTO {target_table_name} AS target
            USING source_for_merge AS source
            ON {merge_condition}
            WHEN MATCHED THEN UPDATE SET {update_set_clause}
            WHEN NOT MATCHED THEN INSERT ({all_cols_str}) VALUES ({values_str})
        """
        print(f"Executando MERGE na tabela {target_table_name}...")
        spark.sql(merge_sql)
        print(f"MERGE concluído para {target_table_name}.")
    except Exception as e:
        print(f"ERRO ao processar a tabela {target_table_name}: {e}")
        if "TABLE_OR_VIEW_NOT_FOUND" in str(e):
            print(f"ERRO DE DADOS: A tabela de origem '{source_table_name}' não foi encontrada.")
        else:
            raise e

# --- Ponto de Entrada Específico desta Tabela ---

if __name__ == "__main__":
    print("Iniciando o Job de Ingestão da Camada Raw para 'Hoteis'...")
    
    # Configuração específica desta tabela
    SOURCE_TABLE_NAME = "dev.transient.source_hoteis"
    TARGET_TABLE_NAME = "dev.raw.hoteis"
    SCHEMA = schema_raw_hoteis
    PRIMARY_KEYS = ["hotel_id"]
    
    spark_session = SparkSession.builder.appName("RawIngestion_Hoteis").getOrCreate()
    
    create_table_if_not_exists(spark_session, TARGET_TABLE_NAME, SCHEMA)
    run_scd1_merge(
        spark=spark_session,
        source_table_name=SOURCE_TABLE_NAME,
        target_table_name=TARGET_TABLE_NAME,
        target_schema=SCHEMA,
        primary_keys=PRIMARY_KEYS
    )
    
    print("Job de Ingestão de 'Hoteis' concluído.")