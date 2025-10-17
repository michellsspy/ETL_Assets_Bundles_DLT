from pyspark.sql.types import (StructType, StructField, IntegerType, StringType,
                               DateType, DoubleType, BooleanType, TimestampType)

# Definição do schema para a tabela final dev.raw.hospedes
# Baseado no schema "memorizado" da source_hospedes + colunas de auditoria

schema_raw_hospedes = StructType([
    # Colunas da Tabela de Origem (source_hospedes)
    StructField("hospede_id", IntegerType(), False), # Chave Primária
    StructField("nome_completo", StringType(), True),
    StructField("cpf", StringType(), True),
    StructField("data_nascimento", DateType(), True),
    StructField("email", StringType(), True),
    StructField("telefone", StringType(), True),
    StructField("estado", StringType(), True),
    StructField("nacionalidade", StringType(), True),
    StructField("data_cadastro", DateType(), True),
    StructField("programa_fidelidade", StringType(), True),
    StructField("profissao", StringType(), True),
    StructField("tipo_documento", StringType(), True),
    StructField("numero_documento", StringType(), True),
    StructField("empresa", StringType(), True),
    StructField("eh_viajante_frequente", BooleanType(), True),
    StructField("preferencias_hospede", StringType(), True),
    StructField("restricoes_alimentares", StringType(), True),
    StructField("data_ultima_hospedagem", DateType(), True),
    StructField("total_hospedagens", IntegerType(), True),
    
    # Novas colunas de controle (Auditoria)
    StructField("insert_date", TimestampType(), True),
    StructField("update_date", TimestampType(), True)
])