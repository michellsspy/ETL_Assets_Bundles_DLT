from pyspark.sql.types import (StructType, StructField, IntegerType, StringType,
                               DateType, DoubleType, BooleanType, TimestampType)

# Definição do schema para a tabela final dev.raw.hoteis
# Baseado no schema "memorizado" da source_hoteis + colunas de auditoria

schema_raw_hoteis = StructType([
    # Colunas da Tabela de Origem (source_hoteis)
    StructField("hotel_id", IntegerType(), False), # Chave Primária
    StructField("nome_hotel", StringType(), True),
    StructField("endereco", StringType(), True),
    StructField("cidade", StringType(), True),
    StructField("estado", StringType(), True),
    StructField("estrelas", IntegerType(), True),
    StructField("numero_quartos", IntegerType(), True),
    StructField("comodidades", StringType(), True),
    StructField("telefone", StringType(), True),
    StructField("email_contato", StringType(), True),
    StructField("data_abertura", DateType(), True),
    StructField("horario_checkin", StringType(), True),
    StructField("horario_checkout", StringType(), True),
    StructField("categoria_hotel", StringType(), True),
    StructField("tipo_hotel", StringType(), True),
    StructField("ano_fundacao", IntegerType(), True),
    StructField("capacidade_total", IntegerType(), True),
    StructField("possui_acessibilidade", BooleanType(), True),
    StructField("certificacoes", StringType(), True),
    StructField("latitude", DoubleType(), True),
    StructField("longitude", DoubleType(), True),
    StructField("descricao_hotel", StringType(), True),
    StructField("numero_funcionarios", IntegerType(), True),
    
    # Novas colunas de controle (Auditoria)
    StructField("insert_date", TimestampType(), True),
    StructField("update_date", TimestampType(), True)
])