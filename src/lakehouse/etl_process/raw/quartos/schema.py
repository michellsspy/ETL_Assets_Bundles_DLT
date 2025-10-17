from pyspark.sql.types import (StructType, StructField, IntegerType, StringType,
                               DateType, DoubleType, BooleanType, TimestampType)

# Definição do schema para a tabela final dev.raw.quartos
# Baseado no schema "memorizado" da source_quartos + colunas de auditoria

schema_raw_quartos = StructType([
    # Colunas da Tabela de Origem (source_quartos)
    StructField("quarto_id", IntegerType(), False), # Chave Primária
    StructField("hotel_id", IntegerType(), False), # (False, pois é chave estrangeira da origem)
    StructField("numero_quarto", StringType(), True),
    StructField("tipo_quarto", StringType(), True),
    StructField("capacidade_maxima", IntegerType(), True),
    StructField("preco_diaria_base", DoubleType(), True),
    StructField("andar", IntegerType(), True),
    StructField("vista", StringType(), True),
    StructField("comodidades_quarto", StringType(), True),
    StructField("possui_ar_condicionado", BooleanType(), True),
    StructField("tamanho_quarto", StringType(), True),
    StructField("status_manutencao", StringType(), True),
    StructField("ultima_manutencao", DateType(), True),
    StructField("eh_smoke_free", BooleanType(), True),
    StructField("possui_kit_boas_vindas", BooleanType(), True),
    StructField("numero_camas", IntegerType(), True),
    
    # Novas colunas de controle (Auditoria)
    StructField("insert_date", TimestampType(), True),
    StructField("update_date", TimestampType(), True)
])