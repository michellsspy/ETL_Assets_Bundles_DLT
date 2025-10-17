from pyspark.sql.types import (StructType, StructField, IntegerType, StringType,
                               DateType, DoubleType, TimestampType)

# Definição do schema para a tabela final dev.raw.consumos
# Este schema é a união do schema da 'source_consumos'
# com as novas colunas de controle (insert_date, update_date).

schema_raw_consumos = StructType([
    # Colunas da Tabela de Origem (source_consumos)
    StructField("consumo_id", IntegerType(), False), # Chave primária
    StructField("reserva_id", IntegerType(), True),
    StructField("hospede_id", IntegerType(), True), 
    StructField("hotel_id", IntegerType(), True),
    StructField("nome_servico", StringType(), True),
    StructField("data_consumo", DateType(), True), 
    StructField("quantidade", IntegerType(), True),
    StructField("valor_total_consumo", DoubleType(), True),
    StructField("hora_consumo", StringType(), True), 
    StructField("local_consumo", StringType(), True), 
    StructField("funcionario_responsavel", StringType(), True),
    
    # Novas colunas de controle (Auditoria)
    StructField("insert_date", TimestampType(), True),
    StructField("update_date", TimestampType(), True)
])