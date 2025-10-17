from pyspark.sql.types import (StructType, StructField, IntegerType, StringType,
                               DateType, DoubleType, BooleanType, TimestampType)

# Definição do schema para a tabela final dev.raw.reservas
# Baseado no schema "memorizado" da source_reservas + colunas de auditoria

schema_raw_reservas = StructType([
    # Colunas da Tabela de Origem (source_reservas)
    StructField("reserva_id", IntegerType(), False), # Chave Primária
    StructField("hospede_id", IntegerType(), True),
    StructField("quarto_id", IntegerType(), True),
    StructField("hotel_id", IntegerType(), True),
    StructField("data_reserva", DateType(), True),
    StructField("data_checkin", DateType(), True),
    StructField("data_checkout", DateType(), True),
    StructField("numero_noites", IntegerType(), True),
    StructField("numero_adultos", IntegerType(), True),
    StructField("numero_criancas", IntegerType(), True),
    StructField("canal_reserva", StringType(), True),
    StructField("status_reserva", StringType(), True),
    StructField("data_cancelamento", DateType(), True),
    StructField("solicitacoes_especiais", StringType(), True),
    StructField("valor_total_estadia", DoubleType(), True),
    StructField("motivo_viagem", StringType(), True),
    StructField("motivo_cancelamento", StringType(), True),
    StructField("taxa_limpeza", DoubleType(), True),
    StructField("taxa_turismo", DoubleType(), True),
    StructField("avaliacao_hospede", DoubleType(), True),
    StructField("comentarios_hospede", StringType(), True),
    
    # Novas colunas de controle (Auditoria)
    StructField("insert_date", TimestampType(), True),
    StructField("update_date", TimestampType(), True)
])