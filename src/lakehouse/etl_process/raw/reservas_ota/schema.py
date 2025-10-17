from pyspark.sql.types import (StructType, StructField, IntegerType, StringType,
                               DateType, DoubleType, BooleanType, TimestampType)

# Definição do schema para a tabela final dev.raw.reserva_ota
# Baseado no schema que você forneceu + colunas de auditoria

schema_raw_reserva_ota = StructType([
    # Colunas da Tabela de Origem (source_reserva_ota)
    StructField("ota_reserva_id", IntegerType(), False), # Chave Primária
    StructField("reserva_id", IntegerType(), True),
    StructField("ota_codigo_confirmacao", StringType(), True),
    StructField("ota_nome_convidado", StringType(), True),
    StructField("total_pago_ota", DoubleType(), True),
    StructField("taxa_comissao", DoubleType(), True),
    StructField("valor_liquido_recebido", DoubleType(), True),
    StructField("ota_solicitacoes_especificas", StringType(), True),
    
    # Novas colunas de controle (Auditoria)
    StructField("insert_date", TimestampType(), True),
    StructField("update_date", TimestampType(), True)
])