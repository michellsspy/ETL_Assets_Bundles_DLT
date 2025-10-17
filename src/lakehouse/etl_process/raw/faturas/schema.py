from pyspark.sql.types import (StructType, StructField, IntegerType, StringType,
                               DateType, DoubleType, BooleanType, TimestampType)

# Definição do schema para a tabela final dev.raw.faturas
# Baseado no schema "memorizado" da source_faturas + colunas de auditoria

schema_raw_faturas = StructType([
    # Colunas da Tabela de Origem (source_faturas)
    StructField("fatura_id", IntegerType(), False), # Chave Primária
    StructField("reserva_id", IntegerType(), True),
    StructField("hospede_id", IntegerType(), True), 
    StructField("data_emissao", DateType(), True),
    StructField("data_vencimento", DateType(), True),
    StructField("status_pagamento", StringType(), True),
    StructField("forma_pagamento", StringType(), True),
    StructField("subtotal_estadia", DoubleType(), True),
    StructField("subtotal_consumos", DoubleType(), True),
    StructField("descontos", DoubleType(), True),
    StructField("impostos", DoubleType(), True),
    StructField("valor_total", DoubleType(), True),
    StructField("data_pagamento", DateType(), True),
    StructField("taxa_limpeza", DoubleType(), True),
    StructField("taxa_turismo", DoubleType(), True),
    StructField("taxa_servico", DoubleType(), True),
    StructField("numero_transacao", StringType(), True),
    
    # Novas colunas de controle (Auditoria)
    StructField("insert_date", TimestampType(), True),
    StructField("update_date", TimestampType(), True)
])