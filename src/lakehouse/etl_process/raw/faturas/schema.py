from pyspark.sql.types import (StructType, StructField, IntegerType, StringType,
                               DateType, DoubleType, TimestampType)

# Definição do schema para a tabela final dev.raw.faturas
# Inclui comentários detalhados (metadata) para cada coluna.

schema_raw_faturas = StructType([
    # Colunas da Tabela de Origem (source_faturas)
    StructField("fatura_id", IntegerType(), False, 
                metadata={"comment": "Chave primária única para cada fatura."}),
    StructField("reserva_id", IntegerType(), True, 
                metadata={"comment": "Chave estrangeira que liga à reserva associada."}),
    StructField("hospede_id", IntegerType(), True, 
                metadata={"comment": "Chave estrangeira que liga ao hóspede principal da fatura."}),
    StructField("data_emissao", DateType(), True, 
                metadata={"comment": "Data em que a fatura foi gerada (geralmente no checkout)."}),
    StructField("data_vencimento", DateType(), True, 
                metadata={"comment": "Data limite para o pagamento da fatura."}),
    StructField("status_pagamento", StringType(), True, 
                metadata={"comment": "Status atual do pagamento (ex: 'Pago', 'Pendente', 'Atrasado')."}),
    StructField("forma_pagamento", StringType(), True, 
                metadata={"comment": "Método utilizado para o pagamento (ex: 'Cartão de Crédito', 'Pix')."}),
    StructField("subtotal_estadia", DoubleType(), True, 
                metadata={"comment": "Valor total referente apenas às diárias."}),
    StructField("subtotal_consumos", DoubleType(), True, 
                metadata={"comment": "Valor total de todos os consumos (restaurante, spa, etc.)."}),
    StructField("descontos", DoubleType(), True, 
                metadata={"comment": "Valor total de descontos aplicados (ex: fidelidade)."}),
    StructField("impostos", DoubleType(), True, 
                metadata={"comment": "Valor total de impostos (ex: ISS)."}),
    StructField("valor_total", DoubleType(), True, 
                metadata={"comment": "Valor final da fatura (Estadia + Consumos - Descontos + Impostos + Taxas)."}),
    StructField("data_pagamento", DateType(), True, 
                metadata={"comment": "Data em que o pagamento foi efetivado (nulo se pendente)."}),
    StructField("taxa_limpeza", DoubleType(), True, 
                metadata={"comment": "Taxa de limpeza cobrada na reserva."}),
    StructField("taxa_turismo", DoubleType(), True, 
                metadata={"comment": "Taxa de turismo municipal."}),
    StructField("taxa_servico", DoubleType(), True, 
                metadata={"comment": "Taxa de serviço opcional ou obrigatória."}),
    StructField("numero_transacao", StringType(), True, 
                metadata={"comment": "Código de autorização ou ID da transação de pagamento."}),

    # Colunas de Auditoria (Controle)
    StructField("insert_date", TimestampType(), True, 
                metadata={"comment": "Data e hora em que o registro foi inserido pela primeira vez na camada Raw."}),
    StructField("update_date", TimestampType(), True, 
                metadata={"comment": "Data e hora da última atualização do registro na camada Raw."})
])