from pyspark.sql.types import (StructType, StructField, IntegerType, StringType,
                               DateType, DoubleType, TimestampType)

# Definição do schema para a tabela final dev.raw.reserva_ota
# Inclui comentários detalhados (metadata) para cada coluna.

schema_raw_reserva_ota = StructType([
    # Colunas da Tabela de Origem (source_reserva_ota)
    StructField("ota_reserva_id", IntegerType(), False, 
                metadata={"comment": "Chave primária única para o registro da OTA."}),
    StructField("reserva_id", IntegerType(), True, 
                metadata={"comment": "Chave estrangeira que liga à reserva principal no sistema do hotel."}),
    StructField("ota_codigo_confirmacao", StringType(), True, 
                metadata={"comment": "Código de confirmação gerado pela OTA (ex: Booking, Expedia)."}),
    StructField("ota_nome_convidado", StringType(), True, 
                metadata={"comment": "Nome do convidado como enviado pela OTA (pode diferir do cadastro principal)."}),
    StructField("total_pago_ota", DoubleType(), True, 
                metadata={"comment": "Valor total que o hóspede pagou diretamente à OTA."}),
    StructField("taxa_comissao", DoubleType(), True, 
                metadata={"comment": "Valor ou percentual da comissão retida pela OTA."}),
    StructField("valor_liquido_recebido", DoubleType(), True, 
                metadata={"comment": "Valor líquido que a OTA repassou ao hotel."}),
    StructField("ota_solicitacoes_especificas", StringType(), True, 
                metadata={"comment": "Solicitações especiais feitas através do canal da OTA."}),

    # Colunas de Auditoria (Controle)
    StructField("insert_date", TimestampType(), True, 
                metadata={"comment": "Data e hora em que o registro foi inserido pela primeira vez na camada Raw."}),
    StructField("update_date", TimestampType(), True, 
                metadata={"comment": "Data e hora da última atualização do registro na camada Raw."})
])