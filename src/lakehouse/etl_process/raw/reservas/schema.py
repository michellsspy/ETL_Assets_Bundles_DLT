from pyspark.sql.types import (StructType, StructField, IntegerType, StringType,
                               DateType, DoubleType, TimestampType)

# Definição do schema para a tabela final dev.raw.reservas
# Inclui comentários detalhados (metadata) para cada coluna.

schema_raw_reservas = StructType([
    # Colunas da Tabela de Origem (source_reservas)
    StructField("reserva_id", IntegerType(), False, 
                metadata={"comment": "Chave primária única para cada reserva."}),
    StructField("hospede_id", IntegerType(), True, 
                metadata={"comment": "Chave estrangeira que liga ao hóspede que fez a reserva."}),
    StructField("quarto_id", IntegerType(), True, 
                metadata={"comment": "Chave estrangeira que liga ao quarto reservado."}),
    StructField("hotel_id", IntegerType(), True, 
                metadata={"comment": "Chave estrangeira que liga ao hotel onde a reserva foi feita."}),
    StructField("data_reserva", DateType(), True, 
                metadata={"comment": "Data em que a reserva foi criada no sistema."}),
    StructField("data_checkin", DateType(), True, 
                metadata={"comment": "Data planejada para a chegada (check-in) do hóspede."}),
    StructField("data_checkout", DateType(), True, 
                metadata={"comment": "Data planejada para a saída (check-out) do hóspede."}),
    StructField("numero_noites", IntegerType(), True, 
                metadata={"comment": "Número total de noites da estadia."}),
    StructField("numero_adultos", IntegerType(), True, 
                metadata={"comment": "Número de adultos na reserva."}),
    StructField("numero_criancas", IntegerType(), True, 
                metadata={"comment": "Número de crianças na reserva."}),
    StructField("canal_reserva", StringType(), True, 
                metadata={"comment": "Canal por onde a reserva foi feita (ex: 'Website Hotel', 'Booking.com')."}),
    StructField("status_reserva", StringType(), True, 
                metadata={"comment": "Status atual da reserva (ex: 'Confirmada', 'Concluída', 'Cancelada', 'Hospedado')."}),
    StructField("data_cancelamento", DateType(), True, 
                metadata={"comment": "Data em que a reserva foi cancelada (nulo se não aplicável)."}),
    StructField("solicitacoes_especiais", StringType(), True, 
                metadata={"comment": "Lista de solicitações especiais do hóspede (ex: 'Berço', 'Andar alto')."}),
    StructField("valor_total_estadia", DoubleType(), True, 
                metadata={"comment": "Valor base total das diárias (numero_noites * preco_diaria_base)."}),
    StructField("motivo_viagem", StringType(), True, 
                metadata={"comment": "Motivo da viagem declarado (ex: 'Lazer', 'Negócios')."}),
    StructField("motivo_cancelamento", StringType(), True, 
                metadata={"comment": "Motivo do cancelamento (nulo se não aplicável)."}),
    StructField("taxa_limpeza", DoubleType(), True, 
                metadata={"comment": "Taxa de limpeza associada à reserva."}),
    StructField("taxa_turismo", DoubleType(), True, 
                metadata={"comment": "Taxa de turismo associada à reserva."}),
    StructField("avaliacao_hospede", DoubleType(), True, 
                metadata={"comment": "Nota (ex: 0-5) da avaliação deixada pelo hóspede (nulo se não avaliado)."}),
    StructField("comentarios_hospede", StringType(), True, 
                metadata={"comment": "Texto do comentário deixado pelo hóspede (nulo se não avaliado)."}),

    # Colunas de Auditoria (Controle)
    StructField("insert_date", TimestampType(), True, 
                metadata={"comment": "Data e hora em que o registro foi inserido pela primeira vez na camada Raw."}),
    StructField("update_date", TimestampType(), True, 
                metadata={"comment": "Data e hora da última atualização do registro na camada Raw."})
])