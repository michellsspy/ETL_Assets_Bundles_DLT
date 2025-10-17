from pyspark.sql.types import (StructType, StructField, IntegerType, StringType,
                               DateType, DoubleType, TimestampType)

# Definição do schema para a tabela final dev.raw.consumos
# Inclui comentários detalhados (metadata) para cada coluna.

schema_raw_consumos = StructType([
    # Colunas da Tabela de Origem (source_consumos)
    StructField("consumo_id", IntegerType(), False, 
                metadata={"comment": "Chave primária única para cada item de consumo."}),
    StructField("reserva_id", IntegerType(), True, 
                metadata={"comment": "Chave estrangeira que liga ao registro da reserva."}),
    StructField("hospede_id", IntegerType(), True, 
                metadata={"comment": "Chave estrangeira que liga ao hóspede que consumiu."}), 
    StructField("hotel_id", IntegerType(), True, 
                metadata={"comment": "Chave estrangeira que liga ao hotel onde ocorreu o consumo."}),
    StructField("nome_servico", StringType(), True, 
                metadata={"comment": "Descrição do serviço ou produto consumido (ex: 'Restaurante - Jantar')."}),
    StructField("data_consumo", DateType(), True, 
                metadata={"comment": "Data em que o consumo foi registrado."}), 
    StructField("quantidade", IntegerType(), True, 
                metadata={"comment": "Número de unidades do serviço/produto consumido."}),
    StructField("valor_total_consumo", DoubleType(), True, 
                metadata={"comment": "Valor total do consumo (quantidade * preço unitário)."}),
    StructField("hora_consumo", StringType(), True, 
                metadata={"comment": "Hora aproximada do consumo (formato HH:MM)."}), 
    StructField("local_consumo", StringType(), True, 
                metadata={"comment": "Local onde o consumo foi registrado (ex: 'Restaurante', 'Quarto')."}), 
    StructField("funcionario_responsavel", StringType(), True, 
                metadata={"comment": "Nome do funcionário que registrou o consumo (pode ser nulo)."}),
    
    # Colunas de Auditoria (Controle)
    StructField("insert_date", TimestampType(), True, 
                metadata={"comment": "Data e hora em que o registro foi inserido pela primeira vez na camada Raw."}),
    StructField("update_date", TimestampType(), True, 
                metadata={"comment": "Data e hora da última atualização do registro na camada Raw."})
])