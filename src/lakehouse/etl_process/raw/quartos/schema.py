from pyspark.sql.types import (StructType, StructField, IntegerType, StringType,
                               DateType, DoubleType, BooleanType, TimestampType)

# Definição do schema para a tabela final dev.raw.quartos
# Inclui comentários detalhados (metadata) para cada coluna.

schema_raw_quartos = StructType([
    # Colunas da Tabela de Origem (source_quartos)
    StructField("quarto_id", IntegerType(), False, 
                metadata={"comment": "Chave primária única para cada quarto do hotel."}),
    StructField("hotel_id", IntegerType(), False, 
                metadata={"comment": "Chave estrangeira que liga ao hotel ao qual o quarto pertence."}),
    StructField("numero_quarto", StringType(), True, 
                metadata={"comment": "Número/identificador do quarto (ex: '101', '2305')."}),
    StructField("tipo_quarto", StringType(), True, 
                metadata={"comment": "Categoria do quarto (ex: 'Standard', 'Suíte', 'Superior')."}),
    StructField("capacidade_maxima", IntegerType(), True, 
                metadata={"comment": "Número máximo de hóspedes permitido no quarto."}),
    StructField("preco_diaria_base", DoubleType(), True, 
                metadata={"comment": "Valor base da diária do quarto, sem taxas ou descontos."}),
    StructField("andar", IntegerType(), True, 
                metadata={"comment": "Andar em que o quarto está localizado."}),
    StructField("vista", StringType(), True, 
                metadata={"comment": "Descrição da vista do quarto (ex: 'Mar', 'Cidade', 'Piscina')."}),
    StructField("comodidades_quarto", StringType(), True, 
                metadata={"comment": "Lista de comodidades específicas do quarto (ex: 'Frigobar, Varanda')."}),
    StructField("possui_ar_condicionado", BooleanType(), True, 
                metadata={"comment": "Indicador se o quarto possui ar condicionado."}),
    StructField("tamanho_quarto", StringType(), True, 
                metadata={"comment": "Descrição do tamanho do quarto (ex: 'Médio (30m²)', 'Pequeno (20m²)')."}),
    StructField("status_manutencao", StringType(), True, 
                metadata={"comment": "Status operacional do quarto (ex: 'Disponível', 'Em Manutenção')."}),
    StructField("ultima_manutencao", DateType(), True, 
                metadata={"comment": "Data da última manutenção registrada para o quarto."}),
    StructField("eh_smoke_free", BooleanType(), True, 
                metadata={"comment": "Indicador se o quarto é exclusivo para não fumantes."}),
    StructField("possui_kit_boas_vindas", BooleanType(), True, 
                metadata={"comment": "Indicador se o quarto oferece um kit de boas-vindas."}),
    StructField("numero_camas", IntegerType(), True, 
                metadata={"comment": "Quantidade de camas no quarto."}),

    # Colunas de Auditoria (Controle)
    StructField("insert_date", TimestampType(), True, 
                metadata={"comment": "Data e hora em que o registro foi inserido pela primeira vez na camada Raw."}),
    StructField("update_date", TimestampType(), True, 
                metadata={"comment": "Data e hora da última atualização do registro na camada Raw."})
])