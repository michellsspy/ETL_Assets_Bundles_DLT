from pyspark.sql.types import (StructType, StructField, IntegerType, StringType,
                               DateType, DoubleType, BooleanType, TimestampType)

# Definição do schema para a tabela final dev.raw.hospedes
# Inclui comentários detalhados (metadata) para cada coluna.

schema_raw_hospedes = StructType([
    # Colunas da Tabela de Origem (source_hospedes)
    StructField("hospede_id", IntegerType(), False, 
                metadata={"comment": "Chave primária única para cada hóspede."}),
    StructField("nome_completo", StringType(), True, 
                metadata={"comment": "Nome completo do hóspede."}),
    StructField("cpf", StringType(), True, 
                metadata={"comment": "CPF do hóspede (documento principal no Brasil)."}),
    StructField("data_nascimento", DateType(), True, 
                metadata={"comment": "Data de nascimento do hóspede."}),
    StructField("email", StringType(), True, 
                metadata={"comment": "Endereço de e-mail de contato do hóspede."}),
    StructField("telefone", StringType(), True, 
                metadata={"comment": "Número de telefone de contato do hóspede."}),
    StructField("estado", StringType(), True, 
                metadata={"comment": "Estado de residência do hóspede (UF)."}),
    StructField("nacionalidade", StringType(), True, 
                metadata={"comment": "Nacionalidade do hóspede."}),
    StructField("data_cadastro", DateType(), True, 
                metadata={"comment": "Data em que o hóspede foi cadastrado no sistema."}),
    StructField("programa_fidelidade", StringType(), True, 
                metadata={"comment": "Nível do programa de fidelidade (ex: 'Bronze', 'Gold')."}),
    StructField("profissao", StringType(), True, 
                metadata={"comment": "Profissão declarada pelo hóspede."}),
    StructField("tipo_documento", StringType(), True, 
                metadata={"comment": "Tipo de documento alternativo (ex: 'Passaporte', 'RG')."}),
    StructField("numero_documento", StringType(), True, 
                metadata={"comment": "Número do documento alternativo."}),
    StructField("empresa", StringType(), True, 
                metadata={"comment": "Empresa à qual o hóspede está vinculado (para viagens corporativas)."}),
    StructField("eh_viajante_frequente", BooleanType(), True, 
                metadata={"comment": "Indicador se o hóspede é marcado como viajante frequente."}),
    StructField("preferencias_hospede", StringType(), True, 
                metadata={"comment": "Lista de preferências do hóspede (ex: 'Andar alto', 'Quieto')."}),
    StructField("restricoes_alimentares", StringType(), True, 
                metadata={"comment": "Restrições alimentares declaradas (ex: 'Vegano', 'Sem glúten')."}),
    StructField("data_ultima_hospedagem", DateType(), True, 
                metadata={"comment": "Data do último checkout do hóspede na rede."}),
    StructField("total_hospedagens", IntegerType(), True, 
                metadata={"comment": "Contagem total de hospedagens (estadias) do hóspede na rede."}),

    # Colunas de Auditoria (Controle)
    StructField("insert_date", TimestampType(), True, 
                metadata={"comment": "Data e hora em que o registro foi inserido pela primeira vez na camada Raw."}),
    StructField("update_date", TimestampType(), True, 
                metadata={"comment": "Data e hora da última atualização do registro na camada Raw."})
])