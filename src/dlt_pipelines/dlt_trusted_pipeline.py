# Databricks notebook source
# src/dlt_pipelines/dlt_trusted_pipeline.py

import dlt
from pyspark.sql.functions import col, current_timestamp
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType # Import needed for type hinting

# Obtenha a sessão Spark
spark = SparkSession.builder.getOrCreate()

# --- Camada TRUSTED (Prata) ---
# Implementada com MERGE explícito para controle das colunas de auditoria

# --- Funções Helper ---
# Estas funções ajudam a construir o SQL do MERGE dinamicamente

def get_business_cols(schema: StructType) -> list[str]:
    """Retorna nomes de colunas do schema, excluindo as de auditoria."""
    return [f.name for f in schema.fields if f.name not in ("insert_date", "update_date")]

def build_update_set_clause(schema: StructType, primary_keys: list[str], source_alias="source", target_alias="target") -> str:
    """Constrói a cláusula SET para o UPDATE do MERGE."""
    cols = get_business_cols(schema)
    # Atualiza todas as colunas de negócio que NÃO são chave primária
    set_clauses = [f"{target_alias}.`{c}` = {source_alias}.`{c}`" for c in cols if c not in primary_keys]
    # Adiciona a atualização da coluna 'update_date'
    set_clauses.append(f"{target_alias}.update_date = current_timestamp()")
    return ", ".join(set_clauses)

def build_insert_clause(schema: StructType, source_alias="source") -> str:
    """Constrói as cláusulas COLUMNS e VALUES para o INSERT do MERGE."""
    cols = [f"`{f.name}`" for f in schema.fields] # Nomes de todas as colunas no destino
    values = []
    business_cols = get_business_cols(schema) # Nomes das colunas de negócio (da origem)
    for f in schema.fields:
        if f.name == "insert_date":
            values.append("current_timestamp()") # Preenche insert_date com agora
        elif f.name == "update_date":
            values.append("NULL") # Preenche update_date com NULL
        elif f.name in business_cols:
            values.append(f"{source_alias}.`{f.name}`") # Pega o valor da origem
        else:
             # Caso raro: coluna existe no schema mas não na origem (exceto auditoria)
             values.append("NULL")
    return f"({', '.join(cols)}) VALUES ({', '.join(values)})"

def execute_merge_for_table(target_dlt_table_name: str, source_view_name: str, primary_keys: list[str]):
    """Função genérica para executar o MERGE SQL dentro de uma função @dlt.table."""
    target_table_name_qualified = f"LIVE.{target_dlt_table_name}" # Nome completo usado no SQL
    
    # Obtém o schema da view preparada (que já tem os casts corretos)
    schema = spark.table(source_view_name).schema
    
    # Garante que o schema tenha as colunas de auditoria
    schema_fields = schema.fields
    if not any(f.name == "insert_date" for f in schema_fields):
        schema = schema.add("insert_date", "timestamp")
    if not any(f.name == "update_date" for f in schema_fields):
        schema = schema.add("update_date", "timestamp")

    # Cria a view temporária necessária para o SQL MERGE
    # dlt.read garante a dependência correta no grafo DLT
    dlt.read(source_view_name).createOrReplaceTempView(source_view_name)
    
    # Constrói as partes do SQL MERGE
    merge_condition = " AND ".join([f"target.`{k}` = source.`{k}`" for k in primary_keys])
    update_clause = build_update_set_clause(schema, primary_keys, "source", "target")
    insert_clause = build_insert_clause(schema, "source")

    # Monta e executa o SQL
    merge_sql = f"""
        MERGE INTO {target_table_name_qualified} AS target
        USING {source_view_name} AS source
        ON {merge_condition}
        WHEN MATCHED THEN UPDATE SET {update_clause}
        WHEN NOT MATCHED THEN INSERT {insert_clause}
    """
    spark.sql(merge_sql)
    
    # Retorna um select da tabela alvo para o DLT
    # Isso é necessário para que o DLT entenda que a tabela foi criada/atualizada
    return spark.table(target_table_name_qualified)


# --- Processamento das Tabelas (BATCH com MERGE Explícito) ---

# --- HOTEIS ---
@dlt.view(
  name="hoteis_raw_prepared",
  comment="Prepara os dados brutos de hoteis para o MERGE."
)
def hoteis_raw_prepared():
  df_raw = spark.read.table("dev.raw.hoteis_raw") # Leitura BATCH
  return df_raw.select( # Seleciona e faz cast das colunas de negócio
    col("hotel_id").cast("INT"), col("nome_hotel").cast("STRING"), col("endereco").cast("STRING"),
    col("cidade").cast("STRING"), col("estado").cast("STRING"), col("estrelas").cast("INT"),
    col("numero_quartos").cast("INT"), col("comodidades").cast("STRING"), col("telefone").cast("STRING"),
    col("email_contato").cast("STRING"), col("data_abertura").cast("DATE"), col("horario_checkin").cast("STRING"),
    col("horario_checkout").cast("STRING"), col("categoria_hotel").cast("STRING"), col("tipo_hotel").cast("STRING"),
    col("ano_fundacao").cast("INT"), col("capacidade_total").cast("INT"), col("possui_acessibilidade").cast("BOOLEAN"),
    col("certificacoes").cast("STRING"), col("latitude").cast("DOUBLE"), col("longitude").cast("DOUBLE"),
    col("descricao_hotel").cast("STRING"), col("numero_funcionarios").cast("INT")
  )

@dlt.table(
    name="hoteis_trusted",
    comment="Tabela Trusted de Hoteis (carregada via MERGE)."
)
def hoteis_trusted():
    return execute_merge_for_table(
        target_dlt_table_name="hoteis_trusted",
        source_view_name="hoteis_raw_prepared",
        primary_keys=["hotel_id"]
    )

# --- QUARTOS ---
@dlt.view(name="quartos_raw_prepared", comment="Prepara os dados brutos de quartos.")
def quartos_raw_prepared():
  df_raw = spark.read.table("dev.raw.quartos_raw") # Leitura BATCH
  return df_raw.select(
      col("quarto_id").cast("INT"), col("hotel_id").cast("INT"), col("numero_quarto").cast("STRING"),
      col("tipo_quarto").cast("STRING"), col("capacidade_maxima").cast("INT"), col("preco_diaria_base").cast("DOUBLE"),
      col("andar").cast("INT"), col("vista").cast("STRING"), col("comodidades_quarto").cast("STRING"),
      col("possui_ar_condicionado").cast("BOOLEAN"), col("tamanho_quarto").cast("STRING"), col("status_manutencao").cast("STRING"),
      col("ultima_manutencao").cast("DATE"), col("eh_smoke_free").cast("BOOLEAN"), col("possui_kit_boas_vindas").cast("BOOLEAN"),
      col("numero_camas").cast("INT")
  )

@dlt.table(name="quartos_trusted", comment="Tabela Trusted de Quartos (carregada via MERGE).")
def quartos_trusted():
    return execute_merge_for_table(
        target_dlt_table_name="quartos_trusted",
        source_view_name="quartos_raw_prepared",
        primary_keys=["quarto_id"]
    )

# --- HOSPEDES ---
@dlt.view(name="hospedes_raw_prepared", comment="Prepara os dados brutos de hospedes.")
def hospedes_raw_prepared():
    df_raw = spark.read.table("dev.raw.hospedes_raw") # Leitura BATCH
    return df_raw.select(
        col("hospede_id").cast("INT"), col("nome_completo").cast("STRING"), col("cpf").cast("STRING"),
        col("data_nascimento").cast("DATE"), col("email").cast("STRING"), col("telefone").cast("STRING"),
        col("estado").cast("STRING"), col("nacionalidade").cast("STRING"), col("data_cadastro").cast("DATE"),
        col("programa_fidelidade").cast("STRING"), col("profissao").cast("STRING"), col("tipo_documento").cast("STRING"),
        col("numero_documento").cast("STRING"), col("empresa").cast("STRING"), col("eh_viajante_frequente").cast("BOOLEAN"),
        col("preferencias_hospede").cast("STRING"), col("restricoes_alimentares").cast("STRING"),
        col("data_ultima_hospedagem").cast("DATE"), col("total_hospedagens").cast("INT")
    )

@dlt.table(name="hospedes_trusted", comment="Tabela Trusted de Hóspedes (carregada via MERGE).")
def hospedes_trusted():
    return execute_merge_for_table(
        target_dlt_table_name="hospedes_trusted",
        source_view_name="hospedes_raw_prepared",
        primary_keys=["hospede_id"]
    )

# --- RESERVAS ---
@dlt.view(name="reservas_raw_prepared", comment="Prepara os dados brutos de reservas.")
def reservas_raw_prepared():
    df_raw = spark.read.table("dev.raw.reservas_raw") # Leitura BATCH
    return df_raw.select(
        col("reserva_id").cast("INT"), col("hospede_id").cast("INT"), col("quarto_id").cast("INT"),
        col("hotel_id").cast("INT"), col("data_reserva").cast("DATE"), col("data_checkin").cast("DATE"),
        col("data_checkout").cast("DATE"), col("numero_noites").cast("INT"), col("numero_adultos").cast("INT"),
        col("numero_criancas").cast("INT"), col("canal_reserva").cast("STRING"), col("status_reserva").cast("STRING"),
        col("data_cancelamento").cast("DATE"), col("solicitacoes_especiais").cast("STRING"),
        col("valor_total_estadia").cast("DOUBLE"), col("motivo_viagem").cast("STRING"), col("motivo_cancelamento").cast("STRING"),
        col("taxa_limpeza").cast("DOUBLE"), col("taxa_turismo").cast("DOUBLE"), col("avaliacao_hospede").cast("DOUBLE"),
        col("comentarios_hospede").cast("STRING")
    )

@dlt.table(name="reservas_trusted", comment="Tabela Trusted de Reservas (carregada via MERGE).")
def reservas_trusted():
    return execute_merge_for_table(
        target_dlt_table_name="reservas_trusted",
        source_view_name="reservas_raw_prepared",
        primary_keys=["reserva_id"]
    )

# --- CONSUMOS ---
@dlt.view(name="consumos_raw_prepared", comment="Prepara os dados brutos de consumos.")
def consumos_raw_prepared():
    df_raw = spark.read.table("dev.raw.consumos_raw") # Leitura BATCH
    return df_raw.select(
        col("consumo_id").cast("INT"), col("reserva_id").cast("INT"), col("hospede_id").cast("INT"),
        col("hotel_id").cast("INT"), col("data_consumo").cast("DATE"), col("nome_servico").cast("STRING"),
        col("quantidade").cast("INT"), col("valor_total_consumo").cast("DOUBLE"), col("hora_consumo").cast("STRING"),
        col("local_consumo").cast("STRING"), col("funcionario_responsavel").cast("STRING")
    )

@dlt.table(name="consumos_trusted", comment="Tabela Trusted de Consumos (carregada via MERGE).")
def consumos_trusted():
    return execute_merge_for_table(
        target_dlt_table_name="consumos_trusted",
        source_view_name="consumos_raw_prepared",
        primary_keys=["consumo_id"]
    )

# --- FATURAS ---
@dlt.view(name="faturas_raw_prepared", comment="Prepara os dados brutos de faturas.")
def faturas_raw_prepared():
    df_raw = spark.read.table("dev.raw.faturas_raw") # Leitura BATCH
    return df_raw.select(
        col("fatura_id").cast("INT"), col("reserva_id").cast("INT"), col("hospede_id").cast("INT"),
        col("data_emissao").cast("DATE"), col("data_vencimento").cast("DATE"), col("status_pagamento").cast("STRING"),
        col("forma_pagamento").cast("STRING"), col("subtotal_estadia").cast("DOUBLE"), col("subtotal_consumos").cast("DOUBLE"),
        col("descontos").cast("DOUBLE"), col("impostos").cast("DOUBLE"), col("valor_total").cast("DOUBLE"),
        col("data_pagamento").cast("DATE"), col("taxa_limpeza").cast("DOUBLE"), col("taxa_turismo").cast("DOUBLE"),
        col("taxa_servico").cast("DOUBLE"), col("numero_transacao").cast("STRING")
    )

@dlt.table(name="faturas_trusted", comment="Tabela Trusted de Faturas (carregada via MERGE).")
def faturas_trusted():
    return execute_merge_for_table(
        target_dlt_table_name="faturas_trusted",
        source_view_name="faturas_raw_prepared",
        primary_keys=["fatura_id"]
    )

# --- RESERVAS OTA ---
@dlt.view(name="reservas_ota_raw_prepared", comment="Prepara os dados brutos de reservas OTA.")
def reservas_ota_raw_prepared():
    df_raw = spark.read.table("dev.raw.reserva_ota_raw") # Leitura BATCH
    return df_raw.select(
        col("ota_reserva_id").cast("INT"), col("reserva_id").cast("INT"), col("ota_codigo_confirmacao").cast("STRING"),
        col("ota_nome_convidado").cast("STRING"), col("total_pago_ota").cast("DOUBLE"), col("taxa_comissao").cast("DOUBLE"),
        col("valor_liquido_recebido").cast("DOUBLE"), col("ota_solicitacoes_especificas").cast("STRING")
    )

@dlt.table(name="reservas_ota_trusted", comment="Tabela Trusted de Reservas OTA (carregada via MERGE).")
def reservas_ota_trusted():
    return execute_merge_for_table(
        target_dlt_table_name="reservas_ota_trusted",
        source_view_name="reservas_ota_raw_prepared",
        primary_keys=["ota_reserva_id"]
    )