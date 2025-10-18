# Databricks notebook source
# src/dlt_pipelines/dlt_trusted_pipeline.py

import dlt
from pyspark.sql.functions import col, current_timestamp
from pyspark.sql import SparkSession

# Embora o DLT injete 'spark', é boa prática inicializá-lo
# para rodar/testar fora do DLT se necessário.
spark = SparkSession.builder.getOrCreate()

# --- Camada TRUSTED (Prata) ---
# O objetivo desta camada é limpar, padronizar tipos de dados (Recarga Total).

@dlt.table(
  name="hoteis_trusted",
  comment="Tabela de hoteis limpa e padronizada (Recarga Total)."
)
def hoteis_trusted():
  # Ajustado para ler do schema 'dev' e nome com sufixo '_raw'
  df_hoteis_raw = spark.read.table("dev.raw.hoteis_raw")
  
  df_trusted = df_hoteis_raw.select(
    # Colunas originais
    col("hotel_id").cast("INT"),
    col("nome_hotel").cast("STRING"),
    col("endereco").cast("STRING"),
    col("cidade").cast("STRING"),
    col("estado").cast("STRING"),
    col("estrelas").cast("INT"),
    col("numero_quartos").cast("INT"),
    col("comodidades").cast("STRING"),
    # Novas colunas adicionadas
    col("telefone").cast("STRING"),
    col("email_contato").cast("STRING"),
    col("data_abertura").cast("DATE"),
    col("horario_checkin").cast("STRING"),
    col("horario_checkout").cast("STRING"),
    col("categoria_hotel").cast("STRING"),
    col("tipo_hotel").cast("STRING"),
    col("ano_fundacao").cast("INT"),
    col("capacidade_total").cast("INT"),
    col("possui_acessibilidade").cast("BOOLEAN"),
    col("certificacoes").cast("STRING"),
    col("latitude").cast("DOUBLE"),
    col("longitude").cast("DOUBLE"),
    col("descricao_hotel").cast("STRING"),
    col("numero_funcionarios").cast("INT")
    # Exclui insert_date e update_date da Raw
  ).withColumn("data_carga_trusted", current_timestamp()) # Adiciona auditoria da Trusted
  
  return df_trusted

# ---

@dlt.table(
  name="quartos_trusted",
  comment="Tabela de quartos limpa e com tipos de dados garantidos (Recarga Total)."
)
def quartos_trusted():
  df_quartos_raw = spark.read.table("dev.raw.quartos_raw")

  df_trusted = df_quartos_raw.select(
      # Colunas originais
      col("quarto_id").cast("INT"),
      col("hotel_id").cast("INT"),
      col("numero_quarto").cast("STRING"),
      col("tipo_quarto").cast("STRING"),
      col("capacidade_maxima").cast("INT"),
      col("preco_diaria_base").cast("DOUBLE"),
      # Novas colunas adicionadas
      col("andar").cast("INT"),
      col("vista").cast("STRING"),
      col("comodidades_quarto").cast("STRING"),
      col("possui_ar_condicionado").cast("BOOLEAN"),
      col("tamanho_quarto").cast("STRING"),
      col("status_manutencao").cast("STRING"),
      col("ultima_manutencao").cast("DATE"),
      col("eh_smoke_free").cast("BOOLEAN"),
      col("possui_kit_boas_vindas").cast("BOOLEAN"),
      col("numero_camas").cast("INT")
      # Exclui insert_date e update_date da Raw
  ).withColumn("data_carga_trusted", current_timestamp())

  return df_trusted

# ---

@dlt.table(
  name="hospedes_trusted",
  comment="Tabela de hóspedes limpa e padronizada (Recarga Total)."
)
def hospedes_trusted():
  df_hospedes_raw = spark.read.table("dev.raw.hospedes_raw")

  df_trusted = df_hospedes_raw.select(
      # Colunas originais
      col("hospede_id").cast("INT"),
      col("nome_completo").cast("STRING"),
      col("cpf").cast("STRING"),
      col("data_nascimento").cast("DATE"),
      col("email").cast("STRING"),
      col("estado").cast("STRING"),
      # Novas colunas adicionadas
      col("telefone").cast("STRING"),
      col("nacionalidade").cast("STRING"),
      col("data_cadastro").cast("DATE"),
      col("programa_fidelidade").cast("STRING"),
      col("profissao").cast("STRING"),
      col("tipo_documento").cast("STRING"),
      col("numero_documento").cast("STRING"),
      col("empresa").cast("STRING"),
      col("eh_viajante_frequente").cast("BOOLEAN"),
      col("preferencias_hospede").cast("STRING"),
      col("restricoes_alimentares").cast("STRING"),
      col("data_ultima_hospedagem").cast("DATE"),
      col("total_hospedagens").cast("INT")
      # Exclui insert_date e update_date da Raw
  ).withColumn("data_carga_trusted", current_timestamp())

  return df_trusted

# ---

@dlt.table(
  name="reservas_trusted",
  comment="Tabela de reservas com dados limpos e tipados (Recarga Total)."
)
def reservas_trusted():
  df_reservas_raw = spark.read.table("dev.raw.reservas_raw")

  df_trusted = df_reservas_raw.select(
      # Colunas originais
      col("reserva_id").cast("INT"),
      col("hospede_id").cast("INT"),
      col("quarto_id").cast("INT"),
      col("hotel_id").cast("INT"),
      col("data_reserva").cast("DATE"),
      col("data_checkin").cast("DATE"),
      col("data_checkout").cast("DATE"),
      col("numero_noites").cast("INT"),
      col("valor_total_estadia").cast("DOUBLE"),
      # Novas colunas adicionadas
      col("numero_adultos").cast("INT"),
      col("numero_criancas").cast("INT"),
      col("canal_reserva").cast("STRING"), # Veio da tabela antiga 'reservas_canal'
      col("status_reserva").cast("STRING"),
      col("data_cancelamento").cast("DATE"),
      col("solicitacoes_especiais").cast("STRING"),
      col("motivo_viagem").cast("STRING"),
      col("motivo_cancelamento").cast("STRING"),
      col("taxa_limpeza").cast("DOUBLE"),
      col("taxa_turismo").cast("DOUBLE"),
      col("avaliacao_hospede").cast("DOUBLE"),
      col("comentarios_hospede").cast("STRING")
      # Exclui insert_date e update_date da Raw
  ).withColumn("data_carga_trusted", current_timestamp())

  return df_trusted

# --- A tabela 'reservas_canal_trusted' foi REMOVIDA ---

# ---

@dlt.table(
  name="consumos_trusted",
  comment="Tabela de consumos com valores e datas padronizados (Recarga Total)."
)
def consumos_trusted():
  df_consumos_raw = spark.read.table("dev.raw.consumos_raw")

  df_trusted = df_consumos_raw.select(
      # Colunas originais (com nomes ajustados)
      col("consumo_id").cast("INT"),
      col("reserva_id").cast("INT"),
      col("hospede_id").cast("INT"),
      col("hotel_id").cast("INT"),
      col("data_consumo").cast("DATE"),
      col("nome_servico").cast("STRING"),     # Ajustado de 'tipo_servico'
      col("valor_total_consumo").cast("DOUBLE"), # Ajustado de 'valor'
      # Novas colunas adicionadas
      col("quantidade").cast("INT"),
      col("hora_consumo").cast("STRING"),
      col("local_consumo").cast("STRING"),
      col("funcionario_responsavel").cast("STRING")
      # Exclui insert_date e update_date da Raw
  ).withColumn("data_carga_trusted", current_timestamp())

  return df_trusted

# ---

@dlt.table(
  name="faturas_trusted",
  comment="Tabela de faturas com valores e datas padronizados (Recarga Total)."
)
def faturas_trusted():
  df_faturas_raw = spark.read.table("dev.raw.faturas_raw")

  df_trusted = df_faturas_raw.select(
      # Colunas originais
      col("fatura_id").cast("INT"),
      col("reserva_id").cast("INT"),
      col("hospede_id").cast("INT"),
      col("data_emissao").cast("DATE"),
      col("valor_total").cast("DOUBLE"),
      col("forma_pagamento").cast("STRING"),
      # Novas colunas adicionadas
      col("data_vencimento").cast("DATE"),
      col("status_pagamento").cast("STRING"),
      col("subtotal_estadia").cast("DOUBLE"),
      col("subtotal_consumos").cast("DOUBLE"),
      col("descontos").cast("DOUBLE"),
      col("impostos").cast("DOUBLE"),
      col("data_pagamento").cast("DATE"),
      col("taxa_limpeza").cast("DOUBLE"),
      col("taxa_turismo").cast("DOUBLE"),
      col("taxa_servico").cast("DOUBLE"),
      col("numero_transacao").cast("STRING")
      # Exclui insert_date e update_date da Raw
  ).withColumn("data_carga_trusted", current_timestamp())

  return df_trusted

# --- NOVA TABELA ADICIONADA ---
@dlt.table(
  name="reservas_ota_trusted",
  comment="Tabela de detalhes de reservas de OTA (Recarga Total)."
)
def reservas_ota_trusted():
  df_reservas_ota_raw = spark.read.table("dev.raw.reserva_ota_raw")

  df_trusted = df_reservas_ota_raw.select(
      col("ota_reserva_id").cast("INT"),
      col("reserva_id").cast("INT"),
      col("ota_codigo_confirmacao").cast("STRING"),
      col("ota_nome_convidado").cast("STRING"),
      col("total_pago_ota").cast("DOUBLE"),
      col("taxa_comissao").cast("DOUBLE"),
      col("valor_liquido_recebido").cast("DOUBLE"),
      col("ota_solicitacoes_especificas").cast("STRING")
      # Exclui insert_date e update_date da Raw
  ).withColumn("data_carga_trusted", current_timestamp())

  return df_trusted