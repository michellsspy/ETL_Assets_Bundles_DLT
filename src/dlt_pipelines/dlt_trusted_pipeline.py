# Databricks notebook source
import dlt
from pyspark.sql.functions import col, current_timestamp

# --- Camada TRUSTED (Prata) ---
# O objetivo desta camada é limpar, padronizar e enriquecer os dados.

@dlt.table(
  name="hoteis_trusted",
  comment="Tabela de hoteis limpa e padronizada com todas as colunas de negócios."
)
def hoteis_trusted():
  df_hoteis_raw = spark.read.table("dev.raw.hoteis")
  
  df_trusted = df_hoteis_raw.select(
    col("hotel_id").cast("INT"),
    col("nome_hotel").cast("STRING"),
    col("endereco").cast("STRING"),
    col("cidade").cast("STRING"),
    col("estado").cast("STRING"),
    col("estrelas").cast("INT"),
    col("numero_quartos").cast("INT"),
    col("comodidades").cast("STRING"),
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
  ).withColumn("data_carga_trusted", current_timestamp())
  
  return df_trusted


@dlt.table(
  name="quartos_trusted",
  comment="Tabela de quartos limpa e com tipos de dados garantidos."
)
def quartos_trusted():
  df_quartos_raw = spark.read.table("dev.raw.quartos")

  df_trusted = df_quartos_raw.select(
      col("quarto_id").cast("INT"),
      col("hotel_id").cast("INT"),
      col("numero_quarto").cast("STRING"),
      col("tipo_quarto").cast("STRING"),
      col("capacidade_maxima").cast("INT"),
      col("preco_diaria_base").cast("DOUBLE"),
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
  ).withColumn("data_carga_trusted", current_timestamp())

  return df_trusted


@dlt.table(
  name="hospedes_trusted",
  comment="Tabela de hóspedes limpa e padronizada."
)
def hospedes_trusted():
  df_hospedes_raw = spark.read.table("dev.raw.hospedes")

  df_trusted = df_hospedes_raw.select(
      col("hospede_id").cast("INT"),
      col("nome_completo").cast("STRING"),
      col("cpf").cast("STRING"),
      col("data_nascimento").cast("DATE"),
      col("email").cast("STRING"),
      col("telefone").cast("STRING"),
      col("estado").cast("STRING"),
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
  ).withColumn("data_carga_trusted", current_timestamp())

  return df_trusted


@dlt.table(
  name="reservas_trusted",
  comment="Tabela de reservas com dados limpos e tipados."
)
def reservas_trusted():
  df_reservas_raw = spark.read.table("dev.raw.reservas")

  df_trusted = df_reservas_raw.select(
      col("reserva_id").cast("INT"),
      col("hospede_id").cast("INT"),
      col("quarto_id").cast("INT"),
      col("hotel_id").cast("INT"),
      col("data_reserva").cast("DATE"),
      col("data_checkin").cast("DATE"),
      col("data_checkout").cast("DATE"),
      col("numero_noites").cast("INT"),
      col("numero_adultos").cast("INT"),
      col("numero_criancas").cast("INT"),
      col("canal_reserva").cast("STRING"),
      col("status_reserva").cast("STRING"),
      col("data_cancelamento").cast("DATE"),
      col("solicitacoes_especiais").cast("STRING"),
      col("valor_total_estadia").cast("DOUBLE"),
      col("motivo_viagem").cast("STRING"),
      col("motivo_cancelamento").cast("STRING"),
      col("taxa_limpeza").cast("DOUBLE"),
      col("taxa_turismo").cast("DOUBLE"),
      col("avaliacao_hospede").cast("DOUBLE"),
      col("comentarios_hospede").cast("STRING")
  ).withColumn("data_carga_trusted", current_timestamp())

  return df_trusted


@dlt.table(
  name="consumos_trusted",
  comment="Tabela de consumos com valores e datas padronizados."
)
def consumos_trusted():
  df_consumos_raw = spark.read.table("dev.raw.consumos")

  df_trusted = df_consumos_raw.select(
      col("consumo_id").cast("INT"),
      col("reserva_id").cast("INT"),
      col("hospede_id").cast("INT"),
      col("hotel_id").cast("INT"),
      col("data_consumo").cast("DATE"),
      col("nome_servico").cast("STRING"), 
      col("quantidade").cast("INT"),
      col("valor_total_consumo").cast("DOUBLE"), 
      col("hora_consumo").cast("STRING"),
      col("local_consumo").cast("STRING"),
      col("funcionario_responsavel").cast("STRING")
  ).withColumn("data_carga_trusted", current_timestamp())

  return df_trusted


@dlt.table(
  name="faturas_trusted",
  comment="Tabela de faturas com valores e datas padronizados."
)
def faturas_trusted():
  df_faturas_raw = spark.read.table("dev.raw.faturas")

  df_trusted = df_faturas_raw.select(
      col("fatura_id").cast("INT"),
      col("reserva_id").cast("INT"),
      col("hospede_id").cast("INT"),
      col("data_emissao").cast("DATE"),
      col("data_vencimento").cast("DATE"),
      col("status_pagamento").cast("STRING"),
      col("forma_pagamento").cast("STRING"),
      col("subtotal_estadia").cast("DOUBLE"),
      col("subtotal_consumos").cast("DOUBLE"),
      col("descontos").cast("DOUBLE"),
      col("impostos").cast("DOUBLE"),
      col("valor_total").cast("DOUBLE"),
      col("data_pagamento").cast("DATE"),
      col("taxa_limpeza").cast("DOUBLE"),
      col("taxa_turismo").cast("DOUBLE"),
      col("taxa_servico").cast("DOUBLE"),
      col("numero_transacao").cast("STRING")
  ).withColumn("data_carga_trusted", current_timestamp())

  return df_trusted


@dlt.table(
  name="reservas_ota_trusted",
  comment="Tabela de detalhes de reservas de OTA (Online Travel Agencies)."
)
def reservas_ota_trusted():
  df_reservas_ota_raw = spark.read.table("dev.raw.reserva_ota") # Nome da tabela raw

  df_trusted = df_reservas_ota_raw.select(
      col("ota_reserva_id").cast("INT"),
      col("reserva_id").cast("INT"),
      col("ota_codigo_confirmacao").cast("STRING"),
      col("ota_nome_convidado").cast("STRING"),
      col("total_pago_ota").cast("DOUBLE"),
      col("taxa_comissao").cast("DOUBLE"),
      col("valor_liquido_recebido").cast("DOUBLE"),
      col("ota_solicitacoes_especificas").cast("STRING")
  ).withColumn("data_carga_trusted", current_timestamp())

  return df_trusted