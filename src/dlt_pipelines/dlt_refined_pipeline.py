# Databricks notebook source
# src/dlt_pipelines/dlt_refined_pipeline.py

import dlt
from pyspark.sql.functions import (
    col, monotonically_increasing_id, to_date, year, month,
    dayofmonth, quarter, date_format, expr, dayofweek, weekofyear
)
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

# --- Camada REFINED (Ouro) ---
# Modelo Star Schema (Recarga Total a cada execução)

# --- DIMENSÕES "PASSTHROUGH" (Cópias diretas da Trusted) ---

@dlt.table(
  name="d_hoteis",
  comment="Dimensão de Hotéis (Recarga Total)."
)
def d_hoteis():
  # Ajustado: Nome correto da tabela Trusted
  return spark.read.table("dev.trusted.hoteis_trusted")

@dlt.table(
  name="d_hospedes",
  comment="Dimensão de Hóspedes (Recarga Total)."
)
def d_hospedes():
  # Ajustado: Nome correto da tabela Trusted
  return spark.read.table("dev.trusted.hospedes_trusted")

@dlt.table(
  name="d_quartos",
  comment="Dimensão de Quartos (Recarga Total)."
)
def d_quartos():
  # Ajustado: Nome correto da tabela Trusted
  return spark.read.table("dev.trusted.quartos_trusted")


# --- NOVAS DIMENSÕES CRIADAS A PARTIR DOS DADOS (Recarga Total) ---

@dlt.table(
  name="d_canal",
  comment="Dimensão de Canais de Reserva (Recarga Total)."
)
def d_canal():
  # AJUSTADO: Lê 'canal_reserva' da tabela 'reservas_trusted'
  return (
    spark.read.table("dev.trusted.reservas_trusted")
      .select("canal_reserva")
      .distinct()
      .filter(col("canal_reserva").isNotNull())
      .withColumn("id_canal", monotonically_increasing_id()) # Chave substituta NÃO ESTÁVEL
  )

@dlt.table(
  name="d_servicos",
  comment="Dimensão de Serviços (Recarga Total)."
)
def d_servicos():
  # NOVO: Dimensão derivada de consumos_trusted
  return (
    spark.read.table("dev.trusted.consumos_trusted")
      .select("nome_servico")
      .distinct()
      .filter(col("nome_servico").isNotNull())
      .withColumn("id_servico", monotonically_increasing_id()) # Chave substituta NÃO ESTÁVEL
  )

@dlt.table(
  name="d_motivo_viagem",
  comment="Dimensão de Motivos de Viagem (Recarga Total)."
)
def d_motivo_viagem():
  # NOVO: Dimensão derivada de reservas_trusted
  return (
    spark.read.table("dev.trusted.reservas_trusted")
      .select("motivo_viagem")
      .distinct()
      .filter(col("motivo_viagem").isNotNull())
      .withColumn("id_motivo_viagem", monotonically_increasing_id()) # Chave substituta NÃO ESTÁVEL
  )

@dlt.table(
  name="d_status_reserva",
  comment="Dimensão de Status de Reserva (Recarga Total)."
)
def d_status_reserva():
  # NOVO: Dimensão derivada de reservas_trusted
  return (
    spark.read.table("dev.trusted.reservas_trusted")
      .select("status_reserva")
      .distinct()
      .filter(col("status_reserva").isNotNull())
      .withColumn("id_status_reserva", monotonically_increasing_id()) # Chave substituta NÃO ESTÁVEL
  )

@dlt.table(
  name="d_tempo",
  comment="Dimensão de Tempo com atributos de calendário (Recarga Total)."
)
def d_tempo():
  # AJUSTADO: Usa mais colunas de data de TODAS as tabelas trusted relevantes
  datas_reservas = spark.read.table("dev.trusted.reservas_trusted").select(col("data_reserva").alias("data"))
  datas_checkin = spark.read.table("dev.trusted.reservas_trusted").select(col("data_checkin").alias("data"))
  datas_checkout = spark.read.table("dev.trusted.reservas_trusted").select(col("data_checkout").alias("data"))
  datas_cancelamento = spark.read.table("dev.trusted.reservas_trusted").select(col("data_cancelamento").alias("data"))
  datas_consumo = spark.read.table("dev.trusted.consumos_trusted").select(col("data_consumo").alias("data"))
  datas_fatura_emissao = spark.read.table("dev.trusted.faturas_trusted").select(col("data_emissao").alias("data"))
  datas_fatura_pagamento = spark.read.table("dev.trusted.faturas_trusted").select(col("data_pagamento").alias("data"))
  datas_hospede_cadastro = spark.read.table("dev.trusted.hospedes_trusted").select(col("data_cadastro").alias("data"))
  datas_hospede_nasc = spark.read.table("dev.trusted.hospedes_trusted").select(col("data_nascimento").alias("data"))
  datas_hotel_abertura = spark.read.table("dev.trusted.hoteis_trusted").select(col("data_abertura").alias("data"))

  datas_unicas = (
      datas_reservas.unionByName(datas_checkin, allowMissingColumns=True)
      .unionByName(datas_checkout, allowMissingColumns=True)
      .unionByName(datas_cancelamento, allowMissingColumns=True)
      .unionByName(datas_consumo, allowMissingColumns=True)
      .unionByName(datas_fatura_emissao, allowMissingColumns=True)
      .unionByName(datas_fatura_pagamento, allowMissingColumns=True)
      .unionByName(datas_hospede_cadastro, allowMissingColumns=True)
      .unionByName(datas_hospede_nasc, allowMissingColumns=True)
      .unionByName(datas_hotel_abertura, allowMissingColumns=True)
      .distinct()
      .filter(col("data").isNotNull())
  )

  # Adiciona mais atributos de data úteis
  return (
    datas_unicas
      .withColumn("ano", year(col("data")))
      .withColumn("mes", month(col("data")))
      .withColumn("dia", dayofmonth(col("data")))
      .withColumn("trimestre", quarter(col("data")))
      .withColumn("semana_do_ano", weekofyear(col("data")))
      .withColumn("dia_da_semana", dayofweek(col("data"))) # 1=Domingo, 7=Sábado
      .withColumn("nome_dia_semana", date_format(col("data"), "EEEE"))
      .withColumn("nome_mes", date_format(col("data"), "MMMM"))
      .withColumn("eh_fim_de_semana", col("dia_da_semana").isin([1, 7]))
      .select(
        col("data").alias("id_data"), # A própria data serve como chave
        "ano", "mes", "dia", "trimestre", "semana_do_ano",
        "dia_da_semana", "nome_dia_semana", "nome_mes", "eh_fim_de_semana"
      )
  )


# --- TABELAS DE FATOS (Recarga Total) ---

@dlt.table(
  name="f_reservas",
  comment="Tabela de factos central de reservas (Recarga Total)."
)
def f_reservas():
  # Lê as tabelas da camada trusted (BATCH)
  reservas = spark.read.table("dev.trusted.reservas_trusted")
  # A tabela 'reservas_canal_trusted' NÃO existe mais

  # Lê as dimensões criadas neste pipeline (BATCH)
  d_canal_df = dlt.read("d_canal")
  d_tempo_df = dlt.read("d_tempo")
  d_motivo_df = dlt.read("d_motivo_viagem") # Leitura da nova dimensão
  d_status_df = dlt.read("d_status_reserva") # Leitura da nova dimensão

  # Passo 1: Junta reservas com as dimensões derivadas para obter as chaves substitutas
  reservas_com_ids = (
      reservas
      .join(d_canal_df, ["canal_reserva"], "left")
      .join(d_motivo_df, ["motivo_viagem"], "left")
      .join(d_status_df, ["status_reserva"], "left")
  )
  
  # Passo 2: Junta com a dimensão de tempo para obter as chaves de data
  reservas_finais = (
    reservas_com_ids
      .join(d_tempo_df.alias("tempo_reserva"), col("data_reserva") == col("tempo_reserva.id_data"), "left")
      .join(d_tempo_df.alias("tempo_checkin"), col("data_checkin") == col("tempo_checkin.id_data"), "left")
      .join(d_tempo_df.alias("tempo_checkout"), col("data_checkout") == col("tempo_checkout.id_data"), "left")
      .join(d_tempo_df.alias("tempo_cancel"), col("data_cancelamento") == col("tempo_cancel.id_data"), "left") # Adicionado join para data cancelamento
  )

  # Passo 3: Seleciona apenas as chaves e as métricas
  return reservas_finais.select(
    # Chaves Estrangeiras
    col("reserva_id"), # Chave de negócio (degenerada)
    col("hospede_id"),
    col("quarto_id"),
    col("hotel_id"),
    col("id_canal"), # Chave NÃO ESTÁVEL
    col("id_motivo_viagem"), # Chave NÃO ESTÁVEL
    col("id_status_reserva"), # Chave NÃO ESTÁVEL
    col("tempo_reserva.id_data").alias("id_data_reserva"),
    col("tempo_checkin.id_data").alias("id_data_checkin"),
    col("tempo_checkout.id_data").alias("id_data_checkout"),
    col("tempo_cancel.id_data").alias("id_data_cancelamento"), # Adicionada chave de data
    # Métricas (Factos) - Adicionadas novas métricas
    col("numero_noites"),
    col("numero_adultos"),
    col("numero_criancas"),
    col("valor_total_estadia"),
    col("taxa_limpeza"),
    col("taxa_turismo"),
    col("avaliacao_hospede")
  )

# --- NOVA TABELA DE FATOS: FATURAMENTO ---
@dlt.table(
  name="f_faturamento",
  comment="Tabela de factos de faturamento (Recarga Total)."
)
def f_faturamento():
  faturas = spark.read.table("dev.trusted.faturas_trusted")
  d_tempo_df = dlt.read("d_tempo")

  faturas_com_datas = (
    faturas
      .join(d_tempo_df.alias("t_emissao"), col("data_emissao") == col("t_emissao.id_data"), "left")
      .join(d_tempo_df.alias("t_venc"), col("data_vencimento") == col("t_venc.id_data"), "left")
      .join(d_tempo_df.alias("t_pgto"), col("data_pagamento") == col("t_pgto.id_data"), "left")
  )

  return faturas_com_datas.select(
    # Chaves
    col("fatura_id"),
    col("reserva_id"),
    col("hospede_id"),
    col("t_emissao.id_data").alias("id_data_emissao"),
    col("t_venc.id_data").alias("id_data_vencimento"),
    col("t_pgto.id_data").alias("id_data_pagamento"),
    # Métricas
    col("subtotal_estadia"),
    col("subtotal_consumos"),
    col("descontos"),
    col("impostos"),
    col("taxa_limpeza"),
    col("taxa_turismo"),
    col("taxa_servico"),
    col("valor_total")
  )

# --- NOVA TABELA DE FATOS: CONSUMOS ---
@dlt.table(
  name="f_consumos",
  comment="Tabela de factos de consumos (Recarga Total)."
)
def f_consumos():
  consumos = spark.read.table("dev.trusted.consumos_trusted")
  d_tempo_df = dlt.read("d_tempo")
  d_servicos_df = dlt.read("d_servicos")

  consumos_com_dims = (
    consumos
      .join(d_tempo_df, col("data_consumo") == col("id_data"), "left")
      .join(d_servicos_df, ["nome_servico"], "left")
  )

  return consumos_com_dims.select(
    # Chaves
    col("consumo_id"),
    col("reserva_id"),
    col("hospede_id"),
    col("hotel_id"),
    col("id_servico"), # Chave NÃO ESTÁVEL
    col("id_data").alias("id_data_consumo"),
    # Métricas
    col("quantidade"),
    col("valor_total_consumo")
  )

# --- NOVA TABELA DE FATOS: RESERVAS OTA ---
@dlt.table(
  name="f_reservas_ota",
  comment="Tabela de factos de Reservas OTA (Recarga Total)."
)
def f_reservas_ota():
  reservas_ota = spark.read.table("dev.trusted.reservas_ota_trusted")

  # Esta fato pode não precisar de join com dimensões neste momento
  return reservas_ota.select(
    # Chaves
    col("ota_reserva_id"),
    col("reserva_id"),
    # Métricas
    col("total_pago_ota"),
    col("taxa_comissao"),
    col("valor_liquido_recebido")
  )

# --- ALERTA SOBRE QUALIDADE DE DADOS ---
# Exemplo: Verificar se alguma chave substituta ficou nula na fato principal
@dlt.expect_or_fail("chaves_f_reservas_nao_nulas", "id_canal IS NOT NULL AND id_motivo_viagem IS NOT NULL AND id_status_reserva IS NOT NULL")
def check_f_reservas_keys():
    return dlt.read("f_reservas")