import dlt
from pyspark.sql.functions import (
    col, md5, current_timestamp, to_date, year, month, 
    dayofmonth, quarter, date_format, expr, dayofweek, weekofyear
)

# Camada REFINED (Ouro)
# Modelo Star Schema com dimensões carregadas via SCD1 (MERGE).

#------------------------------------------------------------------------
# 1. DIMENSÕES DE "PASSTHROUGH" (Carregadas com SCD1)
#------------------------------------------------------------------------

# HOTEIS
@dlt.table(
  name="d_hoteis_trusted_stream",
  comment="Stream de dados da camada trusted de hoteis.",
  temporary=True
)
def d_hoteis_trusted_stream():
  # Lê o Change Data Feed (CDF) da tabela trusted
  return dlt.read_stream("hoteis_trusted")

dlt.apply_changes(
  target = "d_hoteis", # Tabela final
  source = "d_hoteis_trusted_stream", # Stream que acabamos de definir
  keys = ["hotel_id"], # Chave natural para o MERGE
  sequence_by = "data_carga_trusted", # Coluna para ordenar eventos fora de ordem
  stored_as_scd_type = 1 # Aplica o MERGE (UPDATE/INSERT)
)

# HOSPEDES
@dlt.table(
  name="d_hospedes_trusted_stream",
  comment="Stream de dados da camada trusted de hospedes.",
  temporary=True
)
def d_hospedes_trusted_stream():
  return dlt.read_stream("hospedes_trusted")

dlt.apply_changes(
  target = "d_hospedes",
  source = "d_hospedes_trusted_stream",
  keys = ["hospede_id"],
  sequence_by = "data_carga_trusted",
  stored_as_scd_type = 1
)

# QUARTOS
@dlt.table(
  name="d_quartos_trusted_stream",
  comment="Stream de dados da camada trusted de quartos.",
  temporary=True
)
def d_quartos_trusted_stream():
  return dlt.read_stream("quartos_trusted")

dlt.apply_changes(
  target = "d_quartos",
  source = "d_quartos_trusted_stream",
  keys = ["quarto_id"],
  sequence_by = "data_carga_trusted",
  stored_as_scd_type = 1
)

#------------------------------------------------------------------------
# 2. NOVAS DIMENSÕES (DERIVADAS E CARREGADAS COM SCD1)
#------------------------------------------------------------------------

# CANAL
@dlt.table(
  name="d_canal_source_stream",
  comment="Stream de canais de reserva únicos da tabela de reservas.",
  temporary=True
)
def d_canal_source_stream():
  return (
    dlt.read_stream("reservas_trusted")
      .select(
        col("canal_reserva"),
        # Adiciona a chave substituta ESTÁVEL (MD5)
        md5(col("canal_reserva")).alias("id_canal") 
      )
      .distinct()
      .withColumn("data_carga", current_timestamp()) # Coluna para o sequence_by
  )

dlt.apply_changes(
  target = "d_canal",
  source = "d_canal_source_stream",
  keys = ["canal_reserva"], # Chave de negócio para o MERGE
  sequence_by = "data_carga",
  stored_as_scd_type = 1,
  # Ignora a 'id_canal' no update, ela só deve ser inserida na primeira vez
  except_column_list = ["id_canal"] 
)

# SERVIÇOS
@dlt.table(
  name="d_servicos_source_stream",
  comment="Stream de serviços únicos da tabela de consumos.",
  temporary=True
)
def d_servicos_source_stream():
  return (
    dlt.read_stream("consumos_trusted")
      .select(
        col("nome_servico"),
        md5(col("nome_servico")).alias("id_servico")
      )
      .distinct()
      .withColumn("data_carga", current_timestamp())
  )

dlt.apply_changes(
  target = "d_servicos",
  source = "d_servicos_source_stream",
  keys = ["nome_servico"],
  sequence_by = "data_carga",
  stored_as_scd_type = 1,
  except_column_list = ["id_servico"]
)

# MOTIVO VIAGEM
@dlt.table(
  name="d_motivo_viagem_source_stream",
  comment="Stream de motivos de viagem únicos.",
  temporary=True
)
def d_motivo_viagem_source_stream():
  return (
    dlt.read_stream("reservas_trusted")
      .select(
        col("motivo_viagem"),
        md5(col("motivo_viagem")).alias("id_motivo_viagem")
      )
      .distinct()
      .withColumn("data_carga", current_timestamp())
  )

dlt.apply_changes(
  target = "d_motivo_viagem",
  source = "d_motivo_viagem_source_stream",
  keys = ["motivo_viagem"],
  sequence_by = "data_carga",
  stored_as_scd_type = 1,
  except_column_list = ["id_motivo_viagem"]
)


# STATUS RESERVA
@dlt.table(
  name="d_status_reserva_source_stream",
  comment="Stream de status de reserva únicos.",
  temporary=True
)
def d_status_reserva_source_stream():
  return (
    dlt.read_stream("reservas_trusted")
      .select(
        col("status_reserva"),
        md5(col("status_reserva")).alias("id_status_reserva")
      )
      .distinct()
      .withColumn("data_carga", current_timestamp())
  )

dlt.apply_changes(
  target = "d_status_reserva",
  source = "d_status_reserva_source_stream",
  keys = ["status_reserva"],
  sequence_by = "data_carga",
  stored_as_scd_type = 1,
  except_column_list = ["id_status_reserva"]
)


# DIMENSÃO DE TEMPO
# (Esta dimensão continua sendo recarregamento total, o que é aceitável)
@dlt.table(
  name="d_tempo",
  comment="Dimensão de Tempo com atributos de calendário. (Completa)"
)
def d_tempo():
  # (O código da d_tempo da resposta anterior é mantido aqui)
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
      datas_reservas.union(datas_checkin).union(datas_checkout).union(datas_cancelamento)
      .union(datas_consumo).union(datas_fatura_emissao).union(datas_fatura_pagamento)
      .union(datas_hospede_cadastro).union(datas_hospede_nasc).union(datas_hotel_abertura)
      .distinct()
      .filter(col("data").isNotNull())
  )
  return (
    datas_unicas
      .withColumn("ano", year(col("data")))
      .withColumn("mes", month(col("data")))
      .withColumn("dia", dayofmonth(col("data")))
      .withColumn("trimestre", quarter(col("data")))
      .withColumn("semana_do_ano", weekofyear(col("data")))
      .withColumn("dia_da_semana", dayofweek(col("data")))
      .withColumn("nome_dia_semana", date_format(col("data"), "EEEE"))
      .withColumn("nome_mes", date_format(col("data"), "MMMM"))
      .withColumn("eh_fim_de_semana", col("dia_da_semana").isin([1, 7]))
      .select(
        col("data").alias("id_data"),
        "ano", "mes", "dia", "trimestre", "semana_do_ano", 
        "dia_da_semana", "nome_dia_semana", "nome_mes", "eh_fim_de_semana"
      )
  )

#------------------------------------------------------------------------
# 3. TABELAS DE FATOS (Recarregamento Total)
# Elas são reconstruídas a cada execução, usando as dimensões estáveis.
#------------------------------------------------------------------------

@dlt.table(
  name="f_reservas",
  comment="Tabela de factos central de reservas (reconstruída)."
)
def f_reservas():
  reservas = spark.read.table("dev.trusted.reservas_trusted")
  
  # Lê as DIMENSÕES FINAIS (não os streams)
  d_canal = dlt.read("d_canal")
  d_tempo = dlt.read("d_tempo")
  d_motivo = dlt.read("d_motivo_viagem")
  d_status = dlt.read("d_status_reserva")

  reservas_finais = (
    reservas
      .join(d_canal, ["canal_reserva"], "left")
      .join(d_motivo, ["motivo_viagem"], "left")
      .join(d_status, ["status_reserva"], "left")
      .join(d_tempo.alias("t_res"), col("data_reserva") == col("t_res.id_data"), "left")
      .join(d_tempo.alias("t_checkin"), col("data_checkin") == col("t_checkin.id_data"), "left")
      .join(d_tempo.alias("t_checkout"), col("data_checkout") == col("t_checkout.id_data"), "left")
      .join(d_tempo.alias("t_cancel"), col("data_cancelamento") == col("t_cancel.id_data"), "left")
  )

  return reservas_finais.select(
    # Chaves Estrangeiras
    col("reserva_id"), 
    col("hospede_id"),
    col("quarto_id"),
    col("hotel_id"),
    col("id_canal"), # <-- Chave estável (MD5)
    col("id_motivo_viagem"), # <-- Chave estável (MD5)
    col("id_status_reserva"), # <-- Chave estável (MD5)
    col("t_res.id_data").alias("id_data_reserva"),
    col("t_checkin.id_data").alias("id_data_checkin"),
    col("t_checkout.id_data").alias("id_data_checkout"),
    col("t_cancel.id_data").alias("id_data_cancelamento"),
    
    # Métricas (Factos)
    col("numero_noites"),
    col("numero_adultos"),
    col("numero_criancas"),
    col("valor_total_estadia"),
    col("taxa_limpeza"),
    col("taxa_turismo"),
    col("avaliacao_hospede")
  )


@dlt.table(
  name="f_faturamento",
  comment="Tabela de factos de faturamento (reconstruída)."
)
def f_faturamento():
  faturas = spark.read.table("dev.trusted.faturas_trusted")
  d_tempo = dlt.read("d_tempo")

  faturas_com_datas = (
    faturas
      .join(d_tempo.alias("t_emissao"), col("data_emissao") == col("t_emissao.id_data"), "left")
      .join(d_tempo.alias("t_venc"), col("data_vencimento") == col("t_venc.id_data"), "left")
      .join(d_tempo.alias("t_pgto"), col("data_pagamento") == col("t_pgto.id_data"), "left")
  )

  return faturas_com_datas.select(
    col("fatura_id"), 
    col("reserva_id"),
    col("hospede_id"),
    col("t_emissao.id_data").alias("id_data_emissao"),
    col("t_venc.id_data").alias("id_data_vencimento"),
    col("t_pgto.id_data").alias("id_data_pagamento"),
    col("subtotal_estadia"),
    col("subtotal_consumos"),
    col("descontos"),
    col("impostos"),
    col("taxa_limpeza"),
    col("taxa_turismo"),
    col("taxa_servico"),
    col("valor_total")
  )


@dlt.table(
  name="f_consumos",
  comment="Tabela de factos de consumos (reconstruída)."
)
def f_consumos():
  consumos = spark.read.table("dev.trusted.consumos_trusted")
  d_tempo = dlt.read("d_tempo")
  d_servicos = dlt.read("d_servicos")

  consumos_com_dims = (
    consumos
      .join(d_tempo, col("data_consumo") == col("id_data"), "left")
      .join(d_servicos, ["nome_servico"], "left")
  )

  return consumos_com_dims.select(
    col("consumo_id"), 
    col("reserva_id"),
    col("hospede_id"),
    col("hotel_id"),
    col("id_servico"), # <-- Chave estável (MD5)
    col("id_data").alias("id_data_consumo"),
    col("quantidade"),
    col("valor_total_consumo")
  )
  
@dlt.table(
  name="f_reservas_ota",
  comment="Tabela de factos sobre os custos e pagamentos de OTAs (reconstruída)."
)
def f_reservas_ota():
  reservas_ota = spark.read.table("dev.trusted.reservas_ota_trusted")
  
  return reservas_ota.select(
    col("ota_reserva_id"),
    col("reserva_id"),
    col("total_pago_ota"),
    col("taxa_comissao"),
    col("valor_liquido_recebido")
  )