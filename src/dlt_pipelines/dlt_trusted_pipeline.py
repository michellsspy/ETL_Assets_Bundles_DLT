# Databricks notebook source
import dlt
from pyspark.sql.functions import col, current_timestamp
# Importe o spark session - necessário para spark.readStream
from pyspark.sql import SparkSession

# Obtenha a sessão Spark (necessário para spark.readStream)
spark = SparkSession.builder.getOrCreate()

# --- Camada TRUSTED (Prata) ---
# Implementada com SCD Type 1 usando apply_changes

# --- HOTEIS ---
@dlt.table(
  name="hoteis_raw_stream",
  comment="Stream de dados brutos de hoteis, tipados e com data de carga.",
  temporary=True
)
def hoteis_raw_stream():
  # CORREÇÃO: Use spark.readStream.table para ler tabelas externas
  df_raw = spark.readStream.table("dev.raw.hoteis")
  return df_raw.select(
    col("hotel_id").cast("INT"),
    col("nome_hotel").cast("STRING"),
    # ... (resto das colunas de hoteis)
    col("numero_funcionarios").cast("INT"),
    col("update_date")
  ).withColumn("data_carga_trusted", current_timestamp())

@dlt.table(
    name="hoteis_trusted",
    comment="Tabela Trusted de Hoteis (SCD Type 1)."
)
def hoteis_trusted():
    return dlt.apply_changes(
        target = "hoteis_trusted",
        source = "hoteis_raw_stream",
        keys = ["hotel_id"],
        sequence_by = col("update_date"),
        stored_as_scd_type = 1
    )

# --- QUARTOS ---
@dlt.table(name="quartos_raw_stream", temporary=True)
def quartos_raw_stream():
  # CORREÇÃO: Use spark.readStream.table
  df_raw = spark.readStream.table("dev.raw.quartos")
  return df_raw.select(
      col("quarto_id").cast("INT"), col("hotel_id").cast("INT"),
      # ... (resto das colunas de quartos)
      col("numero_camas").cast("INT"), col("update_date")
  ).withColumn("data_carga_trusted", current_timestamp())

@dlt.table(name="quartos_trusted", comment="Tabela Trusted de Quartos (SCD Type 1).")
def quartos_trusted():
    return dlt.apply_changes(
        target = "quartos_trusted", source = "quartos_raw_stream", keys = ["quarto_id"],
        sequence_by = col("update_date"), stored_as_scd_type = 1
    )

# --- HOSPEDES ---
@dlt.table(name="hospedes_raw_stream", temporary=True)
def hospedes_raw_stream():
    # CORREÇÃO: Use spark.readStream.table
    df_raw = spark.readStream.table("dev.raw.hospedes")
    return df_raw.select(
        col("hospede_id").cast("INT"), col("nome_completo").cast("STRING"),
        # ... (resto das colunas de hospedes)
        col("total_hospedagens").cast("INT"), col("update_date")
    ).withColumn("data_carga_trusted", current_timestamp())

@dlt.table(name="hospedes_trusted", comment="Tabela Trusted de Hóspedes (SCD Type 1).")
def hospedes_trusted():
    return dlt.apply_changes(
        target = "hospedes_trusted", source = "hospedes_raw_stream", keys = ["hospede_id"],
        sequence_by = col("update_date"), stored_as_scd_type = 1
    )

# --- RESERVAS ---
@dlt.table(name="reservas_raw_stream", temporary=True)
def reservas_raw_stream():
    # CORREÇÃO: Use spark.readStream.table
    df_raw = spark.readStream.table("dev.raw.reservas")
    return df_raw.select(
        col("reserva_id").cast("INT"), col("hospede_id").cast("INT"),
        # ... (resto das colunas de reservas)
        col("comentarios_hospede").cast("STRING"), col("update_date")
    ).withColumn("data_carga_trusted", current_timestamp())

@dlt.table(name="reservas_trusted", comment="Tabela Trusted de Reservas (SCD Type 1).")
def reservas_trusted():
    return dlt.apply_changes(
        target = "reservas_trusted", source = "reservas_raw_stream", keys = ["reserva_id"],
        sequence_by = col("update_date"), stored_as_scd_type = 1
    )

# --- CONSUMOS ---
@dlt.table(name="consumos_raw_stream", temporary=True)
def consumos_raw_stream():
    # CORREÇÃO: Use spark.readStream.table
    df_raw = spark.readStream.table("dev.raw.consumos")
    return df_raw.select(
        col("consumo_id").cast("INT"), col("reserva_id").cast("INT"),
        # ... (resto das colunas de consumos)
        col("funcionario_responsavel").cast("STRING"), col("update_date")
    ).withColumn("data_carga_trusted", current_timestamp())

@dlt.table(name="consumos_trusted", comment="Tabela Trusted de Consumos (SCD Type 1).")
def consumos_trusted():
    return dlt.apply_changes(
        target = "consumos_trusted", source = "consumos_raw_stream", keys = ["consumo_id"],
        sequence_by = col("update_date"), stored_as_scd_type = 1
    )

# --- FATURAS ---
@dlt.table(name="faturas_raw_stream", temporary=True)
def faturas_raw_stream():
    # CORREÇÃO: Use spark.readStream.table
    df_raw = spark.readStream.table("dev.raw.faturas")
    return df_raw.select(
        col("fatura_id").cast("INT"), col("reserva_id").cast("INT"),
        # ... (resto das colunas de faturas)
        col("numero_transacao").cast("STRING"), col("update_date")
    ).withColumn("data_carga_trusted", current_timestamp())

@dlt.table(name="faturas_trusted", comment="Tabela Trusted de Faturas (SCD Type 1).")
def faturas_trusted():
    return dlt.apply_changes(
        target = "faturas_trusted", source = "faturas_raw_stream", keys = ["fatura_id"],
        sequence_by = col("update_date"), stored_as_scd_type = 1
    )

# --- RESERVAS OTA ---
@dlt.table(name="reservas_ota_raw_stream", temporary=True)
def reservas_ota_raw_stream():
    # CORREÇÃO: Use spark.readStream.table
    df_raw = spark.readStream.table("dev.raw.reserva_ota")
    return df_raw.select(
        col("ota_reserva_id").cast("INT"), col("reserva_id").cast("INT"),
        # ... (resto das colunas de reserva_ota)
        col("ota_solicitacoes_especificas").cast("STRING"), col("update_date")
    ).withColumn("data_carga_trusted", current_timestamp())

@dlt.table(name="reservas_ota_trusted", comment="Tabela Trusted de Reservas OTA (SCD Type 1).")
def reservas_ota_trusted():
    return dlt.apply_changes(
        target = "reservas_ota_trusted", source = "reservas_ota_raw_stream", keys = ["ota_reserva_id"],
        sequence_by = col("update_date"), stored_as_scd_type = 1
    )