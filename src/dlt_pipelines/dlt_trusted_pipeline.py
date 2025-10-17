# src/dlt_pipelines/dlt_trusted_pipeline.py

import dlt
from pyspark.sql.functions import col, current_timestamp
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

# --- Funções Helper ---
def get_business_cols(schema):
    return [f.name for f in schema.fields if f.name not in ("insert_date", "update_date")]

def build_update_set_clause(schema, primary_keys, source_alias="source", target_alias="target"):
    cols = get_business_cols(schema)
    set_clauses = [f"{target_alias}.`{c}` = {source_alias}.`{c}`" for c in cols if c not in primary_keys] # Não atualiza PKs
    set_clauses.append(f"{target_alias}.update_date = current_timestamp()") # Atualiza a data de update
    return ", ".join(set_clauses)

def build_insert_clause(schema, source_alias="source"):
    cols = [f"`{f.name}`" for f in schema.fields]
    values = []
    for f in schema.fields:
        if f.name == "insert_date":
            values.append("current_timestamp()")
        elif f.name == "update_date":
            values.append("NULL")
        else:
            values.append(f"{source_alias}.`{f.name}`")
    return f"({', '.join(cols)}) VALUES ({', '.join(values)})"

# --- Processamento das Tabelas (MODIFICADO para BATCH) ---

# --- HOTEIS ---
@dlt.view(
  name="hoteis_raw_prepared",
  comment="Prepara os dados brutos de hoteis para o MERGE."
)
def hoteis_raw_prepared():
  # MODIFICADO: readStream -> read
  df_raw = spark.read.table("dev.raw.hoteis_raw")
  # Seleciona e faz o cast (SEM colunas de auditoria da RAW)
  return df_raw.select(
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
    source_view_name = "hoteis_raw_prepared"
    target_table_name = "LIVE.hoteis_trusted"
    primary_keys = ["hotel_id"]
    schema = spark.table(source_view_name).schema
    
    # Adiciona colunas de auditoria ao schema se não existirem
    schema_fields = schema.fields
    if not any(f.name == "insert_date" for f in schema_fields):
        schema.add("insert_date", "timestamp")
    if not any(f.name == "update_date" for f in schema_fields):
         schema.add("update_date", "timestamp")

    dlt.read(source_view_name).createOrReplaceTempView(source_view_name)
    
    merge_condition = " AND ".join([f"target.`{k}` = source.`{k}`" for k in primary_keys])
    update_clause = build_update_set_clause(schema, primary_keys, "source", "target")
    insert_clause = build_insert_clause(schema, "source")

    spark.sql(f"""
        MERGE INTO {target_table_name} AS target
        USING {source_view_name} AS source
        ON {merge_condition}
        WHEN MATCHED THEN UPDATE SET {update_clause}
        WHEN NOT MATCHED THEN INSERT {insert_clause}
    """)
    return spark.table(target_table_name)


# --- QUARTOS ---
@dlt.view(name="quartos_raw_prepared", comment="Prepara os dados brutos de quartos.")
def quartos_raw_prepared():
  # MODIFICADO: readStream -> read
  df_raw = spark.read.table("dev.raw.quartos_raw")
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
    source_view_name = "quartos_raw_prepared"
    target_table_name = "LIVE.quartos_trusted"
    primary_keys = ["quarto_id"]
    schema = spark.table(source_view_name).schema
    schema.add("insert_date", "timestamp").add("update_date", "timestamp")

    dlt.read(source_view_name).createOrReplaceTempView(source_view_name)
    merge_condition = " AND ".join([f"target.`{k}` = source.`{k}`" for k in primary_keys])
    update_clause = build_update_set_clause(schema, primary_keys, "source", "target")
    insert_clause = build_insert_clause(schema, "source")

    spark.sql(f"""
        MERGE INTO {target_table_name} AS target USING {source_view_name} AS source
        ON {merge_condition} WHEN MATCHED THEN UPDATE SET {update_clause}
        WHEN NOT MATCHED THEN INSERT {insert_clause}
    """)
    return spark.table(target_table_name)


# --- HOSPEDES ---
@dlt.view(name="hospedes_raw_prepared", comment="Prepara os dados brutos de hospedes.")
def hospedes_raw_prepared():
    # MODIFICADO: readStream -> read
    df_raw = spark.read.table("dev.raw.hospedes_raw")
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
    source_view_name = "hospedes_raw_prepared"
    target_table_name = "LIVE.hospedes_trusted"
    primary_keys = ["hospede_id"]
    schema = spark.table(source_view_name).schema
    schema.add("insert_date", "timestamp").add("update_date", "timestamp")

    dlt.read(source_view_name).createOrReplaceTempView(source_view_name)
    merge_condition = " AND ".join([f"target.`{k}` = source.`{k}`" for k in primary_keys])
    update_clause = build_update_set_clause(schema, primary_keys, "source", "target")
    insert_clause = build_insert_clause(schema, "source")

    spark.sql(f"""
        MERGE INTO {target_table_name} AS target USING {source_view_name} AS source
        ON {merge_condition} WHEN MATCHED THEN UPDATE SET {update_clause}
        WHEN NOT MATCHED THEN INSERT {insert_clause}
    """)
    return spark.table(target_table_name)


# --- RESERVAS ---
@dlt.view(name="reservas_raw_prepared", comment="Prepara os dados brutos de reservas.")
def reservas_raw_prepared():
    # MODIFICADO: readStream -> read
    df_raw = spark.read.table("dev.raw.reservas_raw")
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
    source_view_name = "reservas_raw_prepared"
    target_table_name = "LIVE.reservas_trusted"
    primary_keys = ["reserva_id"]
    schema = spark.table(source_view_name).schema
    schema.add("insert_date", "timestamp").add("update_date", "timestamp")

    dlt.read(source_view_name).createOrReplaceTempView(source_view_name)
    merge_condition = " AND ".join([f"target.`{k}` = source.`{k}`" for k in primary_keys])
    update_clause = build_update_set_clause(schema, primary_keys, "source", "target")
    insert_clause = build_insert_clause(schema, "source")

    spark.sql(f"""
        MERGE INTO {target_table_name} AS target USING {source_view_name} AS source
        ON {merge_condition} WHEN MATCHED THEN UPDATE SET {update_clause}
        WHEN NOT MATCHED THEN INSERT {insert_clause}
    """)
    return spark.table(target_table_name)


# --- CONSUMOS ---
@dlt.view(name="consumos_raw_prepared", comment="Prepara os dados brutos de consumos.")
def consumos_raw_prepared():
    # MODIFICADO: readStream -> read
    df_raw = spark.read.table("dev.raw.consumos_raw")
    return df_raw.select(
        col("consumo_id").cast("INT"), col("reserva_id").cast("INT"), col("hospede_id").cast("INT"),
        col("hotel_id").cast("INT"), col("data_consumo").cast("DATE"), col("nome_servico").cast("STRING"),
        col("quantidade").cast("INT"), col("valor_total_consumo").cast("DOUBLE"), col("hora_consumo").cast("STRING"),
        col("local_consumo").cast("STRING"), col("funcionario_responsavel").cast("STRING")
    )

@dlt.table(name="consumos_trusted", comment="Tabela Trusted de Consumos (carregada via MERGE).")
def consumos_trusted():
    source_view_name = "consumos_raw_prepared"
    target_table_name = "LIVE.consumos_trusted"
    primary_keys = ["consumo_id"]
    schema = spark.table(source_view_name).schema
    schema.add("insert_date", "timestamp").add("update_date", "timestamp")

    dlt.read(source_view_name).createOrReplaceTempView(source_view_name)
    merge_condition = " ".join([f"target.`{k}` = source.`{k}`" for k in primary_keys])
    update_clause = build_update_set_clause(schema, primary_keys, "source", "target")
    insert_clause = build_insert_clause(schema, "source")

    spark.sql(f"""
        MERGE INTO {target_table_name} AS target USING {source_view_name} AS source
        ON {merge_condition} WHEN MATCHED THEN UPDATE SET {update_clause}
        WHEN NOT MATCHED THEN INSERT {insert_clause}
    """)
    return spark.table(target_table_name)


# --- FATURAS ---
@dlt.view(name="faturas_raw_prepared", comment="Prepara os dados brutos de faturas.")
def faturas_raw_prepared():
    # MODIFICADO: readStream -> read
    df_raw = spark.read.table("dev.raw.faturas_raw")
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
    source_view_name = "faturas_raw_prepared"
    target_table_name = "LIVE.faturas_trusted"
    primary_keys = ["fatura_id"]
    schema = spark.table(source_view_name).schema
    schema.add("insert_date", "timestamp").add("update_date", "timestamp")

    dlt.read(source_view_name).createOrReplaceTempView(source_view_name)
    merge_condition = " AND ".join([f"target.`{k}` = source.`{k}`" for k in primary_keys])
    update_clause = build_update_set_clause(schema, primary_keys, "source", "target")
    insert_clause = build_insert_clause(schema, "source")

    spark.sql(f"""
        MERGE INTO {target_table_name} AS target USING {source_view_name} AS source
        ON {merge_condition} WHEN MATCHED THEN UPDATE SET {update_clause}
        WHEN NOT MATCHED THEN INSERT {insert_clause}
    """)
    return spark.table(target_table_name)


# --- RESERVAS OTA ---
@dlt.view(name="reservas_ota_raw_prepared", comment="Prepara os dados brutos de reservas OTA.")
def reservas_ota_raw_prepared():
    # MODIFICADO: readStream -> read
    df_raw = spark.read.table("dev.raw.reserva_ota_raw")
    return df_raw.select(
        col("ota_reserva_id").cast("INT"), col("reserva_id").cast("INT"), col("ota_codigo_confirmacao").cast("STRING"),
        col("ota_nome_convidado").cast("STRING"), col("total_pago_ota").cast("DOUBLE"), col("taxa_comissao").cast("DOUBLE"),
        col("valor_liquido_recebido").cast("DOUBLE"), col("ota_solicitacoes_especificas").cast("STRING")
    )

@dlt.table(name="reservas_ota_trusted", comment="Tabela Trusted de Reservas OTA (carregada via MERGE).")
def reservas_ota_trusted():
    source_view_name = "reservas_ota_raw_prepared"
    target_table_name = "LIVE.reservas_ota_trusted"
    primary_keys = ["ota_reserva_id"]
    schema = spark.table(source_view_name).schema
    schema.add("insert_date", "timestamp").add("update_date", "timestamp")

    dlt.read(source_view_name).createOrReplaceTempView(source_view_name)
    merge_condition = " AND ".join([f"target.`{k}` = source.`{k}`" for k in primary_keys])
    update_clause = build_update_set_clause(schema, primary_keys, "source", "target")
    insert_clause = build_insert_clause(schema, "source")

    spark.sql(f"""
        MERGE INTO {target_table_name} AS target USING {source_view_name} AS source
        ON {merge_condition} WHEN MATCHED THEN UPDATE SET {update_clause}
        WHEN NOT MATCHED THEN INSERT {insert_clause}
    """)
    return spark.table(target_table_name)