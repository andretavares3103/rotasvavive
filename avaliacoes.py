# -*- coding: utf-8 -*-
import streamlit as st
import pandas as pd
import numpy as np
import os
import locale
import json
import tempfile
from datetime import datetime, timedelta
from geopy.distance import geodesic
import io

import smtplib
from email.mime.text import MIMEText


# =========================
# ARQUIVOS / CONSTANTES
# =========================
PORTAL_EXCEL = "portal_atendimentos_clientes.xlsx"
PORTAL_OS_LIST = "portal_atendimentos_os_list.json"

ACEITES_FILE = "aceites.xlsx"
ROTAS_FILE = "rotas_bh_dados_tratados_completos.xlsx"

CONFIG_FILE = "config_portal.json"

st.set_page_config(page_title="BELO HORIZONTE || Otimização Rotas Vavivê", layout="wide")


# =========================
# CONFIG (MAX_PROF_COLS + LIMITES)
# =========================
def load_config():
    default = {
        "MAX_PROF_COLS": 4,                   # quantas profissionais por OS no Excel (pipeline)
        "MAX_ACEITES_SIM_POR_OS": 1,          # quantos "SIM" por OS antes de ocultar no portal
        "MAX_ACEITES_SIM_POR_TEL_TOTAL": 1,   # quantos "SIM" por telefone no portal (total)
        "MAX_ACEITES_POR_TEL_POR_OS": 1       # quantos aceites (SIM/NAO) por tel na mesma OS
    }
    try:
        if os.path.exists(CONFIG_FILE):
            with open(CONFIG_FILE, "r", encoding="utf-8") as f:
                data = json.load(f) or {}
            cfg = dict(default)
            cfg.update(data)

            # sanitização
            cfg["MAX_PROF_COLS"] = int(cfg.get("MAX_PROF_COLS", default["MAX_PROF_COLS"]))
            cfg["MAX_PROF_COLS"] = max(1, min(cfg["MAX_PROF_COLS"], 30))

            for k in ["MAX_ACEITES_SIM_POR_OS", "MAX_ACEITES_SIM_POR_TEL_TOTAL", "MAX_ACEITES_POR_TEL_POR_OS"]:
                cfg[k] = int(cfg.get(k, default[k]))
                cfg[k] = max(0, min(cfg[k], 50))

            return cfg
    except Exception:
        pass
    return default


def save_config(cfg: dict):
    # grava somente chaves conhecidas
    keep = ["MAX_PROF_COLS", "MAX_ACEITES_SIM_POR_OS", "MAX_ACEITES_SIM_POR_TEL_TOTAL", "MAX_ACEITES_POR_TEL_POR_OS"]
    out = {k: int(cfg[k]) for k in keep if k in cfg}
    with open(CONFIG_FILE, "w", encoding="utf-8") as f:
        json.dump(out, f, ensure_ascii=False, indent=2)


CONFIG = load_config()


# =========================
# HELPERS (normalização / datas)
# =========================
def normalize_phone_br(tel: str) -> str:
    s = str(tel or "").strip()
    s = "".join([c for c in s if c.isdigit()])
    if not s:
        return ""
    # remove 55 inicial se vier duplicado
    if s.startswith("55") and len(s) >= 12:
        s = s[2:]
    return s


def normalize_os(x) -> str:
    try:
        return str(int(float(x))).strip()
    except Exception:
        return ""


# Tente configurar locale pt_BR (opcional)
try:
    locale.setlocale(locale.LC_TIME, 'pt_BR.UTF-8')
except Exception:
    pass


def formatar_data_portugues(data):
    dias_pt = {
        "Monday": "segunda-feira",
        "Tuesday": "terça-feira",
        "Wednesday": "quarta-feira",
        "Thursday": "quinta-feira",
        "Friday": "sexta-feira",
        "Saturday": "sábado",
        "Sunday": "domingo"
    }
    if pd.isnull(data) or data == "":
        return ""
    try:
        s = str(data)
        if len(s) >= 10 and s[4] == '-' and s[7] == '-':
            # AAAA-MM-DD
            ano, mes, dia = int(s[0:4]), int(s[5:7]), int(s[8:10])
            dt = pd.Timestamp(year=ano, month=mes, day=dia)
        else:
            dt = pd.to_datetime(data, dayfirst=True, errors='coerce')

        if pd.isnull(dt):
            return str(data)

        dia_semana_en = dt.strftime("%A")
        dia_semana_pt = dias_pt.get(dia_semana_en, dia_semana_en)
        return f"{dia_semana_pt}, {dt.strftime('%d/%m/%Y')}"
    except Exception:
        return str(data)


def traduzir_dia_semana(date_obj):
    dias_pt = {
        "Monday": "segunda-feira", "Tuesday": "terça-feira", "Wednesday": "quarta-feira",
        "Thursday": "quinta-feira", "Friday": "sexta-feira", "Saturday": "sábado", "Sunday": "domingo"
    }
    return dias_pt.get(date_obj.strftime('%A'), date_obj.strftime('%A'))


def formatar_nome_simples(nome):
    nome = str(nome or "").strip()
    nome = nome.replace("CI ", "").replace("Ci ", "").replace("C i ", "").replace("C I ", "")
    partes = nome.split()
    if partes and partes[0].lower() in ['ana', 'maria'] and len(partes) > 1:
        return " ".join(partes[:2])
    elif partes:
        return partes[0]
    return nome


# =========================
# E-MAIL (opcional via st.secrets)
# =========================
def enviar_email_aceite_gmail(os_id, profissional, telefone):
    """
    Para habilitar, configure no st.secrets:
    [gmail]
    remetente="..."
    senha_app="..."
    destinatario="..."
    """
    try:
        remetente = st.secrets["gmail"]["remetente"]
        senha = st.secrets["gmail"]["senha_app"]
        destinatario = st.secrets["gmail"]["destinatario"]
    except Exception:
        return  # sem secrets, não envia

    assunto = f"Novo aceite registrado | OS {os_id}"
    corpo = f"""
Um novo aceite foi registrado:

OS: {os_id}
Profissional: {profissional}
Telefone: {telefone}
Data/Hora: {datetime.now().strftime("%d/%m/%Y %H:%M:%S")}
"""

    msg = MIMEText(corpo)
    msg['Subject'] = assunto
    msg['From'] = remetente
    msg['To'] = destinatario

    try:
        with smtplib.SMTP_SSL('smtp.gmail.com', 465) as smtp:
            smtp.login(remetente, senha)
            smtp.sendmail(remetente, destinatario, msg.as_string())
    except Exception:
        pass


# =========================
# ACEITES: leitura / regras / salvar
# =========================
def load_aceites_df():
    if os.path.exists(ACEITES_FILE):
        df = pd.read_excel(ACEITES_FILE)
    else:
        df = pd.DataFrame(columns=[
            "OS", "Profissional", "Telefone", "Aceitou",
            "Data do Aceite", "Dia da Semana", "Horário do Aceite", "Origem"
        ])

    # normalizações
    if "OS" in df.columns:
        df["OS"] = df["OS"].apply(normalize_os)
    else:
        df["OS"] = ""

    if "Telefone" in df.columns:
        df["Telefone"] = df["Telefone"].apply(normalize_phone_br)
    else:
        df["Telefone"] = ""

    if "Aceitou" not in df.columns:
        df["Aceitou"] = ""

    return df


def _count_sim_por_os(df_aceites, os_id: str) -> int:
    os_id = normalize_os(os_id)
    sim = df_aceites[
        (df_aceites["OS"] == os_id) &
        (df_aceites["Aceitou"].astype(str).str.strip().str.lower() == "sim")
    ]
    return int(len(sim))


def _count_sim_por_tel_total(df_aceites, telefone: str) -> int:
    telefone = normalize_phone_br(telefone)
    sim = df_aceites[
        (df_aceites["Telefone"] == telefone) &
        (df_aceites["Aceitou"].astype(str).str.strip().str.lower() == "sim")
    ]
    return int(len(sim))


def _count_por_tel_por_os(df_aceites, telefone: str, os_id: str) -> int:
    telefone = normalize_phone_br(telefone)
    os_id = normalize_os(os_id)
    x = df_aceites[
        (df_aceites["Telefone"] == telefone) &
        (df_aceites["OS"] == os_id)
    ]
    return int(len(x))


def salvar_aceite(os_id, profissional, telefone, aceitou: bool, origem=None):
    cfg = load_config()

    profissional = (profissional or "").strip()
    telefone_norm = normalize_phone_br(telefone)

    if not profissional:
        raise ValueError("Nome da Profissional é obrigatório.")
    if not telefone_norm:
        raise ValueError("Telefone é obrigatório (com DDD).")

    os_id_norm = normalize_os(os_id)
    if not os_id_norm:
        raise ValueError("OS inválida.")

    df = load_aceites_df()

    # Regra 1: limite de SIM por OS
    if aceitou and cfg["MAX_ACEITES_SIM_POR_OS"] > 0:
        if _count_sim_por_os(df, os_id_norm) >= cfg["MAX_ACEITES_SIM_POR_OS"]:
            raise ValueError(f"Esta OS já atingiu o limite de {cfg['MAX_ACEITES_SIM_POR_OS']} aceites SIM.")

    # Regra 2: limite de SIM por telefone no total
    if aceitou and cfg["MAX_ACEITES_SIM_POR_TEL_TOTAL"] > 0:
        if _count_sim_por_tel_total(df, telefone_norm) >= cfg["MAX_ACEITES_SIM_POR_TEL_TOTAL"]:
            raise ValueError(f"Este telefone já atingiu o limite de {cfg['MAX_ACEITES_SIM_POR_TEL_TOTAL']} aceites SIM no portal.")

    # Regra 3: limite de registros por telefone na mesma OS (SIM ou NÃO)
    if cfg["MAX_ACEITES_POR_TEL_POR_OS"] > 0:
        if _count_por_tel_por_os(df, telefone_norm, os_id_norm) >= cfg["MAX_ACEITES_POR_TEL_POR_OS"]:
            raise ValueError(f"Este telefone já registrou o limite de {cfg['MAX_ACEITES_POR_TEL_POR_OS']} respostas nesta OS.")

    agora = datetime.now()
    data = agora.strftime("%d/%m/%Y")
    dia_semana = agora.strftime("%A")
    horario = agora.strftime("%H:%M:%S")

    nova_linha = {
        "OS": os_id_norm,
        "Profissional": profissional,
        "Telefone": telefone_norm,
        "Aceitou": "Sim" if aceitou else "Não",
        "Data do Aceite": data,
        "Dia da Semana": dia_semana,
        "Horário do Aceite": horario,
        "Origem": origem if origem else ""
    }
    df = pd.concat([df, pd.DataFrame([nova_linha])], ignore_index=True)
    df.to_excel(ACEITES_FILE, index=False)

    # opcional: email
    if aceitou:
        enviar_email_aceite_gmail(os_id_norm, profissional, telefone_norm)


# =========================
# MENSAGENS
# =========================
def gerar_mensagem_personalizada(
    nome_profissional, nome_cliente, data_servico, servico,
    duracao, rua, numero, complemento, bairro, cidade, latitude, longitude,
    ja_atendeu, hora_entrada, obs_prestador
):
    nome_profissional_fmt = formatar_nome_simples(nome_profissional)
    nome_cliente_fmt = str(nome_cliente).split()[0].strip().title()

    if isinstance(data_servico, str):
        data_dt = pd.to_datetime(data_servico, dayfirst=True, errors="coerce")
    else:
        data_dt = data_servico

    if pd.isnull(data_dt):
        data_formatada = ""
        dia_semana = ""
    else:
        dia_semana = traduzir_dia_semana(data_dt)
        data_formatada = data_dt.strftime("%d/%m/%Y")

    data_linha = f"{dia_semana}, {data_formatada}"

    endereco_str = f"{rua}, {numero}"
    if complemento and str(complemento).strip().lower() not in ["nan", "none", "-"]:
        endereco_str += f", {complemento}"

    maps_url = f"https://maps.google.com/?q={latitude},{longitude}" if pd.notnull(latitude) and pd.notnull(longitude) else ""

    fechamento = (
        "SIM ou NÃO para o aceite!" if ja_atendeu
        else "Acesse o link ao final da mensagem e responda com SIM caso tenha disponibilidade!"
    )

    rodape = """
O atendimento será confirmado após o aceite!
*1)*    Lembre que o cliente irá receber o *profissional indicado pela Vavivê*.
*2)*    Lembre-se das nossas confirmações do atendimento!

Abs, Vavivê!
"""

    mensagem = f"""Olá, Tudo bem com você?
Temos uma oportunidade especial para você dentro da sua rota!
*Cliente:* {nome_cliente_fmt}
📅 *Data:* {data_linha}
🛠️ *Serviço:* {servico}
🕒 *Hora de entrada:* {hora_entrada}
⏱️ *Duração do Atendimento:* {duracao}
📍 *Endereço:* {endereco_str}
📍 *Bairro:* {bairro}
🏙️ *Cidade:* {cidade}
💬 *Observações do Atendimento:* {obs_prestador}
*GOOGLE MAPAS* {"🌎 (" + maps_url + ")" if maps_url else ""}
{fechamento}
{rodape}
"""
    return mensagem


def padronizar_cpf_cnpj(coluna):
    return (
        coluna.astype(str)
        .str.replace(r'\D', '', regex=True)
        .str.zfill(14)
        .str.strip()
    )


def salvar_df(df, nome_arquivo, output_dir):
    caminho = os.path.join(output_dir, f"{nome_arquivo}.xlsx")
    df.to_excel(caminho, index=False)


# =========================
# PIPELINE (agora com max_prof_cols dinâmico)
# =========================
def pipeline(file_path, output_dir, max_prof_cols: int):
    import xlsxwriter
    from collections import defaultdict

    MAX_PROF_COLS = int(max_prof_cols)

    # ============================
    # 1) Base: leitura e normalização
    # ============================
    df_clientes_raw = pd.read_excel(file_path, sheet_name="Clientes")
    df_clientes = df_clientes_raw[[
        "ID","UpdatedAt","celular","cpf",
        "endereco-1-bairro","endereco-1-cidade","endereco-1-complemento",
        "endereco-1-estado","endereco-1-latitude","endereco-1-longitude",
        "endereco-1-numero","endereco-1-rua","nome"
    ]].copy()

    df_clientes["ID Cliente"] = (
        df_clientes["ID"].fillna("0").astype(str)
        .str.replace(r"\.0$", "", regex=True).str.strip()
    )
    df_clientes["CPF_CNPJ"] = padronizar_cpf_cnpj(df_clientes["cpf"])
    df_clientes["Celular"] = df_clientes["celular"].astype(str).str.strip()
    df_clientes["Complemento"] = df_clientes["endereco-1-complemento"].astype(str).str.strip()
    df_clientes["Número"] = df_clientes["endereco-1-numero"].astype(str).str.strip()
    df_clientes["Nome Cliente"] = df_clientes["nome"].astype(str).str.strip()

    df_clientes = df_clientes.rename(columns={
        "endereco-1-bairro": "Bairro",
        "endereco-1-cidade": "Cidade",
        "endereco-1-estado": "Estado",
        "endereco-1-latitude": "Latitude Cliente",
        "endereco-1-longitude": "Longitude Cliente",
        "endereco-1-rua": "Rua"
    })

    df_clientes["Latitude Cliente"] = pd.to_numeric(df_clientes["Latitude Cliente"], errors="coerce")
    df_clientes["Longitude Cliente"] = pd.to_numeric(df_clientes["Longitude Cliente"], errors="coerce")

    coord_invertida = df_clientes["Latitude Cliente"] < -40
    if coord_invertida.any():
        lat_temp = df_clientes.loc[coord_invertida, "Latitude Cliente"].copy()
        df_clientes.loc[coord_invertida, "Latitude Cliente"] = df_clientes.loc[coord_invertida, "Longitude Cliente"]
        df_clientes.loc[coord_invertida, "Longitude Cliente"] = lat_temp

    df_clientes["coordenadas_validas"] = df_clientes["Latitude Cliente"].notnull() & df_clientes["Longitude Cliente"].notnull()
    df_clientes = df_clientes.sort_values(by=["CPF_CNPJ", "coordenadas_validas"], ascending=[True, False])
    df_clientes = df_clientes.drop_duplicates(subset="CPF_CNPJ", keep="first")
    df_clientes.drop(columns=["coordenadas_validas"], inplace=True)

    df_clientes = df_clientes[[
        "ID Cliente","UpdatedAt","Celular","CPF_CNPJ",
        "Bairro","Cidade","Complemento","Estado","Latitude Cliente","Longitude Cliente",
        "Número","Rua","Nome Cliente"
    ]]
    salvar_df(df_clientes, "df_clientes", output_dir)

    # -------- Profissionais --------
    df_profissionais_raw = pd.read_excel(file_path, sheet_name="Profissionais")
    df_profissionais = df_profissionais_raw[[
        "ID","atendimentos_feitos","celular","cpf",
        "endereco-bairro","endereco-cidade","endereco-complemento","endereco-estado",
        "endereco-latitude","endereco-longitude","endereco-numero","endereco-rua","nome"
    ]].copy()

    df_profissionais["ID Prestador"] = (
        df_profissionais["ID"].fillna("0").astype(str)
        .str.replace(r"\.0$", "", regex=True).str.strip()
    )
    df_profissionais["Qtd Atendimentos"] = df_profissionais["atendimentos_feitos"].fillna(0).astype(int)
    df_profissionais["Celular"] = df_profissionais["celular"].astype(str).str.strip()
    df_profissionais["cpf"] = df_profissionais["cpf"].astype(str).str.replace(r"\D", "", regex=True).str.strip()
    df_profissionais["Complemento"] = df_profissionais["endereco-complemento"].astype(str).str.strip()
    df_profissionais["Número"] = df_profissionais["endereco-numero"].astype(str).str.strip()
    df_profissionais["Nome Prestador"] = df_profissionais["nome"].astype(str).str.strip()

    df_profissionais = df_profissionais.rename(columns={
        "endereco-bairro": "Bairro",
        "endereco-cidade": "Cidade",
        "endereco-estado": "Estado",
        "endereco-latitude": "Latitude Profissional",
        "endereco-longitude": "Longitude Profissional",
        "endereco-rua": "Rua"
    })

    df_profissionais = df_profissionais[~df_profissionais["Nome Prestador"].str.contains("inativo", case=False, na=False)].copy()

    df_profissionais["Latitude Profissional"] = pd.to_numeric(df_profissionais["Latitude Profissional"], errors="coerce")
    df_profissionais["Longitude Profissional"] = pd.to_numeric(df_profissionais["Longitude Profissional"], errors="coerce")
    df_profissionais = df_profissionais[
        df_profissionais["Latitude Profissional"].notnull() &
        df_profissionais["Longitude Profissional"].notnull()
    ].copy()

    df_profissionais = df_profissionais[[
        "ID Prestador","Qtd Atendimentos","Celular","cpf",
        "Bairro","Cidade","Complemento","Estado","Latitude Profissional","Longitude Profissional",
        "Número","Rua","Nome Prestador"
    ]]
    salvar_df(df_profissionais, "df_profissionais", output_dir)

    # -------- Preferências --------
    df_preferencias_raw = pd.read_excel(file_path, sheet_name="Preferencias")
    df_preferencias = df_preferencias_raw[[
        "CPF/CNPJ","Cliente","ID Profissional","Prestador"
    ]].copy()

    df_preferencias["CPF_CNPJ"] = padronizar_cpf_cnpj(df_preferencias["CPF/CNPJ"])
    df_preferencias["Nome Cliente"] = df_preferencias["Cliente"].astype(str).str.strip()
    df_preferencias["ID Prestador"] = (
        df_preferencias["ID Profissional"].fillna("0").astype(str)
        .str.replace(r"\.0$", "", regex=True).str.strip()
    )
    df_preferencias["Nome Prestador"] = df_preferencias["Prestador"].astype(str).str.strip()
    df_preferencias = df_preferencias[["CPF_CNPJ","Nome Cliente","ID Prestador","Nome Prestador"]]
    salvar_df(df_preferencias, "df_preferencias", output_dir)

    # -------- Bloqueio --------
    df_bloqueio_raw = pd.read_excel(file_path, sheet_name="Bloqueio")
    df_bloqueio = df_bloqueio_raw[[
        "CPF/CNPJ","Cliente","ID Profissional","Prestador"
    ]].copy()

    df_bloqueio["CPF_CNPJ"] = padronizar_cpf_cnpj(df_bloqueio["CPF/CNPJ"])
    df_bloqueio["Nome Cliente"] = df_bloqueio["Cliente"].astype(str).str.strip()
    df_bloqueio["ID Prestador"] = (
        df_bloqueio["ID Profissional"].fillna("0").astype(str)
        .str.replace(r"\.0$", "", regex=True).str.strip()
    )
    df_bloqueio["Nome Prestador"] = df_bloqueio["Prestador"].astype(str).str.strip()
    df_bloqueio = df_bloqueio[["CPF_CNPJ","Nome Cliente","ID Prestador","Nome Prestador"]]
    salvar_df(df_bloqueio, "df_bloqueio", output_dir)

    # -------- Queridinhos --------
    df_queridinhos_raw = pd.read_excel(file_path, sheet_name="Profissionais Preferenciais")
    df_queridinhos = df_queridinhos_raw[["ID Profissional","Profissional"]].copy()
    df_queridinhos["ID Prestador"] = (
        df_queridinhos["ID Profissional"].fillna("0").astype(str)
        .str.replace(r"\.0$", "", regex=True).str.strip()
    )
    df_queridinhos["Nome Prestador"] = df_queridinhos["Profissional"].astype(str).str.strip()
    df_queridinhos = df_queridinhos[["ID Prestador","Nome Prestador"]]
    salvar_df(df_queridinhos, "df_queridinhos", output_dir)

    # -------- Sumidinhos --------
    df_sumidinhos_raw = pd.read_excel(file_path, sheet_name="Baixa Disponibilidade")
    df_sumidinhos = df_sumidinhos_raw[["ID Profissional","Profissional"]].copy()
    df_sumidinhos["ID Prestador"] = (
        df_sumidinhos["ID Profissional"].fillna("0").astype(str)
        .str.replace(r"\.0$", "", regex=True).str.strip()
    )
    df_sumidinhos["Nome Prestador"] = df_sumidinhos["Profissional"].astype(str).str.strip()
    df_sumidinhos = df_sumidinhos[["ID Prestador","Nome Prestador"]]
    salvar_df(df_sumidinhos, "df_sumidinhos", output_dir)

    # -------- Atendimentos --------
    df_atendimentos = pd.read_excel(file_path, sheet_name="Atendimentos")
    colunas_desejadas = [
        "OS","Status Serviço","Data 1","Plano","CPF/ CNPJ","Cliente","Serviço",
        "Horas de serviço","Hora de entrada","Observações atendimento",
        "Observações prestador","Ponto de Referencia","#Num Prestador","Prestador"
    ]
    df_atendimentos = df_atendimentos[colunas_desejadas].copy()
    df_atendimentos["Data 1"] = pd.to_datetime(df_atendimentos["Data 1"], errors="coerce")
    df_atendimentos["CPF_CNPJ"] = padronizar_cpf_cnpj(df_atendimentos["CPF/ CNPJ"])
    df_atendimentos["Cliente"] = df_atendimentos["Cliente"].astype(str).str.strip()
    df_atendimentos["Duração do Serviço"] = df_atendimentos["Horas de serviço"]
    df_atendimentos["ID Prestador"] = (
        df_atendimentos["#Num Prestador"].fillna("0").astype(str)
        .str.replace(r"\.0$", "", regex=True).str.strip()
    )
    salvar_df(df_atendimentos, "df_atendimentos", output_dir)

    # -------- Histórico 60 dias --------
    hoje = datetime.now().date()
    limite = hoje - timedelta(days=60)
    data1_datetime = pd.to_datetime(df_atendimentos["Data 1"], errors="coerce")

    df_historico_60_dias = df_atendimentos[
        (df_atendimentos["Status Serviço"].astype(str).str.lower() != "cancelado") &
        (data1_datetime.dt.date < hoje) &
        (data1_datetime.dt.date >= limite)
    ].copy()

    df_historico_60_dias = df_historico_60_dias[[
        "CPF_CNPJ","Cliente","Data 1","Status Serviço","Serviço",
        "Duração do Serviço","Hora de entrada","ID Prestador","Prestador","Observações prestador"
    ]]
    salvar_df(df_historico_60_dias, "df_historico_60_dias", output_dir)

    df_cliente_prestador = df_historico_60_dias.groupby(["CPF_CNPJ","ID Prestador"]).size().reset_index(name="Qtd Atendimentos Cliente-Prestador")
    salvar_df(df_cliente_prestador, "df_cliente_prestador", output_dir)

    df_qtd_por_prestador = df_historico_60_dias.groupby("ID Prestador").size().reset_index(name="Qtd Atendimentos Prestador")
    salvar_df(df_qtd_por_prestador, "df_qtd_por_prestador", output_dir)

    # -------- Distâncias --------
    df_clientes_coord = df_clientes[["CPF_CNPJ","Latitude Cliente","Longitude Cliente"]].dropna().drop_duplicates("CPF_CNPJ")
    df_profissionais_coord = df_profissionais[["ID Prestador","Latitude Profissional","Longitude Profissional"]].dropna().drop_duplicates("ID Prestador")

    distancias = []
    for _, cliente in df_clientes_coord.iterrows():
        coord_cliente = (cliente["Latitude Cliente"], cliente["Longitude Cliente"])
        for _, profissional in df_profissionais_coord.iterrows():
            coord_prof = (profissional["Latitude Profissional"], profissional["Longitude Profissional"])
            distancia_km = round(geodesic(coord_cliente, coord_prof).km, 2)
            distancias.append({
                "CPF_CNPJ": cliente["CPF_CNPJ"],
                "ID Prestador": profissional["ID Prestador"],
                "Distância (km)": distancia_km
            })
    df_distancias = pd.DataFrame(distancias)
    salvar_df(df_distancias, "df_distancias", output_dir)

    # -------- Preferencias/Bloqueios com Geo --------
    df_preferencias_completo = df_preferencias.merge(df_clientes_coord, on="CPF_CNPJ", how="left").merge(df_profissionais_coord, on="ID Prestador", how="left")
    df_preferencias_completo = df_preferencias_completo[[
        "CPF_CNPJ","Nome Cliente","ID Prestador","Nome Prestador",
        "Latitude Cliente","Longitude Cliente",
        "Latitude Profissional","Longitude Profissional"
    ]]
    salvar_df(df_preferencias_completo, "df_preferencias_completo", output_dir)

    df_bloqueio_completo = df_bloqueio.merge(df_clientes_coord, on="CPF_CNPJ", how="left").merge(df_profissionais_coord, on="ID Prestador", how="left")
    df_bloqueio_completo = df_bloqueio_completo[[
        "CPF_CNPJ","Nome Cliente","ID Prestador","Nome Prestador",
        "Latitude Cliente","Longitude Cliente",
        "Latitude Profissional","Longitude Profissional"
    ]]
    salvar_df(df_bloqueio_completo, "df_bloqueio_completo", output_dir)

    # -------- Atendimentos futuros com localização --------
    ontem = datetime.now().date() - timedelta(days=1)
    df_futuros = df_atendimentos[
        (df_atendimentos["Status Serviço"].astype(str).str.lower() != "cancelado") &
        (df_atendimentos["Data 1"].dt.date > ontem)
    ].copy()

    df_futuros_com_clientes = df_futuros.merge(df_clientes_coord, on="CPF_CNPJ", how="left")

    colunas_uteis = [
        "OS","Data 1","Status Serviço","CPF_CNPJ","Cliente","Serviço",
        "Duração do Serviço","Hora de entrada","Ponto de Referencia",
        "ID Prestador","Prestador","Latitude Cliente","Longitude Cliente","Plano","Observações prestador"
    ]

    df_atendimentos_futuros_validos = df_futuros_com_clientes[
        df_futuros_com_clientes["Latitude Cliente"].notnull() &
        df_futuros_com_clientes["Longitude Cliente"].notnull()
    ][colunas_uteis].copy()
    salvar_df(df_atendimentos_futuros_validos, "df_atendimentos_futuros_validos", output_dir)

    df_atendimentos_sem_localizacao = df_futuros_com_clientes[
        df_futuros_com_clientes["Latitude Cliente"].isnull() |
        df_futuros_com_clientes["Longitude Cliente"].isnull()
    ][colunas_uteis].copy()
    salvar_df(df_atendimentos_sem_localizacao, "df_atendimentos_sem_localizacao", output_dir)

    # ============================
    # PARÂMETROS
    # ============================
    DELTA_KM = 1.0
    RAIO_QUERIDINHOS = 5.0
    GARANTIR_COTA_QUERIDINHO = True
    EVITAR_REPETIR_EM_LISTAS_NO_DIA = True

    # ----------------------------
    # Helpers de distância
    # ----------------------------
    def _dist_from_df(cpf, id_prof, df_dist):
        row = df_dist[
            (df_dist["CPF_CNPJ"] == cpf) &
            (df_dist["ID Prestador"].astype(str).str.strip() == str(id_prof).strip())
        ]
        return float(row["Distância (km)"].iloc[0]) if not row.empty else None

    def _parse_hora(hora_str):
        try:
            s = str(hora_str).strip()
            h, m = s.split(":")
            return (int(h), int(m))
        except Exception:
            return (99, 99)

    def _prof_ok(id_prof, df_profissionais_):
        prof = df_profissionais_[df_profissionais_["ID Prestador"].astype(str).str.strip() == str(id_prof).strip()]
        if prof.empty:
            return None
        if "inativo" in str(prof.iloc[0]["Nome Prestador"]).lower():
            return None
        if pd.isnull(prof.iloc[0]["Latitude Profissional"]) or pd.isnull(prof.iloc[0]["Longitude Profissional"]):
            return None
        return prof.iloc[0]

    def _qtd_cli(df_cliente_prestador_, cpf, id_prof):
        x = df_cliente_prestador_[
            (df_cliente_prestador_["CPF_CNPJ"] == cpf) &
            (df_cliente_prestador_["ID Prestador"].astype(str).str.strip() == str(id_prof).strip())
        ]
        return int(x["Qtd Atendimentos Cliente-Prestador"].iloc[0]) if not x.empty else 0

    def _qtd_tot(df_qtd_por_prestador_, id_prof):
        x = df_qtd_por_prestador_[df_qtd_por_prestador_["ID Prestador"].astype(str).str.strip() == str(id_prof).strip()]
        return int(x["Qtd Atendimentos Prestador"].iloc[0]) if not x.empty else 0

    def _ordena_os(df_do_dia):
        tmp = df_do_dia.copy()
        tmp["_hora_tuple"] = tmp["Hora de entrada"].apply(_parse_hora)
        tmp["_dur"] = tmp["Duração do Serviço"]
        return tmp.sort_values(by=["_hora_tuple", "_dur"], ascending=[True, False])

    # ============================
    # PRÉ-RESERVAS / CAMADAS 1..3
    # ============================
    preferida_do_cliente_no_dia     = defaultdict(dict)
    profissionais_reservadas_no_dia = defaultdict(set)
    profissionais_ocupadas_no_dia   = defaultdict(set)
    profissionais_sugeridas_no_dia  = defaultdict(set)

    pref_map = df_preferencias.set_index("CPF_CNPJ")["ID Prestador"].astype(str).str.strip().to_dict()

    for data_atend, df_do_dia in df_atendimentos_futuros_validos.groupby(df_atendimentos_futuros_validos["Data 1"].dt.date):
        candidatos = []
        for _, row in df_do_dia.iterrows():
            cpf = row["CPF_CNPJ"]
            id_pref = pref_map.get(cpf, "")
            if not id_pref:
                continue
            bloqueados = (
                df_bloqueio[df_bloqueio["CPF_CNPJ"] == cpf]["ID Prestador"]
                .astype(str).str.strip().tolist()
            )
            if id_pref in bloqueados:
                continue
            prof = _prof_ok(id_pref, df_profissionais)
            if prof is None:
                continue
            candidatos.append({
                "cpf": cpf,
                "id_prof": id_pref,
                "qtd_cli": _qtd_cli(df_cliente_prestador, cpf, id_pref),
                "dist_km": _dist_from_df(cpf, id_pref, df_distancias) or 9999.0,
                "hora": _parse_hora(row.get("Hora de entrada", "")),
            })

        por_prof = defaultdict(list)
        for c in candidatos:
            por_prof[c["id_prof"]].append(c)
        for id_prof, lst in por_prof.items():
            lst.sort(key=lambda x: (-x["qtd_cli"], x["dist_km"], x["hora"]))
            esc = lst[0]
            preferida_do_cliente_no_dia[data_atend][esc["cpf"]] = id_prof
            profissionais_reservadas_no_dia[data_atend].add(id_prof)

    os_primeira_candidata = {}  # (date, OS) -> (id_prof, crit_texto, criterio_nome)

    for data_atend, df_do_dia in df_atendimentos_futuros_validos.groupby(df_atendimentos_futuros_validos["Data 1"].dt.date):
        df_sorted = _ordena_os(df_do_dia)
        for _, row in df_sorted.iterrows():
            os_id = row["OS"]; cpf = row["CPF_CNPJ"]
            bloqueados = (
                df_bloqueio[df_bloqueio["CPF_CNPJ"] == cpf]["ID Prestador"]
                .astype(str).str.strip().tolist()
            )

            # 1) Preferida
            pref_id = preferida_do_cliente_no_dia[data_atend].get(cpf)
            if pref_id:
                if (pref_id not in bloqueados) and (pref_id not in profissionais_ocupadas_no_dia[data_atend]):
                    prof = _prof_ok(pref_id, df_profissionais)
                    if prof is not None:
                        crit = f"cliente: {_qtd_cli(df_cliente_prestador, cpf, pref_id)} | total: {_qtd_tot(df_qtd_por_prestador, pref_id)}"
                        d = _dist_from_df(cpf, pref_id, df_distancias)
                        if d is not None:
                            crit += f" — {d:.2f} km"
                        os_primeira_candidata[(data_atend, os_id)] = (pref_id, crit, "Preferência do Cliente")
                        profissionais_ocupadas_no_dia[data_atend].add(pref_id)
                        profissionais_sugeridas_no_dia[data_atend].add(pref_id)
                        continue

            # 2) Mais atendeu o cliente
            df_mais = df_cliente_prestador[df_cliente_prestador["CPF_CNPJ"] == cpf]
            if not df_mais.empty:
                max_at = df_mais["Qtd Atendimentos Cliente-Prestador"].max()
                ids_mais = df_mais[df_mais["Qtd Atendimentos Cliente-Prestador"] == max_at]["ID Prestador"].astype(str).tolist()
                ids_mais = sorted(
                    [i for i in ids_mais if i not in bloqueados and i not in profissionais_ocupadas_no_dia[data_atend] and _prof_ok(i, df_profissionais) is not None],
                    key=lambda i: (_dist_from_df(cpf, i, df_distancias) or 9999.0)
                )
                if ids_mais:
                    escolhido = ids_mais[0]
                    crit = f"cliente: {max_at} | total: {_qtd_tot(df_qtd_por_prestador, escolhido)}"
                    d = _dist_from_df(cpf, escolhido, df_distancias)
                    if d is not None:
                        crit += f" — {d:.2f} km"
                    os_primeira_candidata[(data_atend, os_id)] = (escolhido, crit, "Mais atendeu o cliente")
                    profissionais_ocupadas_no_dia[data_atend].add(escolhido)
                    profissionais_sugeridas_no_dia[data_atend].add(escolhido)
                    continue

            # 3) Último profissional (60 dias)
            df_hist = df_historico_60_dias[df_historico_60_dias["CPF_CNPJ"] == cpf].sort_values("Data 1", ascending=False)
            if not df_hist.empty:
                ult_id = str(df_hist["ID Prestador"].iloc[0]).strip()
                if (ult_id not in bloqueados) and (ult_id not in profissionais_ocupadas_no_dia[data_atend]) and (_prof_ok(ult_id, df_profissionais) is not None):
                    crit = f"cliente: {_qtd_cli(df_cliente_prestador, cpf, ult_id)} | total: {_qtd_tot(df_qtd_por_prestador, ult_id)}"
                    d = _dist_from_df(cpf, ult_id, df_distancias)
                    if d is not None:
                        crit += f" — {d:.2f} km"
                    os_primeira_candidata[(data_atend, os_id)] = (ult_id, crit, "Último profissional que atendeu")
                    profissionais_ocupadas_no_dia[data_atend].add(ult_id)
                    profissionais_sugeridas_no_dia[data_atend].add(ult_id)
                    continue

    # 3.5) Cota mínima de queridinhos
    if GARANTIR_COTA_QUERIDINHO:
        for data_atend, df_do_dia in df_atendimentos_futuros_validos.groupby(df_atendimentos_futuros_validos["Data 1"].dt.date):
            df_sorted = _ordena_os(df_do_dia)
            for _, qrow in df_queridinhos.iterrows():
                qid = str(qrow["ID Prestador"]).strip()
                if qid in profissionais_ocupadas_no_dia[data_atend]:
                    continue
                if EVITAR_REPETIR_EM_LISTAS_NO_DIA and qid in profissionais_sugeridas_no_dia[data_atend]:
                    continue
                for _, row in df_sorted.iterrows():
                    os_id = row["OS"]; cpf = row["CPF_CNPJ"]
                    if (data_atend, os_id) in os_primeira_candidata:
                        continue
                    if qid in profissionais_ocupadas_no_dia[data_atend]:
                        break
                    bloqueados = (
                        df_bloqueio[df_bloqueio["CPF_CNPJ"] == cpf]["ID Prestador"]
                        .astype(str).str.strip().tolist()
                    )
                    if qid in bloqueados:
                        continue
                    prof = _prof_ok(qid, df_profissionais)
                    if prof is None:
                        continue
                    d = _dist_from_df(cpf, qid, df_distancias)
                    if d is None or d > RAIO_QUERIDINHOS:
                        continue
                    crit = f"cliente: {_qtd_cli(df_cliente_prestador, cpf, qid)} | total: {_qtd_tot(df_qtd_por_prestador, qid)} — {d:.2f} km"
                    os_primeira_candidata[(data_atend, os_id)] = (qid, crit, "Cota mínima queridinho")
                    profissionais_ocupadas_no_dia[data_atend].add(qid)
                    profissionais_sugeridas_no_dia[data_atend].add(qid)
                    break

    # ============================
    # LOOP PRINCIPAL — montar colunas 1..MAX_PROF_COLS
    # ============================
    def _reservada_para_outro(data_atendimento, id_prof, cpf):
        id_prof = str(id_prof).strip()
        if id_prof not in profissionais_reservadas_no_dia[data_atendimento]:
            return False
        aloc = preferida_do_cliente_no_dia[data_atendimento]
        reservado_para = next((c for c, p in aloc.items() if str(p).strip() == id_prof), None)
        return bool(reservado_para and reservado_para != cpf)

    matriz_resultado_corrigida = []

    for _, atendimento in df_atendimentos_futuros_validos.iterrows():
        data_atendimento = atendimento["Data 1"].date()
        os_id = atendimento["OS"]
        cpf = atendimento["CPF_CNPJ"]
        nome_cliente = atendimento["Cliente"]
        data_1 = atendimento["Data 1"]
        servico = atendimento["Serviço"]
        duracao_servico = atendimento["Duração do Serviço"]
        hora_entrada = atendimento["Hora de entrada"]
        obs_prestador = atendimento["Observações prestador"]
        ponto_referencia = atendimento["Ponto de Referencia"]
        plano = atendimento.get("Plano", "")

        bloqueados = (
            df_bloqueio[df_bloqueio["CPF_CNPJ"] == cpf]["ID Prestador"]
            .astype(str).str.strip().tolist()
        )

        cli = df_clientes[df_clientes["CPF_CNPJ"] == cpf]
        if not cli.empty:
            rua = cli.iloc[0]["Rua"]; numero = cli.iloc[0]["Número"]
            complemento = cli.iloc[0]["Complemento"]; bairro = cli.iloc[0]["Bairro"]
            cidade = cli.iloc[0]["Cidade"]; latitude = cli.iloc[0]["Latitude Cliente"]; longitude = cli.iloc[0]["Longitude Cliente"]
        else:
            rua = numero = complemento = bairro = cidade = latitude = longitude = ""

        linha = {
            "OS": os_id, "CPF_CNPJ": cpf, "Nome Cliente": nome_cliente, "Plano": plano,
            "Data 1": data_1, "Serviço": servico, "Duração do Serviço": duracao_servico,
            "Hora de entrada": hora_entrada, "Observações prestador": obs_prestador,
            "Ponto de Referencia": ponto_referencia
        }

        app_url = "https://rotasvavive.streamlit.app/"
        linha["Mensagem Padrão"] = (
            f"👉 [Clique aqui para validar seu aceite]({app_url}?aceite={int(os_id)})\n\n" +
            gerar_mensagem_personalizada(
                "PROFISSIONAL", nome_cliente, data_1, servico, duracao_servico,
                rua, numero, complemento, bairro, cidade, latitude, longitude,
                ja_atendeu=False, hora_entrada=hora_entrada, obs_prestador=obs_prestador
            )
        )

        utilizados = set()
        col = 1

        def _add(id_prof, criterio_usado, ja_atendeu_flag):
            nonlocal col
            id_prof = str(id_prof).strip()
            if col > MAX_PROF_COLS:
                return False
            if EVITAR_REPETIR_EM_LISTAS_NO_DIA and id_prof in profissionais_sugeridas_no_dia[data_atendimento]:
                return False
            if id_prof in utilizados:
                return False
            if id_prof in bloqueados:
                return False
            if id_prof in profissionais_ocupadas_no_dia[data_atendimento]:
                return False
            prof = _prof_ok(id_prof, df_profissionais)
            if prof is None:
                return False
            if _reservada_para_outro(data_atendimento, id_prof, cpf):
                return False

            q_cli = _qtd_cli(df_cliente_prestador, cpf, id_prof)
            q_tot = _qtd_tot(df_qtd_por_prestador, id_prof)
            d = _dist_from_df(cpf, id_prof, df_distancias)
            crit = f"cliente: {q_cli} | total: {q_tot}" + (f" — {d:.2f} km" if d is not None else "")

            linha[f"Classificação da Profissional {col}"] = col
            linha[f"Critério {col}"] = crit
            linha[f"Nome Prestador {col}"] = prof["Nome Prestador"]
            linha[f"Celular {col}"] = prof["Celular"]
            linha[f"Mensagem {col}"] = gerar_mensagem_personalizada(
                prof["Nome Prestador"], nome_cliente, data_1, servico, duracao_servico,
                rua, numero, complemento, bairro, cidade, latitude, longitude,
                ja_atendeu=ja_atendeu_flag, hora_entrada=hora_entrada, obs_prestador=obs_prestador
            )
            linha[f"Critério Utilizado {col}"] = criterio_usado

            utilizados.add(id_prof)
            if EVITAR_REPETIR_EM_LISTAS_NO_DIA:
                profissionais_sugeridas_no_dia[data_atendimento].add(id_prof)
            col += 1
            return True

        # posição 1: resultado camadas 1..3 (inclui queridinho)
        primeira = os_primeira_candidata.get((data_atendimento, os_id))
        if primeira:
            idp, crit_text, criterio_nome = primeira
            prof = _prof_ok(idp, df_profissionais)
            if prof is not None and col <= MAX_PROF_COLS:
                linha[f"Classificação da Profissional {col}"] = col
                linha[f"Critério {col}"] = crit_text
                linha[f"Nome Prestador {col}"] = prof["Nome Prestador"]
                linha[f"Celular {col}"] = prof["Celular"]
                linha[f"Mensagem {col}"] = gerar_mensagem_personalizada(
                    prof["Nome Prestador"], nome_cliente, data_1, servico, duracao_servico,
                    rua, numero, complemento, bairro, cidade, latitude, longitude,
                    ja_atendeu=True, hora_entrada=hora_entrada, obs_prestador=obs_prestador
                )
                linha[f"Critério Utilizado {col}"] = criterio_nome
                utilizados.add(str(idp).strip())
                col += 1

                # Se 1ª é Preferência do Cliente, não listar mais ninguém
                if criterio_nome == "Preferência do Cliente":
                    matriz_resultado_corrigida.append(linha)
                    continue

        # 2) Mais atendeu o cliente
        if col <= MAX_PROF_COLS:
            df_mais = df_cliente_prestador[df_cliente_prestador["CPF_CNPJ"] == cpf]
            if not df_mais.empty:
                max_at = df_mais["Qtd Atendimentos Cliente-Prestador"].max()
                for idp in df_mais[df_mais["Qtd Atendimentos Cliente-Prestador"] == max_at]["ID Prestador"].astype(str):
                    if col > MAX_PROF_COLS:
                        break
                    _add(idp, "Mais atendeu o cliente", True)

        # 3) Último profissional
        if col <= MAX_PROF_COLS:
            df_hist = df_historico_60_dias[df_historico_60_dias["CPF_CNPJ"] == cpf].sort_values("Data 1", ascending=False)
            if not df_hist.empty:
                _add(str(df_hist["ID Prestador"].iloc[0]), "Último profissional que atendeu", True)

        # 4) Queridinhos (≤ 5 km)
        if col <= MAX_PROF_COLS:
            ids_q = []
            for _, qrow in df_queridinhos.iterrows():
                qid = str(qrow["ID Prestador"]).strip()
                if EVITAR_REPETIR_EM_LISTAS_NO_DIA and qid in profissionais_sugeridas_no_dia[data_atendimento]:
                    continue
                if qid in profissionais_ocupadas_no_dia[data_atendimento]:
                    continue
                d = _dist_from_df(cpf, qid, df_distancias)
                if d is not None and d <= RAIO_QUERIDINHOS:
                    ids_q.append((qid, d))
            for qid, _ in sorted(ids_q, key=lambda x: x[1]):
                if col > MAX_PROF_COLS:
                    break
                _add(qid, "Profissional preferencial da plataforma (até 5 km)", _qtd_cli(df_cliente_prestador, cpf, qid) > 0)

        # 5) Mais próximas geograficamente (delta km)
        if col <= MAX_PROF_COLS:
            dist_cand = df_distancias[df_distancias["CPF_CNPJ"] == cpf].copy()
            dist_cand["ID Prestador"] = dist_cand["ID Prestador"].astype(str).str.strip()

            def _ban(x):
                return (
                    (x in bloqueados) or
                    (x in utilizados) or
                    (x in profissionais_ocupadas_no_dia[data_atendimento]) or
                    _reservada_para_outro(data_atendimento, x, cpf) or
                    (EVITAR_REPETIR_EM_LISTAS_NO_DIA and x in profissionais_sugeridas_no_dia[data_atendimento]) or
                    (_prof_ok(x, df_profissionais) is None)
                )

            dist_cand = dist_cand[~dist_cand["ID Prestador"].apply(_ban)].sort_values("Distância (km)")
            ultimo_km = None
            for _, rowd in dist_cand.iterrows():
                if col > MAX_PROF_COLS:
                    break
                idp = rowd["ID Prestador"]
                dkm = float(rowd["Distância (km)"])
                if ultimo_km is None:
                    if _add(idp, "Mais próxima geograficamente", _qtd_cli(df_cliente_prestador, cpf, idp) > 0):
                        ultimo_km = dkm
                else:
                    if dkm >= (ultimo_km + DELTA_KM):
                        if _add(idp, "Mais próxima geograficamente", _qtd_cli(df_cliente_prestador, cpf, idp) > 0):
                            ultimo_km = dkm

        # 6) Sumidinhas
        if col <= MAX_PROF_COLS:
            for sid in df_sumidinhos["ID Prestador"].astype(str):
                if col > MAX_PROF_COLS:
                    break
                if EVITAR_REPETIR_EM_LISTAS_NO_DIA and sid in profissionais_sugeridas_no_dia[data_atendimento]:
                    continue
                if sid in profissionais_ocupadas_no_dia[data_atendimento]:
                    continue
                _add(sid, "Baixa Disponibilidade", _qtd_cli(df_cliente_prestador, cpf, sid) > 0)

        matriz_resultado_corrigida.append(linha)

    # ============================
    # DataFrame final + Excel
    # ============================
    df_matriz_rotas = pd.DataFrame(matriz_resultado_corrigida)

    # garante colunas até MAX_PROF_COLS
    for i in range(1, MAX_PROF_COLS + 1):
        for c in [
            f"Classificação da Profissional {i}",
            f"Critério {i}",
            f"Nome Prestador {i}",
            f"Celular {i}",
            f"Critério Utilizado {i}",
            f"Mensagem {i}",
        ]:
            if c not in df_matriz_rotas.columns:
                df_matriz_rotas[c] = pd.NA

    base_cols = [
        "OS", "CPF_CNPJ", "Nome Cliente", "Data 1", "Serviço", "Plano",
        "Duração do Serviço", "Hora de entrada", "Observações prestador",
        "Ponto de Referencia", "Mensagem Padrão"
    ]
    prestador_cols = []
    for i in range(1, MAX_PROF_COLS + 1):
        prestador_cols.extend([
            f"Classificação da Profissional {i}",
            f"Critério {i}",
            f"Nome Prestador {i}",
            f"Celular {i}",
            f"Critério Utilizado {i}",
            f"Mensagem {i}",
        ])

    df_matriz_rotas = df_matriz_rotas[base_cols + prestador_cols]

    final_path = os.path.join(output_dir, "rotas_bh_dados_tratados_completos.xlsx")
    with pd.ExcelWriter(final_path, engine='xlsxwriter') as writer:
        df_matriz_rotas.to_excel(writer, sheet_name="Rotas", index=False)
        df_atendimentos.to_excel(writer, sheet_name="Atendimentos", index=False)
        df_clientes.to_excel(writer, sheet_name="Clientes", index=False)
        df_profissionais.to_excel(writer, sheet_name="Profissionais", index=False)
        df_preferencias.to_excel(writer, sheet_name="Preferencias", index=False)
        df_bloqueio.to_excel(writer, sheet_name="Bloqueio", index=False)
        df_queridinhos.to_excel(writer, sheet_name="Queridinhos", index=False)
        df_sumidinhos.to_excel(writer, sheet_name="Sumidinhos", index=False)
        df_historico_60_dias.to_excel(writer, sheet_name="Historico 60 dias", index=False)
        df_cliente_prestador.to_excel(writer, sheet_name="Cliente x Prestador", index=False)
        df_qtd_por_prestador.to_excel(writer, sheet_name="Qtd por Prestador", index=False)
        df_distancias.to_excel(writer, sheet_name="Distancias", index=False)
        df_preferencias_completo.to_excel(writer, sheet_name="Preferencias Geo", index=False)
        df_bloqueio_completo.to_excel(writer, sheet_name="Bloqueios Geo", index=False)
        df_atendimentos_futuros_validos.to_excel(writer, sheet_name="Atend Futuros OK", index=False)
        df_atendimentos_sem_localizacao.to_excel(writer, sheet_name="Atend Futuros Sem Loc", index=False)

    return final_path


# =========================
# PORTAL PÚBLICO (antes da senha global)
# =========================
if "admin_autenticado" not in st.session_state:
    st.session_state.admin_autenticado = False

CONFIG = load_config()

if not st.session_state.admin_autenticado:
    st.markdown("""
        <div style='display:flex;align-items:center;gap:16px'>
            <img src='https://i.imgur.com/gIhC0fC.png' height='48'>
            <span style='font-size:1.7em;font-weight:700;color:#18d96b;letter-spacing:1px;'>BELO HORIZONTE || PORTAL DE ATENDIMENTOS</span>
        </div>
        <p style='color:#666;font-size:1.08em;margin:8px 0 18px 0'>
            Consulte abaixo os atendimentos disponíveis!
        </p>
    """, unsafe_allow_html=True)

    # ---- BLOCO VISUALIZAÇÃO (PÚBLICO) ----
    if os.path.exists(PORTAL_EXCEL) and os.path.exists(PORTAL_OS_LIST):
        df_portal = pd.read_excel(PORTAL_EXCEL, sheet_name="Clientes")
        with open(PORTAL_OS_LIST, "r", encoding="utf-8") as f:
            os_list = json.load(f)

        df_portal = df_portal[~df_portal["OS"].isna()]
        df_portal["OS"] = df_portal["OS"].apply(normalize_os)
        os_list_norm = [normalize_os(x) for x in os_list]
        df_portal = df_portal[df_portal["OS"].isin(os_list_norm)]

        # ---- OCULTAR OS quando atingiu limite de SIM por OS ----
        df_aceites = load_aceites_df()
        if CONFIG["MAX_ACEITES_SIM_POR_OS"] > 0:
            # lista de OS que já atingiram limite
            sim = df_aceites[df_aceites["Aceitou"].astype(str).str.strip().str.lower() == "sim"]
            cont = sim.groupby("OS").size()
            os_bloq = cont[cont >= CONFIG["MAX_ACEITES_SIM_POR_OS"]].index.tolist()
            df_portal = df_portal[~df_portal["OS"].isin(os_bloq)]

        if df_portal.empty:
            st.info("Nenhum atendimento disponível.")
        else:
            st.write(f"Exibindo {len(df_portal)} atendimentos selecionados pelo administrador:")

            for _, row in df_portal.iterrows():
                servico = row.get("Serviço", "")
                bairro = row.get("Bairro", "")
                data_pt = formatar_data_portugues(row.get("Data 1", ""))
                hora_entrada = row.get("Hora de entrada", "")
                hora_servico = row.get("Horas de serviço", "")
                referencia = row.get("Ponto de Referencia", "")
                os_id = row.get("OS", "")
                os_id = normalize_os(os_id)

                st.markdown(f"""
                    <div style="
                        background: #fff;
                        border: 1.5px solid #eee;
                        border-radius: 18px;
                        padding: 18px 18px 12px 18px;
                        margin-bottom: 14px;
                        min-width: 260px;
                        max-width: 440px;
                        color: #00008B;
                        font-family: Arial, sans-serif;
                    ">
                        <div style="font-size:1.2em; font-weight:bold; color:#00008B; margin-bottom:2px;">
                            {servico}
                        </div>
                        <div style="font-size:1em; color:#00008B; margin-bottom:7px;">
                            <b style="color:#00008B;margin-left:24px">Bairro:</b> <span>{bairro}</span>
                        </div>
                        <div style="font-size:0.95em; color:#00008B;">
                            <b>Data:</b> <span>{data_pt}</span><br>
                            <b>Hora de entrada:</b> <span>{hora_entrada}</span><br>
                            <b>Horas de serviço:</b> <span>{hora_servico}</span><br>
                            <b>Ponto de Referência:</b> <span>{referencia if referencia and str(referencia).lower() != 'nan' else '-'}</span>
                        </div>
                    </div>
                """, unsafe_allow_html=True)

                expander_style = """
                <style>
                div[role="button"][aria-expanded] {
                    background: #25D366 !important;
                    color: #fff !important;
                    border-radius: 10px !important;
                    font-weight: bold;
                    font-size: 1.08em;
                }
                </style>
                """
                st.markdown(expander_style, unsafe_allow_html=True)

                with st.expander("Tem disponibilidade? Clique aqui para aceitar este atendimento!"):
                    profissional = st.text_input("Nome da Profissional (OBRIGATÓRIO)", key=f"prof_nome_{os_id}")
                    telefone = st.text_input("Telefone para contato (OBRIGATÓRIO)", key=f"prof_tel_{os_id}")
                    resposta = st.empty()

                    _ok = bool((profissional or "").strip()) and bool(normalize_phone_br(telefone))

                    if st.button("Sim, tenho interesse neste atendimento.", key=f"btn_real_{os_id}", use_container_width=True, disabled=not _ok):
                        try:
                            salvar_aceite(os_id, profissional, telefone, True, origem="portal")
                        except ValueError as e:
                            resposta.error(f"❌ {e}")
                        else:
                            resposta.success("✅ Obrigado! Seu interesse foi registrado com sucesso. Em breve daremos retorno!")

    else:
        st.info("Nenhum atendimento disponível. Aguarde liberação do admin.")

    # ---- SENHA GLOBAL (não mexi na estrutura)
    senha = st.text_input("Área restrita. Digite a senha para liberar as demais abas:", type="password")
    if st.button("Entrar", key="btn_senha_global"):
        if senha == "vvv":
            st.session_state.admin_autenticado = True
            st.rerun()
        else:
            st.error("Senha incorreta. Acesso restrito.")

    st.stop()


# =========================
# ABAS (protegidas pela senha global)
# =========================
tabs = st.tabs([
    "Portal Atendimentos",
    "Upload de Arquivo",
    "Matriz de Rotas",
    "Aceites",
    "Profissionais Próximos",
    "Mensagem Rápida"
])


# =========================
# tabs[0] Portal (admin do portal aqui dentro - não mexi na estrutura)
# =========================
with tabs[0]:
    st.markdown("""
        <div style='display:flex;align-items:center;gap:16px'>
            <img src='https://i.imgur.com/gIhC0fC.png' height='48'>
            <span style='font-size:1.7em;font-weight:700;color:#18d96b;letter-spacing:1px;'>BELO HORIZONTE || PORTAL DE ATENDIMENTOS</span>
        </div>
        <p style='color:#666;font-size:1.08em;margin:8px 0 18px 0'>
            Consulte abaixo os atendimentos disponíveis!
        </p>
        """, unsafe_allow_html=True)

    # Controle de exibição e autenticação admin portal
    if "exibir_admin_portal" not in st.session_state:
        st.session_state.exibir_admin_portal = False
    if "admin_autenticado_portal" not in st.session_state:
        st.session_state.admin_autenticado_portal = False

    if st.button("Acesso admin para editar atendimentos do portal"):
        st.session_state.exibir_admin_portal = True

    if st.session_state.exibir_admin_portal:
        senha_portal = st.text_input("Digite a senha de administrador", type="password", key="senha_portal_admin")
        if st.button("Validar senha", key="btn_validar_senha_portal"):
            if senha_portal == "vvv":
                st.session_state.admin_autenticado_portal = True
            else:
                st.error("Senha incorreta.")

    if st.session_state.admin_autenticado_portal:
        if "portal_file_buffer" not in st.session_state:
            st.session_state.portal_file_buffer = None

        uploaded_file = st.file_uploader("Faça upload do arquivo Excel (aba Clientes)", type=["xlsx"], key="portal_upload")

        if uploaded_file:
            st.session_state.portal_file_buffer = uploaded_file.getbuffer()
            with open(PORTAL_EXCEL, "wb") as f:
                f.write(st.session_state.portal_file_buffer)
            st.success("Arquivo salvo! Escolha agora os atendimentos visíveis.")
            df_admin = pd.read_excel(PORTAL_EXCEL, sheet_name="Clientes")
        elif st.session_state.portal_file_buffer:
            with open(PORTAL_EXCEL, "wb") as f:
                f.write(st.session_state.portal_file_buffer)
            df_admin = pd.read_excel(PORTAL_EXCEL, sheet_name="Clientes")
        elif os.path.exists(PORTAL_EXCEL):
            df_admin = pd.read_excel(PORTAL_EXCEL, sheet_name="Clientes")
        else:
            df_admin = None

        if df_admin is not None and "Data 1" in df_admin.columns:
            datas_disponiveis = sorted(pd.to_datetime(df_admin["Data 1"], errors="coerce").dropna().dt.date.unique())
            datas_formatadas = [str(d) for d in datas_disponiveis]

            datas_selecionadas = st.multiselect(
                "Filtrar atendimentos por Data",
                options=datas_formatadas,
                default=[],
                key="datas_multiselect"
            )
            if datas_selecionadas:
                dts = pd.to_datetime(df_admin["Data 1"], errors="coerce").dt.date.astype(str)
                df_admin = df_admin[dts.isin(datas_selecionadas)]

            # opções
            opcoes = []
            for _, r in df_admin.iterrows():
                osv = normalize_os(r.get("OS", ""))
                if not osv:
                    continue
                opcoes.append(
                    f'OS {osv} | {r.get("Cliente","")} | {r.get("Serviço","")} | {r.get("Bairro","")}'
                )

            selecionadas = st.multiselect(
                "Selecione os atendimentos para exibir (OS | Cliente | Serviço | Bairro)",
                opcoes,
                key="os_multiselect"
            )

            if st.button("Salvar atendimentos exibidos", key="salvar_os_btn"):
                os_ids = []
                for op in selecionadas:
                    if op.startswith("OS "):
                        try:
                            os_ids.append(normalize_os(op.split()[1]))
                        except Exception:
                            pass
                with open(PORTAL_OS_LIST, "w", encoding="utf-8") as f:
                    json.dump(os_ids, f, ensure_ascii=False, indent=2)

                st.success("Seleção salva! Agora os atendimentos ficam disponíveis a todos.")
                st.session_state.exibir_admin_portal = False
                st.session_state.admin_autenticado_portal = False
                st.rerun()

    st.divider()

    # Visualização do portal (autenticado)
    CONFIG = load_config()
    if os.path.exists(PORTAL_EXCEL) and os.path.exists(PORTAL_OS_LIST):
        df_portal = pd.read_excel(PORTAL_EXCEL, sheet_name="Clientes")
        with open(PORTAL_OS_LIST, "r", encoding="utf-8") as f:
            os_list = json.load(f)

        df_portal = df_portal[~df_portal["OS"].isna()]
        df_portal["OS"] = df_portal["OS"].apply(normalize_os)
        os_list_norm = [normalize_os(x) for x in os_list]
        df_portal = df_portal[df_portal["OS"].isin(os_list_norm)]

        df_aceites = load_aceites_df()
        if CONFIG["MAX_ACEITES_SIM_POR_OS"] > 0:
            sim = df_aceites[df_aceites["Aceitou"].astype(str).str.strip().str.lower() == "sim"]
            cont = sim.groupby("OS").size()
            os_bloq = cont[cont >= CONFIG["MAX_ACEITES_SIM_POR_OS"]].index.tolist()
            df_portal = df_portal[~df_portal["OS"].isin(os_bloq)]

        if df_portal.empty:
            st.info("Nenhum atendimento disponível.")
        else:
            st.write(f"Exibindo {len(df_portal)} atendimentos selecionados pelo administrador:")
            for _, row in df_portal.iterrows():
                servico = row.get("Serviço", "")
                bairro = row.get("Bairro", "")
                data_pt = formatar_data_portugues(row.get("Data 1", ""))
                hora_entrada = row.get("Hora de entrada", "")
                hora_servico = row.get("Horas de serviço", "")
                referencia = row.get("Ponto de Referencia", "")
                os_id = normalize_os(row.get("OS", ""))

                st.markdown(f"""
                    <div style="
                        background: #fff;
                        border: 1.5px solid #eee;
                        border-radius: 18px;
                        padding: 18px 18px 12px 18px;
                        margin-bottom: 14px;
                        min-width: 260px;
                        max-width: 440px;
                        color: #00008B;
                        font-family: Arial, sans-serif;
                    ">
                        <div style="font-size:1.2em; font-weight:bold; color:#00008B; margin-bottom:2px;">
                            {servico}
                        </div>
                        <div style="font-size:1em; color:#00008B; margin-bottom:7px;">
                            <b style="color:#00008B;margin-left:24px">Bairro:</b> <span>{bairro}</span>
                        </div>
                        <div style="font-size:0.95em; color:#00008B;">
                            <b>Data:</b> <span>{data_pt}</span><br>
                            <b>Hora de entrada:</b> <span>{hora_entrada}</span><br>
                            <b>Horas de serviço:</b> <span>{hora_servico}</span><br>
                            <b>Ponto de Referência:</b> <span>{referencia if referencia and str(referencia).lower() != 'nan' else '-'}</span>
                        </div>
                    </div>
                """, unsafe_allow_html=True)

                with st.expander("Tem disponibilidade? Clique aqui para aceitar este atendimento!"):
                    profissional = st.text_input("Nome da Profissional (OBRIGATÓRIO)", key=f"prof_nome_auth_{os_id}")
                    telefone = st.text_input("Telefone para contato (OBRIGATÓRIO)", key=f"prof_tel_auth_{os_id}")
                    resposta = st.empty()

                    _ok = bool((profissional or "").strip()) and bool(normalize_phone_br(telefone))

                    if st.button("Sim, tenho interesse neste atendimento.", key=f"btn_real_auth_{os_id}", use_container_width=True, disabled=not _ok):
                        try:
                            salvar_aceite(os_id, profissional, telefone, True, origem="portal")
                        except ValueError as e:
                            resposta.error(f"❌ {e}")
                        else:
                            resposta.success("✅ Obrigado! Seu interesse foi registrado com sucesso. Em breve daremos retorno!")
    else:
        st.info("Nenhum atendimento disponível. Aguarde liberação do admin.")


# =========================
# tabs[1] Upload (PROTEGIDA) + CONFIGURAÇÕES (PROTEGIDA)
# =========================
with tabs[1]:
    st.markdown("## Upload e Processamento (Protegido)")
    CONFIG = load_config()

    st.markdown("### Configurações (somente admin)")
    colcfg1, colcfg2, colcfg3, colcfg4 = st.columns(4)

    max_prof_cols_ui = colcfg1.number_input(
        "MAX_PROF_COLS (profissionais por OS)",
        min_value=1, max_value=30, step=1,
        value=int(CONFIG["MAX_PROF_COLS"]),
        help="Mude quando quiser. Para refletir no Excel, rode o processamento novamente."
    )

    max_sim_por_os_ui = colcfg2.number_input(
        "MAX SIM por OS (ocultar no portal)",
        min_value=0, max_value=50, step=1,
        value=int(CONFIG["MAX_ACEITES_SIM_POR_OS"])
    )

    max_sim_tel_total_ui = colcfg3.number_input(
        "MAX SIM por telefone (total)",
        min_value=0, max_value=50, step=1,
        value=int(CONFIG["MAX_ACEITES_SIM_POR_TEL_TOTAL"])
    )

    max_tel_por_os_ui = colcfg4.number_input(
        "MAX respostas por tel na mesma OS",
        min_value=0, max_value=50, step=1,
        value=int(CONFIG["MAX_ACEITES_POR_TEL_POR_OS"])
    )

    if st.button("💾 Salvar configurações"):
        new_cfg = {
            "MAX_PROF_COLS": int(max_prof_cols_ui),
            "MAX_ACEITES_SIM_POR_OS": int(max_sim_por_os_ui),
            "MAX_ACEITES_SIM_POR_TEL_TOTAL": int(max_sim_tel_total_ui),
            "MAX_ACEITES_POR_TEL_POR_OS": int(max_tel_por_os_ui),
        }
        save_config(new_cfg)
        st.success("Configurações salvas! Para aplicar no Excel, faça o upload e rode o processamento novamente.")
        CONFIG = load_config()

    st.divider()

    if "excel_processado" not in st.session_state:
        st.session_state.excel_processado = False
    if "nome_arquivo_processado" not in st.session_state:
        st.session_state.nome_arquivo_processado = None

    uploaded_file = st.file_uploader("Selecione o arquivo Excel original", type=["xlsx"])

    if uploaded_file is not None:
        if (not st.session_state.excel_processado) or (st.session_state.nome_arquivo_processado != uploaded_file.name):
            with st.spinner("Processando... Isso pode levar alguns segundos."):
                with tempfile.TemporaryDirectory() as tempdir:
                    temp_path = os.path.join(tempdir, uploaded_file.name)
                    with open(temp_path, "wb") as f:
                        f.write(uploaded_file.read())

                    try:
                        CONFIG = load_config()
                        excel_path = pipeline(temp_path, tempdir, CONFIG["MAX_PROF_COLS"])

                        if os.path.exists(excel_path):
                            st.success("Processamento finalizado com sucesso!")
                            st.download_button(
                                label="📥 Baixar Excel consolidado",
                                data=open(excel_path, "rb").read(),
                                file_name="rotas_bh_dados_tratados_completos.xlsx",
                                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                                key="download_excel_consolidado"
                            )
                            import shutil
                            shutil.copy(excel_path, ROTAS_FILE)

                            st.session_state.excel_processado = True
                            st.session_state.nome_arquivo_processado = uploaded_file.name
                        else:
                            st.error("Arquivo final não encontrado. Ocorreu um erro no pipeline.")
                    except Exception as e:
                        st.error(f"Erro no processamento: {e}")
        else:
            if os.path.exists(ROTAS_FILE):
                st.download_button(
                    label="📥 Baixar Excel consolidado",
                    data=open(ROTAS_FILE, "rb").read(),
                    file_name="rotas_bh_dados_tratados_completos.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    key="download_excel_consolidado_reuse"
                )
    else:
        st.session_state.excel_processado = False
        st.session_state.nome_arquivo_processado = None


# =========================
# tabs[2] Matriz de Rotas (robusta para qualquer MAX_PROF_COLS)
# =========================
with tabs[2]:
    if os.path.exists(ROTAS_FILE):
        df_rotas = pd.read_excel(ROTAS_FILE, sheet_name="Rotas")
        if "Data 1" in df_rotas.columns:
            df_rotas["Data 1"] = pd.to_datetime(df_rotas["Data 1"], errors="coerce")

        datas = sorted(df_rotas["Data 1"].dropna().dt.date.unique()) if "Data 1" in df_rotas.columns else []
        data_sel = st.selectbox("Filtrar por data", options=["Todos"] + [str(d) for d in datas], key="data_rotas")

        clientes = df_rotas["Nome Cliente"].dropna().unique() if "Nome Cliente" in df_rotas.columns else []
        cliente_sel = st.selectbox("Filtrar por cliente", options=["Todos"] + list(clientes), key="cliente_rotas")

        # pega todas colunas Nome Prestador X que existirem no arquivo
        prof_cols = [c for c in df_rotas.columns if str(c).startswith("Nome Prestador ")]
        profissionais = []
        for c in prof_cols:
            profissionais.extend(df_rotas[c].dropna().astype(str).tolist())
        profissionais = sorted(list(set([p for p in profissionais if p and p.lower() != "nan"])))

        profissional_sel = st.selectbox("Filtrar por profissional", options=["Todos"] + profissionais, key="prof_rotas")

        df_rotas_filt = df_rotas.copy()
        if data_sel != "Todos" and "Data 1" in df_rotas_filt.columns:
            df_rotas_filt = df_rotas_filt[df_rotas_filt["Data 1"].dt.date.astype(str) == data_sel]
        if cliente_sel != "Todos" and "Nome Cliente" in df_rotas_filt.columns:
            df_rotas_filt = df_rotas_filt[df_rotas_filt["Nome Cliente"] == cliente_sel]
        if profissional_sel != "Todos" and prof_cols:
            mask = False
            for c in prof_cols:
                mask |= (df_rotas_filt[c].astype(str) == profissional_sel)
            df_rotas_filt = df_rotas_filt[mask]

        st.dataframe(df_rotas_filt, use_container_width=True)
        st.download_button(
            label="📥 Baixar Excel consolidado",
            data=open(ROTAS_FILE, "rb").read(),
            file_name="rotas_bh_dados_tratados_completos.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )
    else:
        st.info("Faça o upload e aguarde o processamento para liberar a matriz de rotas.")


# =========================
# tabs[3] Aceites
# =========================
with tabs[3]:
    if os.path.exists(ACEITES_FILE):
        df_aceites = load_aceites_df()
        st.dataframe(df_aceites, use_container_width=True)

        out = io.BytesIO()
        df_aceites.to_excel(out, index=False)
        st.download_button(
            label="📥 Baixar histórico de aceites",
            data=out.getvalue(),
            file_name="aceites.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )
    else:
        st.info("Nenhum aceite registrado ainda.")


# =========================
# tabs[4] Profissionais Próximos
# =========================
with tabs[4]:
    st.subheader("Buscar Profissionais Próximos")
    lat = st.number_input("Latitude", value=-19.900000, format="%.6f")
    lon = st.number_input("Longitude", value=-43.900000, format="%.6f")
    n = st.number_input("Qtd. profissionais", min_value=1, value=5, step=1)

    if st.button("Buscar"):
        if os.path.exists(ROTAS_FILE):
            df_prof = pd.read_excel(ROTAS_FILE, sheet_name="Profissionais")
            df_prof = df_prof[~df_prof['Nome Prestador'].astype(str).str.contains('inativo', case=False, na=False)]
            df_prof = df_prof.dropna(subset=['Latitude Profissional', 'Longitude Profissional'])

            input_coords = (lat, lon)
            df_prof['Distância_km'] = df_prof.apply(
                lambda r: geodesic(input_coords, (r['Latitude Profissional'], r['Longitude Profissional'])).km,
                axis=1
            )
            df_melhores = df_prof.sort_values('Distância_km').head(int(n))
            st.dataframe(df_melhores[['Nome Prestador', 'Celular', 'Qtd Atendimentos', 'Latitude Profissional', 'Longitude Profissional', 'Distância_km']], use_container_width=True)
        else:
            st.info("Faça upload e processamento do arquivo para habilitar a busca.")


# =========================
# tabs[5] Mensagem Rápida
# =========================
with tabs[5]:
    st.subheader("Gerar Mensagem Rápida WhatsApp")
    os_id = st.text_input("Código da OS* (obrigatório)", max_chars=12)
    data = st.text_input("Data do Atendimento (ex: 20/06/2025)")
    bairro = st.text_input("Bairro")
    servico = st.text_input("Serviço")
    hora_entrada = st.text_input("Hora de entrada (ex: 08:00)")
    duracao = st.text_input("Duração do atendimento (ex: 2h)")

    app_url = "https://rotasvavive.streamlit.app"
    link_aceite = f"{app_url}?aceite={os_id}&origem=mensagem_rapida" if os_id.strip() else ""

    if st.button("Gerar Mensagem"):
        if not os_id.strip():
            st.error("Preencha o código da OS!")
        else:
            mensagem = (
                "🚨🚨🚨\n"
                "     *Oportunidade Relâmpago*\n"
                "                              🚨🚨🚨\n\n"
                f"Olá, tudo bem com você?\n\n"
                f"*Data:* {data}\n"
                f"*Bairro:* {bairro}\n"
                f"*Serviço:* {servico}\n"
                f"*Hora de entrada:* {hora_entrada}\n"
                f"*Duração do atendimento:* {duracao}\n\n"
                f"👉 Para aceitar ou recusar, acesse: {link_aceite}\n\n"
                "Se tiver interesse, por favor, nos avise!"
            )
            st.text_area("Mensagem WhatsApp", value=mensagem, height=260)
