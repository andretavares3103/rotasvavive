# -*- coding: utf-8 -*-
import streamlit as st
import pandas as pd
import numpy as np
import os
import json
import locale
import tempfile
from datetime import datetime, timedelta
from geopy.distance import geodesic

# (opcional) e-mail
import smtplib
from email.mime.text import MIMEText

# =========================================================
# CONFIG / ARQUIVOS
# =========================================================
st.set_page_config(page_title="BELO HORIZONTE || Otimização Rotas Vavivê", layout="wide")

PORTAL_EXCEL = "portal_atendimentos_clientes.xlsx"
PORTAL_OS_LIST = "portal_atendimentos_os_list.json"
ACEITES_FILE = "aceites.xlsx"
ROTAS_FILE = "rotas_bh_dados_tratados_completos.xlsx"

ADMIN_PASSWORD = "vvv"

# Persistência das configurações (sobrevive a restart do Streamlit Cloud)
APP_CFG_FILE = "app_config.json"
DEFAULT_CFG = {
    "max_prof_cols": 4,                 # quantas profissionais sugeridas por OS (1..30)
    "max_os_por_telefone": 1,           # (portal) qtas OS o mesmo telefone pode aceitar no total
    "max_aceites_tel_por_os": 1,        # qtas vezes o mesmo telefone pode aceitar a MESMA OS
    "max_sim_por_os": 1                 # qtos "Sim" por OS antes de ocultar no portal
}

# Locale (pode falhar em cloud, tudo bem)
try:
    locale.setlocale(locale.LC_TIME, "pt_BR.UTF-8")
except Exception:
    pass


# =========================================================
# CONFIG (GET/SET) + NORMALIZAÇÕES
# =========================================================
def load_cfg():
    if "cfg" in st.session_state:
        return st.session_state.cfg
    cfg = dict(DEFAULT_CFG)
    if os.path.exists(APP_CFG_FILE):
        try:
            with open(APP_CFG_FILE, "r", encoding="utf-8") as f:
                disk = json.load(f)
            if isinstance(disk, dict):
                cfg.update(disk)
        except Exception:
            pass
    # saneamento
    cfg["max_prof_cols"] = int(max(1, min(30, int(cfg.get("max_prof_cols", 4)))))
    cfg["max_os_por_telefone"] = int(max(1, min(50, int(cfg.get("max_os_por_telefone", 1)))))
    cfg["max_aceites_tel_por_os"] = int(max(1, min(50, int(cfg.get("max_aceites_tel_por_os", 1)))))
    cfg["max_sim_por_os"] = int(max(1, min(50, int(cfg.get("max_sim_por_os", 1)))))
    st.session_state.cfg = cfg
    return cfg


def save_cfg(cfg: dict):
    # saneamento
    cfg = dict(cfg)
    cfg["max_prof_cols"] = int(max(1, min(30, int(cfg.get("max_prof_cols", 4)))))
    cfg["max_os_por_telefone"] = int(max(1, min(50, int(cfg.get("max_os_por_telefone", 1)))))
    cfg["max_aceites_tel_por_os"] = int(max(1, min(50, int(cfg.get("max_aceites_tel_por_os", 1)))))
    cfg["max_sim_por_os"] = int(max(1, min(50, int(cfg.get("max_sim_por_os", 1)))))
    try:
        with open(APP_CFG_FILE, "w", encoding="utf-8") as f:
            json.dump(cfg, f, ensure_ascii=False, indent=2)
    except Exception:
        pass
    st.session_state.cfg = cfg


def norm_tel(tel: str) -> str:
    tel = str(tel or "").strip()
    digits = "".join([c for c in tel if c.isdigit()])
    # mantém último 11 (ex.: DDD+9 dígitos), ajuda contra variações
    if len(digits) > 11:
        digits = digits[-11:]
    return digits


def padronizar_os_series(col: pd.Series) -> pd.Series:
    def safe_os(x):
        try:
            return str(int(float(x))).strip()
        except Exception:
            return ""
    return col.apply(safe_os).astype(str)


def ensure_aceites_file():
    if not os.path.exists(ACEITES_FILE):
        df = pd.DataFrame(columns=[
            "OS", "Profissional", "Telefone", "Aceitou",
            "Data do Aceite", "Dia da Semana", "Horário do Aceite", "Origem"
        ])
        df.to_excel(ACEITES_FILE, index=False)


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
        if len(s) >= 10 and s[4] == "-" and s[7] == "-":
            ano = s[0:4]; mes = s[5:7]; dia = s[8:10]
            dt = pd.Timestamp(year=int(ano), month=int(mes), day=int(dia))
        else:
            dt = pd.to_datetime(data, dayfirst=True, errors="coerce")
        if pd.isnull(dt):
            return str(data)
        dia_semana_en = dt.strftime("%A")
        dia_semana_pt = dias_pt.get(dia_semana_en, dia_semana_en)
        return f"{dia_semana_pt}, {dt.strftime('%d/%m/%Y')}"
    except Exception:
        return str(data)


# =========================================================
# E-MAIL (OPCIONAL) — SEM SENHA NO CÓDIGO
# =========================================================
def enviar_email_aceite_gmail(os_id, profissional, telefone):
    """
    Configure em st.secrets:
      GMAIL_FROM
      GMAIL_APP_PASSWORD
      GMAIL_TO
    Se não existir, a função só ignora.
    """
    try:
        remetente = st.secrets.get("GMAIL_FROM", "")
        senha = st.secrets.get("GMAIL_APP_PASSWORD", "")
        destinatario = st.secrets.get("GMAIL_TO", "")
    except Exception:
        remetente = senha = destinatario = ""

    if not (remetente and senha and destinatario):
        return  # sem configuração, não envia

    assunto = f"Novo aceite registrado | OS {os_id}"
    corpo = f"""Um novo aceite foi registrado:

OS: {os_id}
Profissional: {profissional}
Telefone: {telefone}
Data/Hora: {datetime.now().strftime('%d/%m/%Y %H:%M:%S')}
"""

    msg = MIMEText(corpo)
    msg["Subject"] = assunto
    msg["From"] = remetente
    msg["To"] = destinatario

    try:
        with smtplib.SMTP_SSL("smtp.gmail.com", 465) as smtp:
            smtp.login(remetente, senha)
            smtp.sendmail(remetente, destinatario, msg.as_string())
    except Exception:
        pass


# =========================================================
# ACEITES (COM REGRAS 100% PRONTAS)
# =========================================================
def salvar_aceite(os_id, profissional, telefone, aceitou, origem=None):
    cfg = load_cfg()

    profissional = (profissional or "").strip()
    telefone_raw = (telefone or "").strip()
    telefone_n = norm_tel(telefone_raw)

    if not profissional:
        raise ValueError("Nome da Profissional é obrigatório.")
    if not telefone_n:
        raise ValueError("Telefone é obrigatório (com DDD).")

    ensure_aceites_file()
    df = pd.read_excel(ACEITES_FILE)

    # normaliza
    df["OS"] = padronizar_os_series(df["OS"]) if "OS" in df.columns else ""
    if "Telefone" in df.columns:
        df["Telefone_norm"] = df["Telefone"].apply(norm_tel)
    else:
        df["Telefone_norm"] = ""

    os_id_str = str(int(float(os_id))) if str(os_id).strip() != "" else str(os_id)
    os_id_str = str(os_id_str).strip()

    # aplica regras só pra "Sim"
    if bool(aceitou):
        # (A) Limite global: quantas OS o telefone pode aceitar no portal
        max_os_tel = int(cfg["max_os_por_telefone"])
        aceites_tel_sim = df[
            (df["Telefone_norm"] == telefone_n) &
            (df["Aceitou"].astype(str).str.strip().str.lower() == "sim")
        ]
        # conta OS distintas aceitas por esse telefone
        os_distintas = aceites_tel_sim["OS"].astype(str).str.strip().unique().tolist()
        if os_id_str not in os_distintas and len(os_distintas) >= max_os_tel:
            raise ValueError(f"Este telefone já atingiu o limite de {max_os_tel} aceite(s) no portal.")

        # (B) Limite por OS: quantas vezes o mesmo telefone pode aceitar a mesma OS
        max_tel_os = int(cfg["max_aceites_tel_por_os"])
        aceites_tel_os_sim = df[
            (df["Telefone_norm"] == telefone_n) &
            (df["OS"].astype(str).str.strip() == os_id_str) &
            (df["Aceitou"].astype(str).str.strip().str.lower() == "sim")
        ]
        if len(aceites_tel_os_sim) >= max_tel_os:
            raise ValueError(f"Este telefone já aceitou esta OS o máximo permitido ({max_tel_os}).")

        # (C) Limite total de "Sim" por OS (para evitar overbooking)
        max_sim_os = int(cfg["max_sim_por_os"])
        aceites_os_sim = df[
            (df["OS"].astype(str).str.strip() == os_id_str) &
            (df["Aceitou"].astype(str).str.strip().str.lower() == "sim")
        ]
        if len(aceites_os_sim) >= max_sim_os:
            raise ValueError(f"Esta OS já atingiu {max_sim_os} aceite(s) 'Sim'.")

    agora = pd.Timestamp.now()
    nova_linha = {
        "OS": os_id_str,
        "Profissional": profissional,
        "Telefone": telefone_n,  # grava normalizado
        "Aceitou": "Sim" if bool(aceitou) else "Não",
        "Data do Aceite": agora.strftime("%d/%m/%Y"),
        "Dia da Semana": agora.strftime("%A"),
        "Horário do Aceite": agora.strftime("%H:%M:%S"),
        "Origem": origem if origem else ""
    }
    df = pd.concat([df.drop(columns=["Telefone_norm"], errors="ignore"), pd.DataFrame([nova_linha])], ignore_index=True)
    df.to_excel(ACEITES_FILE, index=False)

    # opcional e-mail
    enviar_email_aceite_gmail(os_id_str, profissional, telefone_n)


# =========================================================
# FORMULÁRIO DE ACEITE VIA LINK (?aceite=OS&origem=...)
# =========================================================
def exibe_formulario_aceite(os_id, origem=None):
    st.header(f"Validação de Aceite (OS {os_id})")
    profissional = st.text_input("Nome da Profissional (OBRIGATÓRIO)")
    telefone = st.text_input("Telefone para contato (OBRIGATÓRIO)")
    resposta = st.empty()

    ok = bool((profissional or "").strip()) and bool((telefone or "").strip())

    col1, col2 = st.columns(2)
    with col1:
        if st.button("Sim, aceito este atendimento", disabled=not ok):
            try:
                salvar_aceite(os_id, profissional, telefone, True, origem=origem)
            except ValueError as e:
                resposta.error(f"❌ {e}")
            else:
                resposta.success("✅ Obrigado! Seu aceite foi registrado com sucesso.")
                st.stop()
    with col2:
        if st.button("Não posso aceitar", disabled=not ok):
            try:
                salvar_aceite(os_id, profissional, telefone, False, origem=origem)
            except ValueError as e:
                resposta.error(f"❌ {e}")
            else:
                resposta.success("✅ Obrigado! Fique de olho em novas oportunidades.")
                st.stop()


# =========================================================
# MENSAGENS / PIPELINE (SEU CÓDIGO — COM MAX_PROF_COLS DINÂMICO)
# =========================================================
def traduzir_dia_semana(date_obj):
    dias_pt = {
        "Monday": "segunda-feira", "Tuesday": "terça-feira", "Wednesday": "quarta-feira",
        "Thursday": "quinta-feira", "Friday": "sexta-feira", "Saturday": "sábado", "Sunday": "domingo"
    }
    return dias_pt.get(date_obj.strftime("%A"), date_obj.strftime("%A"))


def formatar_nome_simples(nome):
    nome = str(nome or "").strip()
    nome = nome.replace("CI ", "").replace("Ci ", "").replace("C i ", "").replace("C I ", "")
    partes = nome.split()
    if partes and partes[0].lower() in ["ana", "maria"] and len(partes) > 1:
        return " ".join(partes[:2])
    elif partes:
        return partes[0]
    return nome


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
    if pd.notnull(latitude) and pd.notnull(longitude):
        maps_url = f"https://maps.google.com/?q={latitude},{longitude}"
    else:
        maps_url = ""
    fechamento = (
        "SIM ou NÃO para o aceite!" if ja_atendeu
        else "Acesse o link ao final da mensagem e responda com SIM caso tenha disponibilidade!"
    )
    rodape = """
O atendimento será confirmado após o aceite!
*1)*    Lembre que o cliente irá receber o *profissional indicado pela Vavivê*.
*2)*    Lembre-se das nossas  confirmações do atendimento!

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
        .str.replace(r"\D", "", regex=True)
        .str.zfill(14)
        .str.strip()
    )


def salvar_df(df, nome_arquivo, output_dir):
    caminho = os.path.join(output_dir, f"{nome_arquivo}.xlsx")
    df.to_excel(caminho, index=False)


def pipeline(file_path, output_dir, MAX_PROF_COLS):
    import xlsxwriter
    from collections import defaultdict

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
    df_profissionais["cpf"] = (
        df_profissionais["cpf"].astype(str).str.replace(r"\D", "", regex=True).str.strip()
    )
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
    df_preferencias = df_preferencias[[
        "CPF_CNPJ","Nome Cliente","ID Prestador","Nome Prestador"
    ]]
    salvar_df(df_preferencias, "df_preferencias", output_dir)

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
    df_bloqueio = df_bloqueio[[
        "CPF_CNPJ","Nome Cliente","ID Prestador","Nome Prestador"
    ]]
    salvar_df(df_bloqueio, "df_bloqueio", output_dir)

    df_queridinhos_raw = pd.read_excel(file_path, sheet_name="Profissionais Preferenciais")
    df_queridinhos = df_queridinhos_raw[[
        "ID Profissional","Profissional"
    ]].copy()
    df_queridinhos["ID Prestador"] = (
        df_queridinhos["ID Profissional"].fillna("0").astype(str)
        .str.replace(r"\.0$", "", regex=True).str.strip()
    )
    df_queridinhos["Nome Prestador"] = df_queridinhos["Profissional"].astype(str).str.strip()
    df_queridinhos = df_queridinhos[["ID Prestador","Nome Prestador"]]
    salvar_df(df_queridinhos, "df_queridinhos", output_dir)

    df_sumidinhos_raw = pd.read_excel(file_path, sheet_name="Baixa Disponibilidade")
    df_sumidinhos = df_sumidinhos_raw[[
        "ID Profissional","Profissional"
    ]].copy()
    df_sumidinhos["ID Prestador"] = (
        df_sumidinhos["ID Profissional"].fillna("0").astype(str)
        .str.replace(r"\.0$", "", regex=True).str.strip()
    )
    df_sumidinhos["Nome Prestador"] = df_sumidinhos["Profissional"].astype(str).str.strip()
    df_sumidinhos = df_sumidinhos[["ID Prestador","Nome Prestador"]]
    salvar_df(df_sumidinhos, "df_sumidinhos", output_dir)

    df_atendimentos = pd.read_excel(file_path, sheet_name="Atendimentos")
    colunas_desejadas = [
        "OS","Status Serviço","Data 1",
        "Plano","CPF/ CNPJ","Cliente","Serviço",
        "Horas de serviço","Hora de entrada","Observações atendimento",
        "Observações prestador","Ponto de Referencia","#Num Prestador","Prestador"
    ]
    # (proteção) algumas bases vêm com "CPF/ CNPJ" como "CPF/ CNPJ" mesmo:
    colunas_desejadas = [c.replace("Data 1\n        ", "Data 1") for c in colunas_desejadas]
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

    df_cliente_prestador = df_historico_60_dias.groupby(
        ["CPF_CNPJ","ID Prestador"]
    ).size().reset_index(name="Qtd Atendimentos Cliente-Prestador")
    salvar_df(df_cliente_prestador, "df_cliente_prestador", output_dir)

    df_qtd_por_prestador = df_historico_60_dias.groupby(
        "ID Prestador"
    ).size().reset_index(name="Qtd Atendimentos Prestador")
    salvar_df(df_qtd_por_prestador, "df_qtd_por_prestador", output_dir)

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
    df_distancias_alerta = df_distancias[df_distancias["Distância (km)"] > 1000]
    salvar_df(df_distancias_alerta, "df_distancias_alerta", output_dir)
    salvar_df(df_distancias, "df_distancias", output_dir)

    df_preferencias_completo = df_preferencias.merge(
        df_clientes_coord, on="CPF_CNPJ", how="left"
    ).merge(
        df_profissionais_coord, on="ID Prestador", how="left"
    )
    df_preferencias_completo = df_preferencias_completo[[
        "CPF_CNPJ","Nome Cliente","ID Prestador","Nome Prestador",
        "Latitude Cliente","Longitude Cliente",
        "Latitude Profissional","Longitude Profissional"
    ]]
    salvar_df(df_preferencias_completo, "df_preferencias_completo", output_dir)

    df_bloqueio_completo = df_bloqueio.merge(
        df_clientes_coord, on="CPF_CNPJ", how="left"
    ).merge(
        df_profissionais_coord, on="ID Prestador", how="left"
    )
    df_bloqueio_completo = df_bloqueio_completo[[
        "CPF_CNPJ","Nome Cliente","ID Prestador","Nome Prestador",
        "Latitude Cliente","Longitude Cliente",
        "Latitude Profissional","Longitude Profissional"
    ]]
    salvar_df(df_bloqueio_completo, "df_bloqueio_completo", output_dir)

    ontem = datetime.now().date() - timedelta(days=1)
    df_futuros = df_atendimentos[
        (df_atendimentos["Status Serviço"].astype(str).str.lower() != "cancelado") &
        (df_atendimentos["Data 1"].dt.date > ontem)
    ].copy()
    df_futuros_com_clientes = df_futuros.merge(
        df_clientes_coord, on="CPF_CNPJ", how="left"
    )
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

    # Persistências (opcional)
    df_clientes.to_pickle("df_clientes.pkl")
    df_profissionais.to_pickle("df_profissionais.pkl")
    df_preferencias.to_pickle("df_preferencias.pkl")
    df_bloqueio.to_pickle("df_bloqueio.pkl")
    df_queridinhos.to_pickle("df_queridinhos.pkl")
    df_sumidinhos.to_pickle("df_sumidinhos.pkl")
    df_atendimentos.to_pickle("df_atendimentos.pkl")
    df_historico_60_dias.to_pickle("df_historico_60_dias.pkl")
    df_cliente_prestador.to_pickle("df_cliente_prestador.pkl")
    df_qtd_por_prestador.to_pickle("df_qtd_por_prestador.pkl")
    df_distancias.to_pickle("df_distancias.pkl")
    df_preferencias_completo.to_pickle("df_preferencias_completo.pkl")
    df_bloqueio_completo.to_pickle("df_bloqueio_completo.pkl")
    df_atendimentos_futuros_validos.to_pickle("df_atendimentos_futuros_validos.pkl")
    df_atendimentos_sem_localizacao.to_pickle("df_atendimentos_sem_localizacao.pkl")
    df_distancias_alerta.to_pickle("df_distancias_alerta.pkl")

    # ============================
    # PARÂMETROS
    # ============================
    DELTA_KM = 1.0
    RAIO_QUERIDINHOS = 5.0
    GARANTIR_COTA_QUERIDINHO = True
    EVITAR_REPETIR_EM_LISTAS_NO_DIA = True

    # ----------------------------
    # Helpers
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

    from collections import defaultdict

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

    os_primeira_candidata = {}

    for data_atend, df_do_dia in df_atendimentos_futuros_validos.groupby(df_atendimentos_futuros_validos["Data 1"].dt.date):
        df_sorted = _ordena_os(df_do_dia)
        for _, row in df_sorted.iterrows():
            os_id = row["OS"]; cpf = row["CPF_CNPJ"]
            bloqueados = (
                df_bloqueio[df_bloqueio["CPF_CNPJ"] == cpf]["ID Prestador"]
                .astype(str).str.strip().tolist()
            )

            pref_id = preferida_do_cliente_no_dia[data_atend].get(cpf)
            if pref_id:
                if (pref_id not in bloqueados) and (pref_id not in profissionais_ocupadas_no_dia[data_atend]):
                    prof = _prof_ok(pref_id, df_profissionais)
                    if prof is not None:
                        crit = f"cliente: {_qtd_cli(df_cliente_prestador, cpf, pref_id)} | total: {_qtd_tot(df_qtd_por_prestador, pref_id)}"
                        d = _dist_from_df(cpf, pref_id, df_distancias)
                        if d is not None: crit += f" — {d:.2f} km"
                        os_primeira_candidata[(data_atend, os_id)] = (pref_id, crit, "Preferência do Cliente")
                        profissionais_ocupadas_no_dia[data_atend].add(pref_id)
                        profissionais_sugeridas_no_dia[data_atend].add(pref_id)
                        continue

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
                    if d is not None: crit += f" — {d:.2f} km"
                    os_primeira_candidata[(data_atend, os_id)] = (escolhido, crit, "Mais atendeu o cliente")
                    profissionais_ocupadas_no_dia[data_atend].add(escolhido)
                    profissionais_sugeridas_no_dia[data_atend].add(escolhido)
                    continue

            df_hist = df_historico_60_dias[df_historico_60_dias["CPF_CNPJ"] == cpf].sort_values("Data 1", ascending=False)
            if not df_hist.empty:
                ult_id = str(df_hist["ID Prestador"].iloc[0]).strip()
                if (ult_id not in bloqueados) and (ult_id not in profissionais_ocupadas_no_dia[data_atend]) and (_prof_ok(ult_id, df_profissionais) is not None):
                    crit = f"cliente: {_qtd_cli(df_cliente_prestador, cpf, ult_id)} | total: {_qtd_tot(df_qtd_por_prestador, ult_id)}"
                    d = _dist_from_df(cpf, ult_id, df_distancias)
                    if d is not None: crit += f" — {d:.2f} km"
                    os_primeira_candidata[(data_atend, os_id)] = (ult_id, crit, "Último profissional que atendeu")
                    profissionais_ocupadas_no_dia[data_atend].add(ult_id)
                    profissionais_sugeridas_no_dia[data_atend].add(ult_id)
                    continue

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

    auditoria_proximidade = []

    try:
        from scipy.optimize import linear_sum_assignment
        def hungarian_min_cost(cost_matrix):
            r, c = linear_sum_assignment(cost_matrix)
            return list(zip(r, c))
    except Exception:
        def hungarian_min_cost(cost):
            import math
            n = max(len(cost), len(cost[0]) if cost else 0)
            C = [[10**6 for _ in range(n)] for __ in range(n)]
            for i in range(len(cost)):
                for j in range(len(cost[0])):
                    C[i][j] = cost[i][j]
            u = [0]*(n+1); v = [0]*(n+1); p = [0]*(n+1); way = [0]*(n+1)
            for i in range(1, n+1):
                p[0] = i
                j0 = 0
                minv = [math.inf]*(n+1)
                used = [False]*(n+1)
                while True:
                    used[j0] = True
                    i0 = p[j0]; delta = math.inf; j1 = 0
                    for j in range(1, n+1):
                        if not used[j]:
                            cur = C[i0-1][j-1] - u[i0] - v[j]
                            if cur < minv[j]:
                                minv[j] = cur; way[j] = j0
                            if minv[j] < delta:
                                delta = minv[j]; j1 = j
                    for j in range(0, n+1):
                        if used[j]:
                            u[p[j]] += delta; v[j] -= delta
                        else:
                            minv[j] -= delta
                    j0 = j1
                    if p[j0] == 0:
                        break
                while True:
                    j1 = way[j0]; p[j0] = p[j1]; j0 = j1
                    if j0 == 0:
                        break
            ans = [(-1,-1)]*n
            for j in range(1, n+1):
                if p[j] != 0 and p[j]-1 < len(cost) and j-1 < len(cost[0]):
                    ans[p[j]-1] = (p[j]-1, j-1)
            return [(i,j) for (i,j) in ans if i != -1 and j != -1]

    PENAL = 10**6

    for data_atend, df_do_dia in df_atendimentos_futuros_validos.groupby(df_atendimentos_futuros_validos["Data 1"].dt.date):
        df_pend = df_do_dia[~df_do_dia["OS"].apply(lambda os_: (data_atend, os_) in os_primeira_candidata)].copy()
        if df_pend.empty:
            continue

        prof_livres = [
            pid for pid in df_profissionais["ID Prestador"].astype(str)
            if (pid not in profissionais_ocupadas_no_dia[data_atend]) and (_prof_ok(pid, df_profissionais) is not None)
        ]
        if not prof_livres:
            continue

        pend_rows = df_pend.reset_index(drop=True)
        cost = []
        elig_map = []
        for _, row in pend_rows.iterrows():
            cpf = row["CPF_CNPJ"]
            bloqueados = (
                df_bloqueio[df_bloqueio["CPF_CNPJ"] == cpf]["ID Prestador"]
                .astype(str).str.strip().tolist()
            )
            linha_cost = []
            linha_elig = []
            for pid in prof_livres:
                if pid in bloqueados:
                    linha_cost.append(PENAL); linha_elig.append(False); continue
                if pid in profissionais_reservadas_no_dia[data_atend]:
                    aloc = preferida_do_cliente_no_dia[data_atend]
                    reservado_para = next((c for c, p in aloc.items() if str(p).strip() == pid), None)
                    if reservado_para and reservado_para != cpf:
                        linha_cost.append(PENAL); linha_elig.append(False); continue
                d = _dist_from_df(cpf, pid, df_distancias)
                if d is None:
                    linha_cost.append(PENAL); linha_elig.append(False); continue
                linha_cost.append(d)
                linha_elig.append(True)
            cost.append(linha_cost)
            elig_map.append(linha_elig)

        if not cost or not cost[0]:
            continue

        pairs = hungarian_min_cost(cost)

        for i, j in pairs:
            if i < 0 or j < 0:
                continue
            if cost[i][j] >= PENAL:
                continue

            row = pend_rows.iloc[i]
            os_id = row["OS"]; cpf = row["CPF_CNPJ"]
            pid = str(prof_livres[j])

            d = cost[i][j]
            crit_texto = f"cliente: {_qtd_cli(df_cliente_prestador, cpf, pid)} | total: {_qtd_tot(df_qtd_por_prestador, pid)} — {d:.2f} km"
            os_primeira_candidata[(data_atend, os_id)] = (pid, crit_texto, "Mais próxima geograficamente (otimizado)")
            profissionais_ocupadas_no_dia[data_atend].add(pid)
            profissionais_sugeridas_no_dia[data_atend].add(pid)

        for idx, row in pend_rows.iterrows():
            os_id = row["OS"]; cpf = row["CPF_CNPJ"]
            dist_line = cost[idx]
            elig_line = elig_map[idx]
            if not dist_line:
                continue
            best_j = None; best_d = None
            for jj, (dd, ok) in enumerate(zip(dist_line, elig_line)):
                if not ok:
                    continue
                if (best_d is None) or (dd < best_d):
                    best_d = dd; best_j = jj
            escolhido = os_primeira_candidata.get((data_atend, os_id))
            if escolhido:
                pid_escolhido = escolhido[0]
                dist_escolhida = _dist_from_df(cpf, pid_escolhido, df_distancias)
                pid_best = str(prof_livres[best_j]) if best_j is not None else None
                dist_best = float(best_d) if best_d is not None else None
                motivo = ""
                if pid_best is not None and pid_escolhido != pid_best:
                    motivo = "Alocação ótima global (outra OS precisava mais), mantendo não-repetição no dia."
                auditoria_proximidade.append({
                    "Data": data_atend, "OS": os_id, "CPF_CNPJ": cpf,
                    "Prof_Atribuida": pid_escolhido, "Dist_Atribuida_km": dist_escolhida,
                    "Prof_Mais_Prox_Elegivel": pid_best, "Dist_Mais_Prox_km": dist_best,
                    "Motivo_Nao_Mais_Proxima": motivo
                })

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
        linha["Mensagem Padrão"] = gerar_mensagem_personalizada(
            "PROFISSIONAL", nome_cliente, data_1, servico, duracao_servico,
            rua, numero, complemento, bairro, cidade, latitude, longitude,
            ja_atendeu=False, hora_entrada=hora_entrada, obs_prestador=obs_prestador
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

        primeira = os_primeira_candidata.get((data_atendimento, os_id))
        if primeira:
            idp, crit_text, criterio_nome = primeira
            prof = _prof_ok(idp, df_profissionais)
            if prof is not None:
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
                utilizados.add(str(idp).strip()); col += 1

                if criterio_nome == "Preferência do Cliente":
                    matriz_resultado_corrigida.append(linha)
                    continue

        if col <= MAX_PROF_COLS:
            df_mais = df_cliente_prestador[df_cliente_prestador["CPF_CNPJ"] == cpf]
            if not df_mais.empty:
                max_at = df_mais["Qtd Atendimentos Cliente-Prestador"].max()
                for idp in df_mais[df_mais["Qtd Atendimentos Cliente-Prestador"] == max_at]["ID Prestador"].astype(str):
                    if col > MAX_PROF_COLS:
                        break
                    _add(idp, "Mais atendeu o cliente", True)

        if col <= MAX_PROF_COLS:
            df_hist = df_historico_60_dias[df_historico_60_dias["CPF_CNPJ"] == cpf].sort_values("Data 1", ascending=False)
            if not df_hist.empty:
                _add(str(df_hist["ID Prestador"].iloc[0]), "Último profissional que atendeu", True)

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
                idp = rowd["ID Prestador"]; dkm = float(rowd["Distância (km)"])
                if ultimo_km is None:
                    if _add(idp, "Mais próxima geograficamente", _qtd_cli(df_cliente_prestador, cpf, idp) > 0):
                        ultimo_km = dkm
                else:
                    if dkm >= (ultimo_km + DELTA_KM):
                        if _add(idp, "Mais próxima geograficamente", _qtd_cli(df_cliente_prestador, cpf, idp) > 0):
                            ultimo_km = dkm

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

    df_auditoria = pd.DataFrame(auditoria_proximidade) if auditoria_proximidade else pd.DataFrame(
        columns=["Data","OS","CPF_CNPJ","Prof_Atribuida","Dist_Atribuida_km","Prof_Mais_Prox_Elegivel","Dist_Mais_Prox_km","Motivo_Nao_Mais_Proxima"]
    )

    df_matriz_rotas = pd.DataFrame(matriz_resultado_corrigida)

    app_url = "https://rotasvavive.streamlit.app/"
    df_matriz_rotas["Mensagem Padrão"] = df_matriz_rotas.apply(
        lambda row: f"👉 [Clique aqui para validar seu aceite]({app_url}?aceite={row['OS']})\n\n{row['Mensagem Padrão']}",
        axis=1
    )

    for i in range(1, MAX_PROF_COLS + 1):
        for c in [f"Classificação da Profissional {i}", f"Critério {i}", f"Nome Prestador {i}", f"Celular {i}", f"Critério Utilizado {i}"]:
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
        ])

    df_matriz_rotas = df_matriz_rotas[base_cols + prestador_cols]

    final_path = os.path.join(output_dir, ROTAS_FILE)
    with pd.ExcelWriter(final_path, engine="xlsxwriter") as writer:
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
        df_auditoria.to_excel(writer, sheet_name="Auditoria Proximidade", index=False)

    return final_path


# =========================================================
# ROTAS: LINK DE ACEITE
# =========================================================
aceite_os = st.query_params.get("aceite", None)
origem_aceite = st.query_params.get("origem", None)
if aceite_os:
    exibe_formulario_aceite(aceite_os, origem=origem_aceite)
    st.stop()


# =========================================================
# LOGIN GLOBAL
# =========================================================
if "admin_autenticado" not in st.session_state:
    st.session_state.admin_autenticado = False

st.markdown("""
<div style='display:flex;align-items:center;gap:16px'>
  <img src='https://i.imgur.com/gIhC0fC.png' height='48'>
  <span style='font-size:1.7em;font-weight:700;color:#18d96b;letter-spacing:1px;'>
    BELO HORIZONTE || PORTAL DE ATENDIMENTOS
  </span>
</div>
<p style='color:#666;font-size:1.08em;margin:8px 0 18px 0'>
  Consulte abaixo os atendimentos disponíveis!
</p>
""", unsafe_allow_html=True)


# =========================================================
# PORTAL PÚBLICO (SEMPRE VISÍVEL)
# =========================================================
cfg = load_cfg()

if os.path.exists(PORTAL_EXCEL) and os.path.exists(PORTAL_OS_LIST):
    dfp = pd.read_excel(PORTAL_EXCEL, sheet_name="Clientes")
    with open(PORTAL_OS_LIST, "r", encoding="utf-8") as f:
        os_list = json.load(f)

    dfp = dfp[~dfp["OS"].isna()].copy()
    dfp["OS"] = padronizar_os_series(dfp["OS"])
    os_list = [str(int(x)).strip() for x in os_list]
    dfp = dfp[dfp["OS"].isin(os_list)]

    # Ocultar OS com >= max_sim_por_os aceites "Sim"
    if os.path.exists(ACEITES_FILE):
        dfa = pd.read_excel(ACEITES_FILE)
        if "OS" in dfa.columns and "Aceitou" in dfa.columns:
            dfa["OS"] = padronizar_os_series(dfa["OS"])
            aceites_sim = dfa[dfa["Aceitou"].astype(str).str.strip().str.lower() == "sim"]
            contagem = aceites_sim.groupby("OS").size()
            os_lotadas = contagem[contagem >= int(cfg["max_sim_por_os"])].index.tolist()
            dfp = dfp[~dfp["OS"].isin(os_lotadas)]

    if dfp.empty:
        st.info("Nenhum atendimento disponível.")
    else:
        st.write(f"Exibindo {len(dfp)} atendimentos:")
        for _, row in dfp.iterrows():
            servico = row.get("Serviço", "")
            bairro = row.get("Bairro", "")
            data_pt = formatar_data_portugues(row.get("Data 1", ""))
            hora_entrada = row.get("Hora de entrada", "")
            hora_servico = row.get("Horas de serviço", "")
            referencia = row.get("Ponto de Referencia", "")
            os_id = str(row.get("OS", "")).strip()

            st.markdown(f"""
            <div style="background:#fff;border:1.5px solid #eee;border-radius:18px;
                        padding:18px 18px 12px 18px;margin-bottom:14px;
                        min-width:260px;max-width:440px;color:#00008B;font-family:Arial,sans-serif;">
              <div style="font-size:1.2em;font-weight:bold;color:#00008B;margin-bottom:2px;">{servico}</div>
              <div style="font-size:1em;color:#00008B;margin-bottom:7px;">
                <b style="color:#00008B;margin-left:24px">Bairro:</b> <span>{bairro}</span>
              </div>
              <div style="font-size:0.95em;color:#00008B;">
                <b>Data:</b> <span>{data_pt}</span><br>
                <b>Hora de entrada:</b> <span>{hora_entrada}</span><br>
                <b>Horas de serviço:</b> <span>{hora_servico}</span><br>
                <b>Ponto de Referência:</b> <span>{referencia if referencia and referencia != 'nan' else '-'}</span>
              </div>
            </div>
            """, unsafe_allow_html=True)

            st.markdown("""
            <style>
            div[role="button"][aria-expanded] {
                background:#25D366 !important;
                color:#fff !important;
                border-radius:10px !important;
                font-weight:bold;
                font-size:1.08em;
            }
            </style>
            """, unsafe_allow_html=True)

            with st.expander("Tem disponibilidade? Clique aqui para aceitar este atendimento!"):
                profissional = st.text_input("Nome da Profissional (OBRIGATÓRIO)", key=f"pub_prof_{os_id}")
                telefone = st.text_input("Telefone para contato (OBRIGATÓRIO)", key=f"pub_tel_{os_id}")
                resposta = st.empty()

                ok = bool((profissional or "").strip()) and bool((telefone or "").strip())

                if st.button("Sim, tenho interesse neste atendimento.", key=f"pub_btn_{os_id}", use_container_width=True, disabled=not ok):
                    try:
                        salvar_aceite(os_id, profissional, telefone, True, origem="portal")
                    except ValueError as e:
                        resposta.error(f"❌ {e}")
                    else:
                        resposta.success("✅ Obrigado! Seu interesse foi registrado com sucesso.")
else:
    st.info("Nenhum atendimento disponível. Aguarde liberação do admin.")


# =========================================================
# CAMPO DE SENHA GLOBAL
# =========================================================
st.divider()
senha = st.text_input("Área restrita. Digite a senha para liberar as demais abas:", type="password")
if st.button("Entrar", key="btn_senha_global"):
    if senha == ADMIN_PASSWORD:
        st.session_state.admin_autenticado = True
        st.rerun()
    else:
        st.error("Senha incorreta. Acesso restrito.")

if not st.session_state.admin_autenticado:
    st.stop()


# =========================================================
# ABAS ADMIN
# =========================================================
tabs = st.tabs([
    "Portal Atendimentos (Admin)",
    "Upload de Arquivo",
    "Matriz de Rotas",
    "Aceites",
    "Profissionais Próximos",
    "Mensagem Rápida",
    "Auditoria (Proximidade)"
])

# ---------------------------------------------------------
# TAB 0 — Admin do portal + Configurações
# ---------------------------------------------------------
with tabs[0]:
    st.subheader("Admin — Portal + Configurações")

    cfg = load_cfg()
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        max_prof = st.number_input("Qtd profissionais por OS", 1, 30, int(cfg["max_prof_cols"]), 1)
    with col2:
        max_os_tel = st.number_input("Max OS por telefone", 1, 50, int(cfg["max_os_por_telefone"]), 1)
    with col3:
        max_tel_os = st.number_input("Max aceite tel/OS", 1, 50, int(cfg["max_aceites_tel_por_os"]), 1)
    with col4:
        max_sim_os = st.number_input("Max 'Sim' por OS", 1, 50, int(cfg["max_sim_por_os"]), 1)

    if st.button("Salvar configurações", key="btn_save_cfg"):
        cfg["max_prof_cols"] = int(max_prof)
        cfg["max_os_por_telefone"] = int(max_os_tel)
        cfg["max_aceites_tel_por_os"] = int(max_tel_os)
        cfg["max_sim_por_os"] = int(max_sim_os)
        save_cfg(cfg)
        st.success("Configurações salvas!")
        st.rerun()

    st.caption("Obs.: mudar 'Qtd profissionais por OS' afeta a geração do Excel. Rode o pipeline novamente para refletir no arquivo.")

    st.divider()

    # Upload/reuso do arquivo do portal e seleção de OS
    if "portal_file_buffer" not in st.session_state:
        st.session_state.portal_file_buffer = None

    up = st.file_uploader("Faça upload do Excel do Portal (aba 'Clientes')", type=["xlsx"], key="portal_upload_admin")
    if up:
        st.session_state.portal_file_buffer = up.getbuffer()
        with open(PORTAL_EXCEL, "wb") as f:
            f.write(st.session_state.portal_file_buffer)
        st.success("Arquivo salvo!")

    df = None
    if st.session_state.portal_file_buffer:
        with open(PORTAL_EXCEL, "wb") as f:
            f.write(st.session_state.portal_file_buffer)
        df = pd.read_excel(PORTAL_EXCEL, sheet_name="Clientes")
    elif os.path.exists(PORTAL_EXCEL):
        df = pd.read_excel(PORTAL_EXCEL, sheet_name="Clientes")

    if df is None:
        st.warning("Envie o Excel do portal para selecionar as OS.")
    else:
        if "Data 1" in df.columns:
            datas_disponiveis = sorted(df["Data 1"].dropna().unique())
            datas_formatadas = [str(pd.to_datetime(d).date()) for d in datas_disponiveis]
            datas_sel = st.multiselect("Filtrar por Data", options=datas_formatadas, default=[])
            if datas_sel:
                df = df[df["Data 1"].astype(str).apply(lambda d: str(pd.to_datetime(d).date()) in datas_sel)]

        opcoes = [
            f'OS {int(float(r.OS))} | {r.get("Cliente","")} | {r.get("Serviço","")} | {r.get("Bairro","")}'
            for _, r in df.iterrows() if not pd.isnull(r.get("OS", np.nan))
        ]
        selecionadas = st.multiselect("Selecione as OS para exibir (OS | Cliente | Serviço | Bairro)", opcoes)

        if st.button("Salvar atendimentos exibidos", key="btn_save_os_list"):
            os_ids = []
            for op in selecionadas:
                if op.startswith("OS "):
                    try:
                        os_ids.append(int(op.split()[1]))
                    except Exception:
                        pass
            with open(PORTAL_OS_LIST, "w", encoding="utf-8") as f:
                json.dump(os_ids, f, ensure_ascii=False, indent=2)
            st.success("Seleção salva!")

# ---------------------------------------------------------
# TAB 1 — Upload pipeline
# ---------------------------------------------------------
with tabs[1]:
    st.subheader("Upload de Arquivo — Rodar Pipeline")

    cfg = load_cfg()
    uploaded_file = st.file_uploader("Selecione o arquivo Excel original", type=["xlsx"], key="upload_pipeline")

    if uploaded_file is not None:
        with st.spinner("Processando... Isso pode levar alguns minutos."):
            with tempfile.TemporaryDirectory() as tempdir:
                temp_path = os.path.join(tempdir, uploaded_file.name)
                with open(temp_path, "wb") as f:
                    f.write(uploaded_file.read())

                try:
                    excel_path = pipeline(temp_path, tempdir, MAX_PROF_COLS=int(cfg["max_prof_cols"]))
                except Exception as e:
                    st.error(f"Erro no processamento: {e}")
                else:
                    if os.path.exists(excel_path):
                        st.success("Processamento finalizado com sucesso!")
                        st.download_button(
                            label="📥 Baixar Excel consolidado",
                            data=open(excel_path, "rb").read(),
                            file_name=ROTAS_FILE,
                            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                            key="download_excel_consolidado"
                        )
                        import shutil
                        shutil.copy(excel_path, ROTAS_FILE)
                    else:
                        st.error("Arquivo final não encontrado. Ocorreu um erro no pipeline.")

# ---------------------------------------------------------
# TAB 2 — Matriz de Rotas (SEM KeyError)
# ---------------------------------------------------------
with tabs[2]:
    st.subheader("Matriz de Rotas")

    if not os.path.exists(ROTAS_FILE):
        st.info("Faça o upload e rode o pipeline para liberar a matriz de rotas.")
    else:
        df_rotas = pd.read_excel(ROTAS_FILE, sheet_name="Rotas")

        datas = df_rotas["Data 1"].dropna().sort_values().dt.date.unique() if "Data 1" in df_rotas else []
        data_sel = st.selectbox("Filtrar por data", options=["Todos"] + [str(d) for d in datas], key="data_rotas")

        clientes = df_rotas["Nome Cliente"].dropna().unique().tolist() if "Nome Cliente" in df_rotas else []
        cliente_sel = st.selectbox("Filtrar por cliente", options=["Todos"] + list(clientes), key="cliente_rotas")

        # ✅ pega só colunas existentes "Nome Prestador X" (evita KeyError)
        nome_cols = [c for c in df_rotas.columns if str(c).startswith("Nome Prestador ")]
        def _idx(c):
            digits = "".join(ch for ch in str(c) if ch.isdigit())
            return int(digits) if digits else 0
        nome_cols = sorted(nome_cols, key=_idx)

        profissionais = []
        for c in nome_cols:
            profissionais.extend(df_rotas[c].dropna().astype(str).tolist())
        profissionais = sorted(list(set([p for p in profissionais if p and p.lower() != "nan"])))

        profissional_sel = st.selectbox("Filtrar por profissional", options=["Todos"] + profissionais, key="prof_rotas")

        df_filt = df_rotas.copy()
        if data_sel != "Todos" and "Data 1" in df_filt:
            df_filt = df_filt[df_filt["Data 1"].dt.date.astype(str) == data_sel]
        if cliente_sel != "Todos" and "Nome Cliente" in df_filt:
            df_filt = df_filt[df_filt["Nome Cliente"] == cliente_sel]
        if profissional_sel != "Todos" and nome_cols:
            mask = False
            for c in nome_cols:
                mask |= (df_filt[c].astype(str) == profissional_sel)
            df_filt = df_filt[mask]

        st.dataframe(df_filt, use_container_width=True)

        st.download_button(
            label="📥 Baixar Excel consolidado",
            data=open(ROTAS_FILE, "rb").read(),
            file_name=ROTAS_FILE,
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )

# ---------------------------------------------------------
# TAB 3 — Aceites
# ---------------------------------------------------------
with tabs[3]:
    st.subheader("Aceites")

    if not os.path.exists(ACEITES_FILE):
        st.info("Nenhum aceite registrado ainda.")
    else:
        df_aceites = pd.read_excel(ACEITES_FILE)
        st.dataframe(df_aceites, use_container_width=True)

# ---------------------------------------------------------
# TAB 4 — Profissionais Próximos
# ---------------------------------------------------------
with tabs[4]:
    st.subheader("Buscar Profissionais Próximos")

    lat = st.number_input("Latitude", value=-19.900000, format="%.6f")
    lon = st.number_input("Longitude", value=-43.900000, format="%.6f")
    n = st.number_input("Qtd. profissionais", min_value=1, value=5, step=1)

    if st.button("Buscar", key="btn_buscar_prox"):
        if not os.path.exists(ROTAS_FILE):
            st.info("Faça upload e processamento do arquivo para habilitar a busca.")
        else:
            df_profissionais = pd.read_excel(ROTAS_FILE, sheet_name="Profissionais")
            mask_inativo_nome = df_profissionais["Nome Prestador"].astype(str).str.contains("inativo", case=False, na=False)
            df_profissionais = df_profissionais[~mask_inativo_nome]
            df_profissionais = df_profissionais.dropna(subset=["Latitude Profissional", "Longitude Profissional"])
            input_coords = (lat, lon)
            df_profissionais["Distância_km"] = df_profissionais.apply(
                lambda row: geodesic(input_coords, (row["Latitude Profissional"], row["Longitude Profissional"])).km, axis=1
            )
            df_melhores = df_profissionais.sort_values("Distância_km").head(int(n))
            st.dataframe(df_melhores[[
                "Nome Prestador", "Celular", "Qtd Atendimentos",
                "Latitude Profissional", "Longitude Profissional", "Distância_km"
            ]], use_container_width=True)

# ---------------------------------------------------------
# TAB 5 — Mensagem Rápida
# ---------------------------------------------------------
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

    if st.button("Gerar Mensagem", key="btn_msg_rapida"):
        if not os_id.strip():
            st.error("Preencha o código da OS!")
        else:
            mensagem = (
                "🚨🚨🚨\n"
                "     *Oportunidade Relâmpago*\n"
                "                              🚨🚨🚨\n\n"
                "Olá, tudo bem com você?\n\n"
                f"*Data:* {data}\n"
                f"*Bairro:* {bairro}\n"
                f"*Serviço:* {servico}\n"
                f"*Hora de entrada:* {hora_entrada}\n"
                f"*Duração do atendimento:* {duracao}\n\n"
                f"👉 Para aceitar ou recusar, acesse: {link_aceite}\n\n"
                "Se tiver interesse, por favor, nos avise!"
            )
            st.text_area("Mensagem WhatsApp", value=mensagem, height=260)

# ---------------------------------------------------------
# TAB 6 — Auditoria
# ---------------------------------------------------------
with tabs[6]:
    st.subheader("Auditoria por OS — Camada 4 (Proximidade)")
    if not os.path.exists(ROTAS_FILE):
        st.info("Faça o upload e processamento do arquivo para habilitar a auditoria.")
    else:
        try:
            df_aud = pd.read_excel(ROTAS_FILE, sheet_name="Auditoria Proximidade")
        except Exception:
            st.info("A planilha não contém a aba 'Auditoria Proximidade'. Rode o pipeline novamente para gerar.")
            df_aud = pd.DataFrame()

        if df_aud.empty:
            st.info("Sem registros de auditoria para exibir.")
        else:
            df_aud["OS"] = df_aud["OS"].astype(str).str.strip()
            df_rotas = pd.read_excel(ROTAS_FILE, sheet_name="Rotas")
            df_rotas["OS"] = df_rotas["OS"].astype(str).str.strip()
            df_profs = pd.read_excel(ROTAS_FILE, sheet_name="Profissionais")
            df_profs["ID Prestador"] = df_profs["ID Prestador"].astype(str).str.strip()
            id2nome = dict(zip(df_profs["ID Prestador"], df_profs["Nome Prestador"]))

            df_aud["Nome Prof Atribuída"] = df_aud["Prof_Atribuida"].astype(str).str.strip().map(id2nome)
            df_aud["Nome Prof Mais Próx."] = df_aud["Prof_Mais_Prox_Elegivel"].astype(str).str.strip().map(id2nome)

            df_aud = df_aud.merge(df_rotas[["OS", "Nome Cliente", "Data 1", "Serviço"]], how="left", on="OS")
            df_aud["Divergência"] = (
                df_aud["Prof_Atribuida"].astype(str).str.strip() !=
                df_aud["Prof_Mais_Prox_Elegivel"].astype(str).str.strip()
            )

            st.dataframe(df_aud, use_container_width=True)

