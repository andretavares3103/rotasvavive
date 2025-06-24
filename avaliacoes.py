ULTIMO CODIGO FUNCIONANDO

import streamlit as st
import pandas as pd
import numpy as np
import os
from datetime import datetime, timedelta
from geopy.distance import geodesic
import tempfile

st.set_page_config(page_title="Otimização Rotas Vavivê", layout="wide")
st.title("Otimização de Rotas Vavivê")
st.write("Faça upload do Excel original para gerar todos os dados tratados automaticamente.")

def traduzir_dia_semana(date_obj):
    dias_pt = {
        "Monday": "segunda-feira", "Tuesday": "terça-feira", "Wednesday": "quarta-feira",
        "Thursday": "quinta-feira", "Friday": "sexta-feira", "Saturday": "sábado", "Sunday": "domingo"
    }
    return dias_pt[date_obj.strftime('%A')]

def formatar_nome_simples(nome):
    nome = nome.strip()
    nome = nome.replace("CI ", "").replace("Ci ", "").replace("C i ", "").replace("C I ", "")
    partes = nome.split()
    if partes[0].lower() in ['ana', 'maria'] and len(partes) > 1:
        return " ".join(partes[:2])
    else:
        return partes[0]

def gerar_mensagem_personalizada(
    nome_profissional, nome_cliente, data_servico, servico,
    duracao, rua, numero, complemento, bairro, cidade, latitude, longitude,
    ja_atendeu, hora_entrada, obs_prestador 
):
    nome_profissional_fmt = formatar_nome_simples(nome_profissional)
    nome_cliente_fmt = nome_cliente.split()[0].strip().title()
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
        else "Responda com SIM caso tenha disponibilidade!"
    )
    rodape = (
        "O atendimento será confirmado após o aceite do atendimento, Nome e observações do cliente. Ok?\n\n"
        "Lembre que o cliente irá receber o *profissional indicado pela Vavivê*. Lembre-se das nossas 3 confirmações do atendimento!\n\n"
        "*CONFIRME SE O ATENDINEMTO AINDA ESTÁ VÁLIDO\n\n*"
        "Abs, Vavivê!"
    )
    mensagem = f"""Olá, Tudo bem com você?
Temos uma oportunidade especial para você nesta região! Quer assumir essa demanda? Está dentro da sua rota!
*Cliente:* {nome_cliente_fmt}
📅 *Data:* {data_linha}
🛠️ *Serviço:* {servico}
🕒 *Hora de entrada:* {hora_entrada}
⏱️ *Duração do Atendimento:* {duracao}
📍 *Endereço:* {endereco_str}
📍 *Bairro:* {bairro}
🏙️ *Cidade:* {cidade}
💬 *Observações do Atendimento:* {obs_prestador}
*LINK DO GOOGLE MAPAS*
{"🌎 [Abrir no Google Mapas](" + maps_url + ")" if maps_url else ""}
{fechamento}
{rodape}
"""
    return mensagem

def padronizar_cpf_cnpj(coluna):
    return (
        coluna.astype(str)
        .str.replace(r'\D', '', regex=True)
        .str.zfill(11)  # Se só CPF, use 11; se também CNPJ, use 14
        .str.strip()
    )

def salvar_df(df, nome_arquivo, output_dir):
    caminho = os.path.join(output_dir, f"{nome_arquivo}.xlsx")
    df.to_excel(caminho, index=False)

def pipeline(file_path, output_dir):
    import xlsxwriter

    # ============= ABA CLIENTES ==================
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

    # ============= ABA PROFISSIONAIS ==================
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

    # ============= ABA PREFERENCIAS ==================
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

    # ============= ABA BLOQUEIO ==================
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

    # ============= ABA QUERIDINHOS ==================
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

    # ============= ABA SUMIDINHOS ==================
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

    # ============= ABA ATENDIMENTOS ==================
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

    hoje = datetime.now().date()
    limite = hoje - timedelta(days=60)
    data1_datetime = pd.to_datetime(df_atendimentos["Data 1"], errors="coerce")
    df_historico_60_dias = df_atendimentos[
        (df_atendimentos["Status Serviço"].str.lower() != "cancelado") &
        (data1_datetime.dt.date < hoje) &
        (data1_datetime.dt.date >= limite)
    ].copy()
    df_historico_60_dias = df_historico_60_dias[[
        "CPF_CNPJ","Cliente","Data 1","Status Serviço","Serviço",
        "Duração do Serviço","Hora de entrada","ID Prestador","Prestador", "Observações prestador"
    ]]
    salvar_df(df_historico_60_dias, "df_historico_60_dias", output_dir)

    # Cliente x Prestador histórico
    df_cliente_prestador = df_historico_60_dias.groupby(
        ["CPF_CNPJ","ID Prestador"]
    ).size().reset_index(name="Qtd Atendimentos Cliente-Prestador")
    salvar_df(df_cliente_prestador, "df_cliente_prestador", output_dir)

    # Qtd atendimentos por prestador histórico
    df_qtd_por_prestador = df_historico_60_dias.groupby(
        "ID Prestador"
    ).size().reset_index(name="Qtd Atendimentos Prestador")
    salvar_df(df_qtd_por_prestador, "df_qtd_por_prestador", output_dir)

    # ============= DISTANCIAS ==================
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

    # ============= JOIN PREFERENCIAS/BLOQUEIO COORDS ==================
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

    # ============= ATEND FUTURO ==================
    ontem = datetime.now().date() - timedelta(days=1)
    df_futuros = df_atendimentos[
        (df_atendimentos["Status Serviço"].str.lower() != "cancelado") &
        (df_atendimentos["Data 1"].dt.date > ontem)
    ].copy()
    df_futuros_com_clientes = df_futuros.merge(
        df_clientes_coord, on="CPF_CNPJ", how="left"
    )
    colunas_uteis = [
        "OS","Data 1","Status Serviço","CPF_CNPJ","Cliente","Serviço",
        "Duração do Serviço","Hora de entrada","Ponto de Referencia",
        "ID Prestador","Prestador","Latitude Cliente","Longitude Cliente","Plano", "Observações prestador"
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

    # ============= EXPORTAR PICKLES ==================
    df_clientes.to_pickle('df_clientes.pkl')
    df_profissionais.to_pickle('df_profissionais.pkl')
    df_preferencias.to_pickle('df_preferencias.pkl')
    df_bloqueio.to_pickle('df_bloqueio.pkl')
    df_queridinhos.to_pickle('df_queridinhos.pkl')
    df_sumidinhos.to_pickle('df_sumidinhos.pkl')
    df_atendimentos.to_pickle('df_atendimentos.pkl')
    df_historico_60_dias.to_pickle('df_historico_60_dias.pkl')
    df_cliente_prestador.to_pickle('df_cliente_prestador.pkl')
    df_qtd_por_prestador.to_pickle('df_qtd_por_prestador.pkl')
    df_distancias.to_pickle('df_distancias.pkl')
    df_preferencias_completo.to_pickle('df_preferencias_completo.pkl')
    df_bloqueio_completo.to_pickle('df_bloqueio_completo.pkl')
    df_atendimentos_futuros_validos.to_pickle('df_atendimentos_futuros_validos.pkl')
    df_atendimentos_sem_localizacao.to_pickle('df_atendimentos_sem_localizacao.pkl')
    df_distancias_alerta.to_pickle('df_distancias_alerta.pkl')

    # ====================== MATRIZ ROTAS - Bloco Corrigido ======================
    matriz_resultado_corrigida = []
    
    preferidas_alocadas_dia = dict()  # {data: set de ids já alocadas como preferidas naquele dia}
    
    for _, atendimento in df_atendimentos_futuros_validos.iterrows():
        data_atendimento = atendimento["Data 1"].date()
        if data_atendimento not in preferidas_alocadas_dia:
            preferidas_alocadas_dia[data_atendimento] = set()
    
        os_id = atendimento["OS"]
        cpf = atendimento["CPF_CNPJ"]
        nome_cliente = atendimento["Cliente"]
        data_1 = atendimento["Data 1"]
        servico = atendimento["Serviço"]
        duracao_servico = atendimento["Duração do Serviço"]
        hora_entrada = atendimento["Hora de entrada"]
        obs_prestador = atendimento["Observações prestador"]
        ponto_referencia = atendimento["Ponto de Referencia"]
        lat_cliente = atendimento["Latitude Cliente"]
        lon_cliente = atendimento["Longitude Cliente"]
        plano = atendimento.get("Plano", "")
    
        bloqueados = (
            df_bloqueio_completo[df_bloqueio_completo["CPF_CNPJ"] == cpf]["ID Prestador"]
            .astype(str).str.strip().tolist()
        )
    
        linha = {
            "OS": os_id,
            "CPF_CNPJ": cpf,
            "Nome Cliente": nome_cliente,
            "Plano": plano,
            "Data 1": data_1,
            "Serviço": servico,
            "Duração do Serviço": duracao_servico,
            "Hora de entrada": hora_entrada,
            "Observações prestador": obs_prestador,
            "Ponto de Referencia": ponto_referencia
        }
    
        cliente_match = df_clientes[df_clientes["CPF_CNPJ"] == cpf]
        cliente_info = cliente_match.iloc[0] if not cliente_match.empty else None
        if cliente_info is not None:
            rua = cliente_info["Rua"]
            numero = cliente_info["Número"]
            complemento = cliente_info["Complemento"]
            bairro = cliente_info["Bairro"]
            cidade = cliente_info["Cidade"]
            latitude = cliente_info["Latitude Cliente"]
            longitude = cliente_info["Longitude Cliente"]
        else:
            rua = numero = complemento = bairro = cidade = latitude = longitude = ""
    
        linha["Mensagem Padrão"] = gerar_mensagem_personalizada(
            "PROFISSIONAL",
            nome_cliente, data_1, servico,
            duracao_servico, rua, numero, complemento, bairro, cidade,
            latitude, longitude, ja_atendeu=False,
            hora_entrada=hora_entrada, 
            obs_prestador=obs_prestador
        )
    
        utilizados = set()
        col = 1
    
        # 1. Preferência do cliente (NÃO repete no mesmo dia)
        preferencia_cliente_df = df_preferencias_completo[df_preferencias_completo["CPF_CNPJ"] == cpf]
        preferida_id = None
        if not preferencia_cliente_df.empty:
            id_preferida_temp = str(preferencia_cliente_df.iloc[0]["ID Prestador"]).strip()
            profissional_preferida_info = df_profissionais[df_profissionais["ID Prestador"].astype(str).str.strip() == id_preferida_temp]
            if (
                not profissional_preferida_info.empty
                and id_preferida_temp not in bloqueados
                and pd.notnull(profissional_preferida_info.iloc[0]["Latitude Profissional"])
                and pd.notnull(profissional_preferida_info.iloc[0]["Longitude Profissional"])
                and "inativo" not in profissional_preferida_info.iloc[0]["Nome Prestador"].lower()
                and id_preferida_temp not in preferidas_alocadas_dia[data_atendimento]  # NOVA REGRA
            ):
                preferida_id = id_preferida_temp
                nome_prof = profissional_preferida_info.iloc[0]["Nome Prestador"]
                celular = profissional_preferida_info.iloc[0]["Celular"]
                lat_prof = profissional_preferida_info.iloc[0]["Latitude Profissional"]
                lon_prof = profissional_preferida_info.iloc[0]["Longitude Profissional"]
                qtd_atend_cliente_pref = df_cliente_prestador[
                    (df_cliente_prestador["CPF_CNPJ"] == cpf) &
                    (df_cliente_prestador["ID Prestador"] == preferida_id)
                ]["Qtd Atendimentos Cliente-Prestador"]
                qtd_atend_cliente_pref = int(qtd_atend_cliente_pref.iloc[0]) if not qtd_atend_cliente_pref.empty else 0
                qtd_atend_total_pref = df_qtd_por_prestador[
                    df_qtd_por_prestador["ID Prestador"] == preferida_id
                ]["Qtd Atendimentos Prestador"]
                qtd_atend_total_pref = int(qtd_atend_total_pref.iloc[0]) if not qtd_atend_total_pref.empty else 0
                distancia_pref_df = df_distancias[
                    (df_distancias["CPF_CNPJ"] == cpf) & (df_distancias["ID Prestador"] == preferida_id)
                ]
                distancia_pref = float(distancia_pref_df["Distância (km)"].iloc[0]) if not distancia_pref_df.empty else np.nan
                criterio = f"cliente: {qtd_atend_cliente_pref} | total: {qtd_atend_total_pref} — {distancia_pref:.2f} km"
                linha[f"Classificação da Profissional {col}"] = col
                linha[f"Critério {col}"] = criterio
                linha[f"Nome Prestador {col}"] = nome_prof
                linha[f"Celular {col}"] = celular
                linha[f"Mensagem {col}"] = gerar_mensagem_personalizada(
                    nome_prof, nome_cliente, data_1, servico,
                    duracao_servico, rua, numero, complemento, bairro, cidade,
                    latitude, longitude, ja_atendeu=True,
                    hora_entrada=hora_entrada,
                    obs_prestador=obs_prestador
                )
                linha[f"Critério Utilizado {col}"] = "Preferência do Cliente"
                utilizados.add(preferida_id)
                preferidas_alocadas_dia[data_atendimento].add(preferida_id)
                col += 1
    
        # 2. Mais atendeu o cliente
        df_candidatos = df_profissionais[
            ~df_profissionais["ID Prestador"].astype(str).str.strip().isin(bloqueados)
        ].copy()
        df_mais_atendeu = df_cliente_prestador[df_cliente_prestador["CPF_CNPJ"] == cpf]
        if not df_mais_atendeu.empty:
            mais_atend = df_mais_atendeu["Qtd Atendimentos Cliente-Prestador"].max()
            mais_atendeu_ids = df_mais_atendeu[df_mais_atendeu["Qtd Atendimentos Cliente-Prestador"] == mais_atend]["ID Prestador"]
            for id_ in mais_atendeu_ids:
                id_prof = str(id_)
                if id_prof in utilizados or id_prof in preferidas_alocadas_dia[data_atendimento]:
                    continue
                prof = df_profissionais[df_profissionais["ID Prestador"].astype(str).str.strip() == id_prof]
                if not prof.empty:
                    lat_prof = prof.iloc[0]["Latitude Profissional"]
                    lon_prof = prof.iloc[0]["Longitude Profissional"]
                    if pd.notnull(lat_prof) and pd.notnull(lon_prof) and "inativo" not in prof.iloc[0]["Nome Prestador"].lower():
                        qtd_atend_cliente = int(mais_atend)
                        qtd_atend_total = int(df_qtd_por_prestador[df_qtd_por_prestador["ID Prestador"] == id_prof]["Qtd Atendimentos Prestador"].iloc[0]) if not df_qtd_por_prestador[df_qtd_por_prestador["ID Prestador"] == id_prof].empty else 0
                        distancia = float(df_distancias[(df_distancias["CPF_CNPJ"] == cpf) & (df_distancias["ID Prestador"] == id_prof)]["Distância (km)"].iloc[0]) if not df_distancias[(df_distancias["CPF_CNPJ"] == cpf) & (df_distancias["ID Prestador"] == id_prof)].empty else np.nan
                        criterio = f"cliente: {qtd_atend_cliente} | total: {qtd_atend_total} — {distancia:.2f} km"
                        linha[f"Classificação da Profissional {col}"] = col
                        linha[f"Critério {col}"] = criterio
                        linha[f"Nome Prestador {col}"] = prof.iloc[0]["Nome Prestador"]
                        linha[f"Celular {col}"] = prof.iloc[0]["Celular"]
                        linha[f"Mensagem {col}"] = gerar_mensagem_personalizada(
                            prof.iloc[0]["Nome Prestador"], nome_cliente, data_1, servico,
                            duracao_servico, rua, numero, complemento, bairro, cidade,
                            latitude, longitude, ja_atendeu=True,
                            hora_entrada=hora_entrada,
                            obs_prestador=obs_prestador
                        )
                        linha[f"Critério Utilizado {col}"] = "Mais atendeu o cliente"
                        utilizados.add(id_prof)
                        col += 1
    
        # 3. Último profissional que atendeu
        df_hist_cliente = df_historico_60_dias[df_historico_60_dias["CPF_CNPJ"] == cpf]
        if not df_hist_cliente.empty:
            df_hist_cliente = df_hist_cliente.sort_values("Data 1", ascending=False)
            ultimo_prof_id = str(df_hist_cliente["ID Prestador"].iloc[0])
            if ultimo_prof_id not in utilizados and ultimo_prof_id not in bloqueados and ultimo_prof_id not in preferidas_alocadas_dia[data_atendimento]:
                prof = df_profissionais[df_profissionais["ID Prestador"].astype(str).str.strip() == ultimo_prof_id]
                if not prof.empty:
                    lat_prof = prof.iloc[0]["Latitude Profissional"]
                    lon_prof = prof.iloc[0]["Longitude Profissional"]
                    if pd.notnull(lat_prof) and pd.notnull(lon_prof) and "inativo" not in prof.iloc[0]["Nome Prestador"].lower():
                        qtd_atend_cliente = int(df_cliente_prestador[(df_cliente_prestador["CPF_CNPJ"] == cpf) & (df_cliente_prestador["ID Prestador"] == ultimo_prof_id)]["Qtd Atendimentos Cliente-Prestador"].iloc[0]) if not df_cliente_prestador[(df_cliente_prestador["CPF_CNPJ"] == cpf) & (df_cliente_prestador["ID Prestador"] == ultimo_prof_id)].empty else 0
                        qtd_atend_total = int(df_qtd_por_prestador[df_qtd_por_prestador["ID Prestador"] == ultimo_prof_id]["Qtd Atendimentos Prestador"].iloc[0]) if not df_qtd_por_prestador[df_qtd_por_prestador["ID Prestador"] == ultimo_prof_id].empty else 0
                        distancia = float(df_distancias[(df_distancias["CPF_CNPJ"] == cpf) & (df_distancias["ID Prestador"] == ultimo_prof_id)]["Distância (km)"].iloc[0]) if not df_distancias[(df_distancias["CPF_CNPJ"] == cpf) & (df_distancias["ID Prestador"] == ultimo_prof_id)].empty else np.nan
                        criterio = f"cliente: {qtd_atend_cliente} | total: {qtd_atend_total} — {distancia:.2f} km"
                        linha[f"Classificação da Profissional {col}"] = col
                        linha[f"Critério {col}"] = criterio
                        linha[f"Nome Prestador {col}"] = prof.iloc[0]["Nome Prestador"]
                        linha[f"Celular {col}"] = prof.iloc[0]["Celular"]
                        linha[f"Mensagem {col}"] = gerar_mensagem_personalizada(
                            prof.iloc[0]["Nome Prestador"], nome_cliente, data_1, servico,
                            duracao_servico, rua, numero, complemento, bairro, cidade,
                            latitude, longitude, ja_atendeu=True,
                            hora_entrada=hora_entrada,
                            obs_prestador=obs_prestador
                        )
                        linha[f"Critério Utilizado {col}"] = "Último profissional que atendeu"
                        utilizados.add(ultimo_prof_id)
                        col += 1
    
        # 4. Profissional preferencial da plataforma (até 5 km)
        if not df_queridinhos.empty:
            for _, qrow in df_queridinhos.iterrows():
                queridinha_id = str(qrow["ID Prestador"]).strip()
                if queridinha_id in utilizados or queridinha_id in bloqueados or queridinha_id in preferidas_alocadas_dia[data_atendimento]:
                    continue
                prof = df_profissionais[df_profissionais["ID Prestador"].astype(str).str.strip() == queridinha_id]
                if not prof.empty:
                    lat_prof = prof.iloc[0]["Latitude Profissional"]
                    lon_prof = prof.iloc[0]["Longitude Profissional"]
                    if pd.notnull(lat_prof) and pd.notnull(lon_prof) and "inativo" not in prof.iloc[0]["Nome Prestador"].lower():
                        dist_row = df_distancias[(df_distancias["CPF_CNPJ"] == cpf) & (df_distancias["ID Prestador"] == queridinha_id)]
                        distancia = float(dist_row["Distância (km)"].iloc[0]) if not dist_row.empty else np.nan
                        if distancia <= 5.0:
                            qtd_atend_cliente = int(df_cliente_prestador[(df_cliente_prestador["CPF_CNPJ"] == cpf) & (df_cliente_prestador["ID Prestador"] == queridinha_id)]["Qtd Atendimentos Cliente-Prestador"].iloc[0]) if not df_cliente_prestador[(df_cliente_prestador["CPF_CNPJ"] == cpf) & (df_cliente_prestador["ID Prestador"] == queridinha_id)].empty else 0
                            qtd_atend_total = int(df_qtd_por_prestador[df_qtd_por_prestador["ID Prestador"] == queridinha_id]["Qtd Atendimentos Prestador"].iloc[0]) if not df_qtd_por_prestador[df_qtd_por_prestador["ID Prestador"] == queridinha_id].empty else 0
                            criterio = f"cliente: {qtd_atend_cliente} | total: {qtd_atend_total} — {distancia:.2f} km"
                            linha[f"Classificação da Profissional {col}"] = col
                            linha[f"Critério {col}"] = criterio
                            linha[f"Nome Prestador {col}"] = prof.iloc[0]["Nome Prestador"]
                            linha[f"Celular {col}"] = prof.iloc[0]["Celular"]
                            linha[f"Mensagem {col}"] = gerar_mensagem_personalizada(
                                prof.iloc[0]["Nome Prestador"], nome_cliente, data_1, servico,
                                duracao_servico, rua, numero, complemento, bairro, cidade,
                                latitude, longitude, ja_atendeu=(qtd_atend_cliente>0),
                                hora_entrada=hora_entrada,
                                obs_prestador=obs_prestador
                            )
                            linha[f"Critério Utilizado {col}"] = "Profissional preferencial da plataforma (até 5 km)"
                            utilizados.add(queridinha_id)
                            col += 1
    
        # 5. Profissional mais próxima geograficamente (até completar 15)
        dist_cand = df_distancias[(df_distancias["CPF_CNPJ"] == cpf)].copy()
        dist_cand = dist_cand[~dist_cand["ID Prestador"].isin(utilizados | set(bloqueados) | preferidas_alocadas_dia[data_atendimento])]
        dist_cand = dist_cand.sort_values("Distância (km)")
        for _, dist_row in dist_cand.iterrows():
            if col > 15:
                break
            prof = df_profissionais[df_profissionais["ID Prestador"].astype(str).str.strip() == str(dist_row["ID Prestador"])]
            if prof.empty:
                continue
            if "inativo" in prof.iloc[0]["Nome Prestador"].lower():
                continue
            lat_prof = prof.iloc[0]["Latitude Profissional"]
            lon_prof = prof.iloc[0]["Longitude Profissional"]
            if not (pd.notnull(lat_prof) and pd.notnull(lon_prof)):
                continue
            qtd_atend_cliente = int(df_cliente_prestador[(df_cliente_prestador["CPF_CNPJ"] == cpf) & (df_cliente_prestador["ID Prestador"] == str(dist_row["ID Prestador"]))]["Qtd Atendimentos Cliente-Prestador"].iloc[0]) if not df_cliente_prestador[(df_cliente_prestador["CPF_CNPJ"] == cpf) & (df_cliente_prestador["ID Prestador"] == str(dist_row["ID Prestador"]))].empty else 0
            qtd_atend_total = int(df_qtd_por_prestador[df_qtd_por_prestador["ID Prestador"] == str(dist_row["ID Prestador"])]["Qtd Atendimentos Prestador"].iloc[0]) if not df_qtd_por_prestador[df_qtd_por_prestador["ID Prestador"] == str(dist_row["ID Prestador"])].empty else 0
            distancia = float(dist_row["Distância (km)"])
            criterio = f"cliente: {qtd_atend_cliente} | total: {qtd_atend_total} — {distancia:.2f} km"
            linha[f"Classificação da Profissional {col}"] = col
            linha[f"Critério {col}"] = criterio
            linha[f"Nome Prestador {col}"] = prof.iloc[0]["Nome Prestador"]
            linha[f"Celular {col}"] = prof.iloc[0]["Celular"]
            linha[f"Mensagem {col}"] = gerar_mensagem_personalizada(
                prof.iloc[0]["Nome Prestador"], nome_cliente, data_1, servico,
                duracao_servico, rua, numero, complemento, bairro, cidade,
                latitude, longitude, ja_atendeu=(qtd_atend_cliente>0),
                hora_entrada=hora_entrada,
                obs_prestador=obs_prestador
            )
            linha[f"Critério Utilizado {col}"] = "Mais próxima geograficamente"
            utilizados.add(str(dist_row["ID Prestador"]))
            col += 1
    
        # 6. Sumidinhos (Baixa Disponibilidade) - posições 16 a 20
        # SÓ entram se já estiverem em utilizados (recomendações anteriores)
        sumidinhos_para_incluir = [sum_id for sum_id in df_sumidinhos["ID Prestador"].astype(str) if sum_id in utilizados]
        for sum_id in sumidinhos_para_incluir:
            if col > 20:
                break
            if sum_id in bloqueados or sum_id in preferidas_alocadas_dia[data_atendimento]:
                continue
            prof = df_profissionais[df_profissionais["ID Prestador"].astype(str).str.strip() == sum_id]
            if prof.empty or "inativo" in prof.iloc[0]["Nome Prestador"].lower():
                continue
            lat_prof = prof.iloc[0]["Latitude Profissional"]
            lon_prof = prof.iloc[0]["Longitude Profissional"]
            if not (pd.notnull(lat_prof) and pd.notnull(lon_prof)):
                continue
            dist_row = df_distancias[(df_distancias["CPF_CNPJ"] == cpf) & (df_distancias["ID Prestador"] == sum_id)]
            distancia = float(dist_row["Distância (km)"].iloc[0]) if not dist_row.empty else np.nan
            qtd_atend_cliente = int(df_cliente_prestador[(df_cliente_prestador["CPF_CNPJ"] == cpf) & (df_cliente_prestador["ID Prestador"] == sum_id)]["Qtd Atendimentos Cliente-Prestador"].iloc[0]) if not df_cliente_prestador[(df_cliente_prestador["CPF_CNPJ"] == cpf) & (df_cliente_prestador["ID Prestador"] == sum_id)].empty else 0
            qtd_atend_total = int(df_qtd_por_prestador[df_qtd_por_prestador["ID Prestador"] == sum_id]["Qtd Atendimentos Prestador"].iloc[0]) if not df_qtd_por_prestador[df_qtd_por_prestador["ID Prestador"] == sum_id].empty else 0
            criterio = f"cliente: {qtd_atend_cliente} | total: {qtd_atend_total} — {distancia:.2f} km"
            linha[f"Classificação da Profissional {col}"] = col
            linha[f"Critério {col}"] = criterio
            linha[f"Nome Prestador {col}"] = prof.iloc[0]["Nome Prestador"]
            linha[f"Celular {col}"] = prof.iloc[0]["Celular"]
            linha[f"Mensagem {col}"] = gerar_mensagem_personalizada(
                prof.iloc[0]["Nome Prestador"], nome_cliente, data_1, servico,
                duracao_servico, rua, numero, complemento, bairro, cidade,
                latitude, longitude, ja_atendeu=(qtd_atend_cliente>0),
                hora_entrada=hora_entrada,
                obs_prestador=obs_prestador
            )
            linha[f"Critério Utilizado {col}"] = "Baixa Disponibilidade"
            col += 1
    
        # 7. Se faltar profissionais para completar até 20, use os mais próximos ainda não recomendados
        if col <= 20:
            dist_restantes = df_distancias[(df_distancias["CPF_CNPJ"] == cpf)].copy()
            dist_restantes = dist_restantes[~dist_restantes["ID Prestador"].isin(utilizados | set(bloqueados) | preferidas_alocadas_dia[data_atendimento])]
            dist_restantes = dist_restantes.sort_values("Distância (km)")
            for _, dist_row in dist_restantes.iterrows():
                if col > 20:
                    break
                prof = df_profissionais[df_profissionais["ID Prestador"].astype(str).str.strip() == str(dist_row["ID Prestador"])]
                if prof.empty:
                    continue
                if "inativo" in prof.iloc[0]["Nome Prestador"].lower():
                    continue
                lat_prof = prof.iloc[0]["Latitude Profissional"]
                lon_prof = prof.iloc[0]["Longitude Profissional"]
                if not (pd.notnull(lat_prof) and pd.notnull(lon_prof)):
                    continue
                qtd_atend_cliente = int(df_cliente_prestador[(df_cliente_prestador["CPF_CNPJ"] == cpf) & (df_cliente_prestador["ID Prestador"] == str(dist_row["ID Prestador"]))]["Qtd Atendimentos Cliente-Prestador"].iloc[0]) if not df_cliente_prestador[(df_cliente_prestador["CPF_CNPJ"] == cpf) & (df_cliente_prestador["ID Prestador"] == str(dist_row["ID Prestador"]))].empty else 0
                qtd_atend_total = int(df_qtd_por_prestador[df_qtd_por_prestador["ID Prestador"] == str(dist_row["ID Prestador"])]["Qtd Atendimentos Prestador"].iloc[0]) if not df_qtd_por_prestador[df_qtd_por_prestador["ID Prestador"] == str(dist_row["ID Prestador"])].empty else 0
                distancia = float(dist_row["Distância (km)"])
                criterio = f"cliente: {qtd_atend_cliente} | total: {qtd_atend_total} — {distancia:.2f} km"
                linha[f"Classificação da Profissional {col}"] = col
                linha[f"Critério {col}"] = criterio
                linha[f"Nome Prestador {col}"] = prof.iloc[0]["Nome Prestador"]
                linha[f"Celular {col}"] = prof.iloc[0]["Celular"]
                linha[f"Mensagem {col}"] = gerar_mensagem_personalizada(
                    prof.iloc[0]["Nome Prestador"], nome_cliente, data_1, servico,
                    duracao_servico, rua, numero, complemento, bairro, cidade,
                    latitude, longitude, ja_atendeu=(qtd_atend_cliente>0),
                    hora_entrada=hora_entrada,
                    obs_prestador=obs_prestador
                )
                linha[f"Critério Utilizado {col}"] = "Mais próxima geograficamente (complemento)"
                col += 1
    
        matriz_resultado_corrigida.append(linha)
# ===================== FIM DO BLOCO DE PRIORIZAÇÃO CORRIGIDO ====================



    df_matriz_rotas = pd.DataFrame(matriz_resultado_corrigida)

    for i in range(1, 21):
        if f"Classificação da Profissional {i}" not in df_matriz_rotas.columns:
            df_matriz_rotas[f"Classificação da Profissional {i}"] = pd.NA
        if f"Critério {i}" not in df_matriz_rotas.columns:
            df_matriz_rotas[f"Critério {i}"] = pd.NA
        if f"Nome Prestador {i}" not in df_matriz_rotas.columns:
            df_matriz_rotas[f"Nome Prestador {i}"] = pd.NA
        if f"Celular {i}" not in df_matriz_rotas.columns:
            df_matriz_rotas[f"Celular {i}"] = pd.NA
        if f"Critério Utilizado {i}" not in df_matriz_rotas.columns:
            df_matriz_rotas[f"Critério Utilizado {i}"] = pd.NA

    base_cols = [
        "OS", "CPF_CNPJ", "Nome Cliente", "Data 1", "Serviço", "Plano", 
        "Duração do Serviço", "Hora de entrada","Observações prestador", "Ponto de Referencia", "Mensagem Padrão"
    ]
    prestador_cols = []
    for i in range(1, 21):
        prestador_cols.extend([
            f"Classificação da Profissional {i}",
            f"Critério {i}",
            f"Nome Prestador {i}",
            f"Celular {i}",
            f"Critério Utilizado {i}",
        ])
    df_matriz_rotas = df_matriz_rotas[base_cols + prestador_cols]

    # Exemplo do final:
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
        df_distancias_alerta.to_excel(writer, sheet_name="df_distancias_alert", index=False)
        # ...salva os outros DataFrames aqui também, se quiser
    return final_path

uploaded_file = st.file_uploader("Selecione o arquivo Excel original", type=["xlsx"])

if uploaded_file:
    with st.spinner("Processando... Isso pode levar alguns segundos."):
        with tempfile.TemporaryDirectory() as tempdir:
            temp_path = os.path.join(tempdir, uploaded_file.name)
            with open(temp_path, "wb") as f:
                f.write(uploaded_file.read())
            try:
                excel_path = pipeline(temp_path, tempdir)
                if os.path.exists(excel_path):
                    with open(excel_path, "rb") as f:
                        data = f.read()
                    st.success("Processamento finalizado com sucesso!")
                    st.download_button(
                        label="📥 Baixar Excel consolidado",
                        data=data,
                        file_name="rotas_bh_dados_tratados_completos.xlsx",
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                    )


                # --- Visualização da aba "Rotas" no Streamlit ---
                    import io
                    
                    st.markdown("### Visualização da aba 'Rotas'")
                    rotas_df = pd.read_excel(io.BytesIO(data), sheet_name="Rotas")
                    st.dataframe(rotas_df, use_container_width=True)

                else:
                    st.error("Arquivo final não encontrado. Ocorreu um erro no pipeline.")
            except Exception as e:
                st.error(f"Erro no processamento: {e}")


st.markdown("""
---
> **Observação:** Os arquivos processados ficam disponíveis para download logo após a execução.  
> Para dúvidas ou adaptações, fale com o suporte!
""")
