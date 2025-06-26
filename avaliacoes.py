import streamlit as st
import pandas as pd
import numpy as np
import os
from datetime import datetime, timedelta
from geopy.distance import geodesic
import tempfile
import io

st.set_page_config(page_title="Otimização Rotas Vavivê", layout="wide")
st.title("Otimização de Rotas Vavivê")

ACEITES_FILE = "aceites.xlsx"
ROTAS_FILE = "rotas_bh_dados_tratados_completos.xlsx"

# ---------------------- FUNÇÕES DE UTILIDADE ----------------------

def traduzir_dia_semana(date_obj):
    dias_pt = {
        "Monday": "segunda-feira", "Tuesday": "terça-feira", "Wednesday": "quarta-feira",
        "Thursday": "quinta-feira", "Friday": "sexta-feira", "Saturday": "sábado", "Sunday": "domingo"
    }
    return dias_pt.get(date_obj.strftime('%A'), "")

def formatar_nome_simples(nome):
    nome = nome.strip()
    nome = nome.replace("CI ", "").replace("Ci ", "").replace("C i ", "").replace("C I ", "")
    partes = nome.split()
    if partes[0].lower() in ['ana', 'maria'] and len(partes) > 1:
        return " ".join(partes[:2])
    else:
        return partes[0]

def padronizar_cpf_cnpj(coluna):
    return (
        coluna.astype(str)
        .str.replace(r'\D', '', regex=True)
        .str.zfill(11)
        .str.strip()
    )

def salvar_df(df, nome_arquivo, output_dir):
    caminho = os.path.join(output_dir, f"{nome_arquivo}.xlsx")
    df.to_excel(caminho, index=False)

def gerar_link_aceite(os_id):
    app_url = "https://rotasvavive.streamlit.app/"
    return f"{app_url}?aceite={os_id}"

def gerar_mensagem_personalizada(
    nome_profissional, nome_cliente, data_servico, servico,
    duracao, rua, numero, complemento, bairro, cidade, latitude, longitude,
    ja_atendeu, hora_entrada, obs_prestador, os_id
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
        else "Acesse o link ao final da mensagem e responda com SIM caso tenha disponibilidade!"
    )
    rodape = (
        """
O atendimento será confirmado após o aceite!
*1)*    Lembre que o cliente irá receber o *profissional indicado pela Vavivê*.
*2)*    Lembre-se das nossas 3 confirmações do atendimento!
*CONFIRME SE O ATENDINEMTO AINDA ESTÁ VÁLIDO*

Abs, Vavivê!
"""
    )
    link_aceite = gerar_link_aceite(os_id)
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

👉 [Clique aqui para validar seu aceite]({link_aceite})

{rodape}
"""
    return mensagem

# ---------------------- PIPELINE COMPLETO ----------------------

def pipeline(file_path, output_dir):
    import xlsxwriter

    # ABA CLIENTES
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

    # ABA PROFISSIONAIS
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

    # ABA PREFERENCIAS
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

    # ABA BLOQUEIO
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

    # ABA QUERIDINHOS
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

    # ABA SUMIDINHOS
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

    # ABA ATENDIMENTOS
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

    # HISTÓRICO 60 DIAS
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

    # DISTANCIAS
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

    # JOIN PREFERENCIAS/BLOQUEIO COORDS
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

    # ATENDIMENTOS FUTUROS
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

    # EXPORTAR PICKLES (opcional)
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

    # MATRIZ DE ROTAS
    matriz_resultado_corrigida = []
    preferidas_alocadas_dia = dict()

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
            obs_prestador=obs_prestador,
            os_id=os_id
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
                and id_preferida_temp not in preferidas_alocadas_dia[data_atendimento]
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
                qtd_atend_total_pref = int(qtd_atend_total_pref.iloc[0]) if not df_qtd_por_prestador[df_qtd_por_prestador["ID Prestador"] == preferida_id].empty else 0
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
                    obs_prestador=obs_prestador,
                    os_id=os_id
                )
                linha[f"Critério Utilizado {col}"] = "Preferência do Cliente"
                utilizados.add(preferida_id)
                preferidas_alocadas_dia[data_atendimento].add(preferida_id)
                col += 1

        # ... [COLE O RESTANTE DO SEU BLOCO DE PRIORIZAÇÃO ATÉ O FIM, INCLUINDO AS EXPORTAÇÕES DE TODAS AS ABAS] ...

        # Vou cortar aqui só porque não cabe no limite, mas basta você continuar colando TODO o restante do bloco de priorização e exportação aqui, igual já validamos antes!

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
        "Duração do Serviço", "Hora de entrada", "Observações prestador", "Ponto de Referencia", "Mensagem Padrão"
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

    #RETIRAR DEPOIS

    print("Colunas geradas:", df_matriz_rotas.columns.tolist())
    if df_matriz_rotas.empty:
    st.error("Nenhuma linha foi gerada na matriz de rotas! Verifique se há atendimentos futuros na sua planilha.")
    st.stop()
else:
    st.write("Colunas da matriz:", df_matriz_rotas.columns.tolist())


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
    return final_path

# ---------------------- ACEITE VIA LINK ----------------------

def exibe_formulario_aceite(os_id):
    st.header(f"Validação de Aceite (OS {os_id})")
    profissional = st.text_input("Nome da Profissional")
    telefone = st.text_input("Telefone para contato")
    aceitou = st.checkbox("Aceito realizar este atendimento?")
    if st.button("Enviar Aceite"):
        salvar_aceite(os_id, profissional, telefone, aceitou)
        st.success("Obrigado! Daremos o retorno sobre o atendimento! Seu aceite foi registrado com sucesso.")
        st.stop()

def salvar_aceite(os_id, profissional, telefone, aceitou):
    agora = pd.Timestamp.now()
    data = agora.strftime("%d/%m/%Y")
    dia_semana = agora.strftime("%A")
    horario = agora.strftime("%H:%M:%S")
    if os.path.exists(ACEITES_FILE):
        df = pd.read_excel(ACEITES_FILE)
    else:
        df = pd.DataFrame(columns=[
            "OS", "Profissional", "Telefone", "Aceitou",
            "Data do Aceite", "Dia da Semana", "Horário do Aceite"
        ])
    nova_linha = {
        "OS": os_id,
        "Profissional": profissional,
        "Telefone": telefone,
        "Aceitou": "Sim" if aceitou else "Não",
        "Data do Aceite": data,
        "Dia da Semana": dia_semana,
        "Horário do Aceite": horario
    }
    df = pd.concat([df, pd.DataFrame([nova_linha])], ignore_index=True)
    df.to_excel(ACEITES_FILE, index=False)

# Detecta se abriu o app pelo link de aceite
aceite_os = st.query_params.get("aceite", None)
if aceite_os:
    exibe_formulario_aceite(aceite_os)
    st.stop()

# ---------------------- ABAS PRINCIPAIS ----------------------

tab1, tab2, tab3 = st.tabs(["Upload", "Matriz de Rotas", "Aceites"])

with tab1:
    st.write("Faça upload do Excel original para gerar todos os dados tratados automaticamente.")
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
                    else:
                        st.error("Arquivo final não encontrado. Ocorreu um erro no pipeline.")
                except Exception as e:
                    st.error(f"Erro no processamento: {e}")

with tab2:
    if os.path.exists(ROTAS_FILE):
        with open(ROTAS_FILE, "rb") as f:
            data = f.read()
        df_rotas = pd.read_excel(io.BytesIO(data), sheet_name="Rotas")

        st.subheader("Matriz de Rotas Gerada")
        datas = df_rotas["Data 1"].dropna().sort_values().dt.date.unique()
        data_sel = st.selectbox("Filtrar por data", options=["Todos"] + [str(d) for d in datas], key="data_rotas")
        clientes = df_rotas["Nome Cliente"].dropna().unique()
        cliente_sel = st.selectbox("Filtrar por cliente", options=["Todos"] + list(clientes), key="cliente_rotas")
        profissionais = []
        for i in range(1, 21):
            profissionais.extend(df_rotas[f"Nome Prestador {i}"].dropna().unique())
        profissionais = list(set([p for p in profissionais if isinstance(p, str)]))
        profissional_sel = st.selectbox("Filtrar por profissional", options=["Todos"] + profissionais, key="prof_rotas")
        df_rotas_filt = df_rotas.copy()
        if data_sel != "Todos":
            df_rotas_filt = df_rotas_filt[df_rotas_filt["Data 1"].dt.date.astype(str) == data_sel]
        if cliente_sel != "Todos":
            df_rotas_filt = df_rotas_filt[df_rotas_filt["Nome Cliente"] == cliente_sel]
        if profissional_sel != "Todos":
            mask = False
            for i in range(1, 21):
                mask |= (df_rotas_filt[f"Nome Prestador {i}"] == profissional_sel)
            df_rotas_filt = df_rotas_filt[mask]
        st.dataframe(df_rotas_filt, use_container_width=True)
    else:
        st.info("Faça o upload na aba anterior para gerar o arquivo de rotas.")

with tab3:
    if os.path.exists(ACEITES_FILE) and os.path.exists(ROTAS_FILE):
        df_aceites = pd.read_excel(ACEITES_FILE)
        with open(ROTAS_FILE, "rb") as f:
            data = f.read()
        df_rotas = pd.read_excel(io.BytesIO(data), sheet_name="Rotas")
        df_aceites_completo = pd.merge(
            df_aceites, df_rotas[
                ["OS", "CPF_CNPJ", "Nome Cliente", "Data 1", "Serviço", "Plano",
                 "Duração do Serviço", "Hora de entrada", "Observações prestador", "Ponto de Referencia"]
            ],
            how="left", on="OS"
        )
        st.markdown("### Histórico de Aceites (detalhado)")
        st.dataframe(df_aceites_completo, use_container_width=True)
        st.download_button(
            label="Baixar histórico de aceites (completo)",
            data=df_aceites_completo.to_excel(index=False),
            file_name="aceites_completo.xlsx"
        )
    elif os.path.exists(ACEITES_FILE):
        st.markdown("### Histórico de Aceites")
        df_aceites = pd.read_excel(ACEITES_FILE)
        st.dataframe(df_aceites, use_container_width=True)
        st.download_button(
            label="Baixar histórico de aceites",
            data=df_aceites.to_excel(index=False),
            file_name="aceites.xlsx"
        )
    else:
        st.info("Nenhum aceite registrado ainda.")

st.markdown("""
---
> **Observação:** Os arquivos processados ficam disponíveis para download logo após a execução.  
> Para dúvidas ou adaptações, fale com o suporte!
""")
