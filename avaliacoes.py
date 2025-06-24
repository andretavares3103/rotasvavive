import streamlit as st
import pandas as pd
import numpy as np
import os
from datetime import datetime, timedelta
from geopy.distance import geodesic
import tempfile
import hashlib
import urllib.parse

# =================== CONFIG DA SUA URL STREAMLIT ===================
APP_URL = "https://vavive-rotas.streamlit.app/"

# =================== GERAR LINK DE ACEITE ===================
def gerar_link_aceite(os_id, cliente_nome):
    chave = f"{os_id}_{cliente_nome}"
    token = hashlib.sha256(chave.encode()).hexdigest()[:10]
    params = urllib.parse.urlencode({
        "os": os_id,
        "cliente": cliente_nome,
        "token": token
    })
    return f"{APP_URL}?{params}"

# =================== PÁGINA DE FORMULÁRIO DE ACEITE ===================
def pagina_aceite():
    st.set_page_config(page_title="Aceite de Atendimento", layout="centered")
    st.title("Confirmação de Atendimento Vavivê")
    query_params = st.query_params
    os_id = query_params.get("os")
    cliente_nome = query_params.get("cliente")
    token = query_params.get("token")
    if not os_id or not cliente_nome or not token:
        st.error("Link inválido. Parâmetros ausentes.")
        st.stop()
    os_id = os_id[0] if isinstance(os_id, list) else os_id
    cliente_nome = cliente_nome[0] if isinstance(cliente_nome, list) else cliente_nome
    token = token[0] if isinstance(token, list) else token

    st.markdown(f"""
    ### Olá!

    Você foi indicada para o atendimento de código **{os_id}** para o cliente **{cliente_nome}**.

    Por favor, confirme se pode assumir este atendimento preenchendo os dados abaixo.
    """)

    with st.form("aceite_form"):
        nome_completo = st.text_input("Seu nome completo")
        telefone = st.text_input("Seu telefone (WhatsApp)")
        resposta = st.radio("Você pode assumir este atendimento?", ["SIM", "NÃO"])
        submitted = st.form_submit_button("Enviar resposta")

        if submitted:
            resposta_df = pd.DataFrame([{
                "Timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "OS": os_id,
                "Cliente": cliente_nome,
                "Nome Digitado": nome_completo,
                "Telefone": telefone,
                "Aceite": resposta,
                "Token Recebido": token
            }])
            path_excel = "respostas_aceite.xlsx"
            if os.path.exists(path_excel):
                existing = pd.read_excel(path_excel)
                resposta_df = pd.concat([existing, resposta_df], ignore_index=True)
            resposta_df.to_excel(path_excel, index=False)
            st.success("✅ Sua resposta foi registrada com sucesso! Obrigado.")
            st.stop()

if any(x in st.query_params for x in ["os", "cliente", "token"]):
    pagina_aceite()
    st.stop()

# =================== APP PRINCIPAL ===================
st.set_page_config(page_title="Otimização Rotas Vavivê", layout="wide")
st.title("Otimização de Rotas Vavivê")
st.write("Faça upload do Excel original para gerar todos os dados tratados automaticamente.")

def traduzir_dia_semana(date_obj):
    dias_pt = {
        "Monday": "segunda-feira", "Tuesday": "terça-feira", "Wednesday": "quarta-feira",
        "Thursday": "quinta-feira", "Friday": "sexta-feira", "Saturday": "sábado", "Sunday": "domingo"
    }
    return dias_pt[date_obj.strftime('%A')]

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

def gerar_mensagem_padrao(nome_cliente, data_servico, servico, duracao, rua, numero, complemento,
                          bairro, cidade, latitude, longitude, hora_entrada, obs_prestador, os_id, link_aceite):
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
    mensagem = f"""Olá, tudo bem?

Temos uma oportunidade especial para você nesta região! Quer assumir essa demanda? Está dentro da sua rota!

*Cliente:* {nome_cliente}
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

---

📲 *Clique aqui para aceitar ou recusar este atendimento:* [Formulário de Aceite]({link_aceite})

O atendimento será confirmado após o aceite do atendimento, nome e observações do cliente.

Abs, Vavivê!
"""
    return mensagem

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

    # ============= ATENDIMENTOS FUTUROS ==================
    ontem = datetime.now().date() - timedelta(days=1)
    df_futuros = df_atendimentos[
        (df_atendimentos["Status Serviço"].str.lower() != "cancelado") &
        (df_atendimentos["Data 1"].dt.date > ontem)
    ].copy()

    matriz_resultado = []
    for _, row in df_futuros.iterrows():
        os_id = row["OS"]
        cliente_nome = row["Cliente"]
        data_1 = row["Data 1"]
        servico = row["Serviço"]
        duracao = row["Horas de serviço"]
        hora_entrada = row["Hora de entrada"]
        obs_prestador = row.get("Observações prestador", "")
        plano = row.get("Plano", "")
        ponto_ref = row.get("Ponto de Referencia", "")
        cli = df_clientes[df_clientes["Nome Cliente"] == cliente_nome]
        if not cli.empty:
            rua = cli.iloc[0]["Rua"]
            numero = cli.iloc[0]["Número"]
            complemento = cli.iloc[0]["Complemento"]
            bairro = cli.iloc[0]["Bairro"]
            cidade = cli.iloc[0]["Cidade"]
            latitude = cli.iloc[0]["Latitude Cliente"]
            longitude = cli.iloc[0]["Longitude Cliente"]
        else:
            rua = numero = complemento = bairro = cidade = latitude = longitude = ""
        link_aceite = gerar_link_aceite(os_id, cliente_nome)
        mensagem = gerar_mensagem_padrao(cliente_nome, data_1, servico, duracao, rua, numero, complemento,
                                         bairro, cidade, latitude, longitude, hora_entrada, obs_prestador, os_id, link_aceite)
        matriz_resultado.append({
            "OS": os_id,
            "Nome Cliente": cliente_nome,
            "Data 1": data_1,
            "Serviço": servico,
            "Plano": plano,
            "Duração do Serviço": duracao,
            "Hora de entrada": hora_entrada,
            "Ponto de Referencia": ponto_ref,
            "Mensagem de Convocação": mensagem
        })

    df_matriz_rotas = pd.DataFrame(matriz_resultado)
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
