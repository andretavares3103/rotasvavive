with tabs[3]:
    st.markdown("""
        <div style='display:flex;align-items:center;gap:16px'>
            <img src='https://i.imgur.com/gIhC0fC.png' height='48'>
            <span style='font-size:1.7em;font-weight:700;color:#18d96b;letter-spacing:1px;'>PORTAL DE ATENDIMENTOS</span>
        </div>
        <p style='color:#666;font-size:1.08em;margin:8px 0 18px 0'>
            Consulte abaixo os atendimentos disponíveis!
        </p>
        """, unsafe_allow_html=True)

    if not os.path.exists(ROTAS_FILE):
        st.info("Faça upload e processe o Excel para liberar o portal.")
    else:
        # 1️⃣ Lê a aba Clientes do arquivo existente (já carregado no app)
        df = carregar_rotas(ROTAS_FILE)  # usa cache
        df = df[df["Data 1"].notnull()]
        df["Data 1"] = pd.to_datetime(df["Data 1"])
        df["Data 1 Formatada"] = df["Data 1"].dt.strftime("%d/%m/%Y")
        dias_pt = {
            "Monday": "segunda-feira", "Tuesday": "terça-feira", "Wednesday": "quarta-feira",
            "Thursday": "quinta-feira", "Friday": "sexta-feira", "Saturday": "sábado", "Sunday": "domingo"
        }
        df["Dia da Semana"] = df["Data 1"].dt.day_name().map(dias_pt)
        df = df[df["OS"].notnull()]
        df = df.copy()
        if "os_list" not in st.session_state:
            st.session_state.os_list = []
        
        # Aqui já pode garantir as colunas necessárias ANTES de qualquer filtro
        if "Data 1" not in df.columns:
            st.warning("A aba 'Clientes' não possui a coluna 'Data 1'. Corrija o arquivo antes de continuar.")
            st.stop()
            df["Data 1"] = pd.to_datetime(df["Data 1"], errors="coerce")
            df["Data 1 Formatada"] = df["Data 1"].dt.strftime("%d/%m/%Y")
            dias_pt = {
                "Monday": "segunda-feira", "Tuesday": "terça-feira", "Wednesday": "quarta-feira",
                "Thursday": "quinta-feira", "Friday": "sexta-feira", "Saturday": "sábado", "Sunday": "domingo"
            }
            df["Dia da Semana"] = df["Data 1"].dt.day_name().map(dias_pt)
        else:
            df["Data 1 Formatada"] = "-"
            df["Dia da Semana"] = "-"

        # 2️⃣ Seletor protegido por senha para admins
# ---- BLOCO DE SENHA ADMIN ----
        if "exibir_admin" not in st.session_state:
            st.session_state.exibir_admin = False
        
        senha = st.text_input("Área Administrativa - digite a senha para selecionar OS", type="password", value="")
        if st.button("Liberar seleção de atendimentos (admin)"):
            if senha == "vvv":
                st.session_state.exibir_admin = True
            else:
                st.warning("Senha incorreta.")
        
        # ---- BLOCO DE SELEÇÃO DE ATENDIMENTOS (APÓS A SENHA) ----
        if st.session_state.exibir_admin:
            if "os_list" not in st.session_state:
                st.session_state.os_list = []
        
            os_opcoes = [
                f'{row["Nome Cliente"]} | {row["Data 1 Formatada"]} | {row["Serviço"]} | {row["Plano"]}'
                for idx, row in df.iterrows()
            ]
            os_ids = list(df["OS"])
        
            os_selecionadas = st.multiselect(
                "Selecione os atendimentos para exibir",
                options=os_ids,
                format_func=lambda x: os_opcoes[os_ids.index(x)],
                default=st.session_state.os_list
            )
        
            if st.button("Salvar lista de OS exibidas"):
                st.session_state.os_list = os_selecionadas
                st.success("Seleção salva!")
        

        # Exibe sempre os cards das OS permitidas
        df_visiveis = df[df["OS"].isin(st.session_state.os_list)].copy()
        if df_visiveis.empty:
            st.info("Nenhum atendimento disponível para exibição.")
        else:
            st.markdown("<h5>Atendimentos disponíveis:</h5>", unsafe_allow_html=True)
            for _, row in df_visiveis.iterrows():
                servico = row.get("Serviço", "")
                bairro = row.get("Bairro", "")
                data = row.get("Data 1 Formatada", "")
                dia_semana = row.get("Dia da Semana", "")
                horas_servico = row.get("Horas de serviço", "")
                hora_entrada = row.get("Hora de entrada", "")
                referencia = row.get("Ponto de Referencia", "")
                nome_cliente = row.get("Nome Cliente", "")
                mensagem = (
                    f"Aceito o atendimento de {servico} para o cliente {nome_cliente}, no bairro {bairro}, "
                    f"dia {dia_semana}, {data}. Horário de entrada: {hora_entrada}"
                )
                mensagem_url = urllib.parse.quote(mensagem)
                celular = "31995265364"
                whatsapp_url = f"https://wa.me/55{celular}?text={mensagem_url}"

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
                        <b style="color:#00008B;">Cliente:</b> <span>{nome_cliente}</span>
                        <b style="color:#00008B;margin-left:24px">Bairro:</b> <span>{bairro}</span>
                    </div>
                    <div style="font-size:0.95em; color:#00008B;">
                        <b>Data:</b> <span>{data} ({dia_semana})</span><br>
                        <b>Duração:</b> <span>{horas_servico}</span><br>
                        <b>Hora de entrada:</b> <span>{hora_entrada}</span><br>
                        <b>Ponto de Referência:</b> <span>{referencia if referencia and referencia != 'nan' else '-'}</span>
                    </div>
                    <a href="{whatsapp_url}" target="_blank">
                        <button style="margin-top:12px;padding:10px 24px;background:#25D366;color:#fff;border:none;border-radius:8px;font-size:1.02em; font-weight:700;cursor:pointer; width:100%;">
                            Aceitar Atendimento no WhatsApp
                        </button>
                    </a>
                </div>
                """, unsafe_allow_html=True)
