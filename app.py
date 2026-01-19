import streamlit as st
import pandas as pd
import sqlite3
from datetime import datetime
import plotly.express as px
import re

# --- CONFIGURAÇÕES INICIAIS ---
st.set_page_config(page_title="Gestor B3 - Trader Pro", layout="wide")

BLUE_CHIPS = ["PETR4", "VALE3", "ITUB4", "BBDC4", "BBAS3", "ABEV3", "MGLU3"]
LIMITE_ISENCAO = 20000.0

# --- FUNÇÕES DE BANCO DE DADOS ---
def init_db():
    conn = sqlite3.connect('investimentos.db')
    c = conn.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS operacoes
                 (id INTEGER PRIMARY KEY AUTOINCREMENT,
                  data TEXT, ticket TEXT, tipo TEXT, 
                  quantidade INTEGER, valor REAL, hora TEXT DEFAULT '00:00:00')''')
    
    c.execute("PRAGMA table_info(operacoes)")
    colunas = [info[1] for info in c.fetchall()]
    if 'hora' not in colunas:
        c.execute("ALTER TABLE operacoes ADD COLUMN hora TEXT DEFAULT '00:00:00'")
    
    conn.commit()
    conn.close()

def get_tickers_da_base():
    try:
        conn = sqlite3.connect('investimentos.db')
        df = pd.read_sql_query("SELECT DISTINCT ticket FROM operacoes", conn)
        conn.close()
        return sorted(list(set(df['ticket'].tolist() + BLUE_CHIPS)))
    except:
        return BLUE_CHIPS

def validar_ticket(tkt):
    if not tkt or str(tkt).strip() == "":
        return False, "O campo Ticket não pode estar vazio."
    tkt_limpo = str(tkt).upper().strip()
    padrao = r'^[A-Z]{4}[0-9]{1,2}$'
    if not re.match(padrao, tkt_limpo):
        return False, f"Ticket '{tkt_limpo}' inválido."
    return True, ""

def salvar_operacao(data, hora, ticket, tipo, qtd, valor):
    conn = sqlite3.connect('investimentos.db')
    c = conn.cursor()
    c.execute("INSERT INTO operacoes (data, hora, ticket, tipo, quantidade, valor) VALUES (?,?,?,?,?,?)",
              (data, hora, ticket.upper().strip(), tipo, qtd, valor))
    conn.commit()
    conn.close()

def atualizar_banco_pelo_editor(df_editado, df_original):
    conn = sqlite3.connect('investimentos.db')
    try:
        ids_originais = set(df_original['id'].tolist())
        ids_atuais = set(df_editado['id'].dropna().tolist())
        for id_del in (ids_originais - ids_atuais):
            conn.execute("DELETE FROM operacoes WHERE id = ?", (int(id_del),))
        for _, row in df_editado.iterrows():
            if validar_ticket(row['ticket'])[0]:
                data_str = pd.to_datetime(row['data']).strftime('%Y-%m-%d')
                hora_str = str(row['hora'])
                if pd.notna(row['id']):
                    conn.execute('''UPDATE operacoes SET data=?, hora=?, ticket=?, tipo=?, quantidade=?, valor=? 
                                    WHERE id=?''', (data_str, hora_str, str(row['ticket']).upper().strip(), 
                                                   row['tipo'], int(row['quantidade']), float(row['valor']), int(row['id'])))
                else:
                    conn.execute('''INSERT INTO operacoes (data, hora, ticket, tipo, quantidade, valor) 
                                    VALUES (?,?,?,?,?,?)''', (data_str, hora_str, str(row['ticket']).upper().strip(), 
                                                           row['tipo'], int(row['quantidade']), float(row['valor'])))
        conn.commit()
    finally:
        conn.close()

def calcular_tudo():
    conn = sqlite3.connect('investimentos.db')
    df = pd.read_sql_query("SELECT * FROM operacoes ORDER BY data ASC, hora ASC", conn)
    conn.close()
    if df.empty: return pd.DataFrame(), pd.DataFrame(), df

    df['data'] = pd.to_datetime(df['data'])
    vendas_realizadas = []
    controle = {} 

    # Processamento por data para capturar Day Trade
    for data in sorted(df['data'].dt.date.unique()):
        df_dia = df[df['data'].dt.date == data].sort_values('hora')
        
        for tkt in df_dia['ticket'].unique():
            if tkt not in controle: controle[tkt] = {'qtd': 0, 'pm': 0.0}
            
            ops_tkt_dia = df_dia[df_dia['ticket'] == tkt]
            q_compra_dia = ops_tkt_dia[ops_tkt_dia['tipo'] == 'Compra']['quantidade'].sum()
            q_venda_dia = ops_tkt_dia[ops_tkt_dia['tipo'] == 'Venda']['quantidade'].sum()
            
            # --- LÓGICA DAY TRADE ---
            qtd_dt = min(q_compra_dia, q_venda_dia)
            if qtd_dt > 0:
                v_compra_medio = ops_tkt_dia[ops_tkt_dia['tipo'] == 'Compra']['valor'].mean()
                v_venda_medio = ops_tkt_dia[ops_tkt_dia['tipo'] == 'Venda']['valor'].mean()
                vendas_realizadas.append({
                    'Data': pd.Timestamp(data), 'Ticket': tkt, 'Tipo': 'Day Trade',
                    'Qtd': qtd_dt, 'Resultado': (v_venda_medio - v_compra_medio) * qtd_dt,
                    'Volume Venda': qtd_dt * v_venda_medio, 'Mês/Ano': data.strftime('%Y-%m')
                })

            # --- ATUALIZAÇÃO DE PREÇO MÉDIO (COMPRAS QUE NÃO FORAM DAY TRADE) ---
            sobra_compra = q_compra_dia - qtd_dt
            if sobra_compra > 0:
                v_compra_medio = ops_tkt_dia[ops_tkt_dia['tipo'] == 'Compra']['valor'].mean()
                total_fin = (controle[tkt]['qtd'] * controle[tkt]['pm']) + (sobra_compra * v_compra_medio)
                controle[tkt]['qtd'] += sobra_compra
                controle[tkt]['pm'] = total_fin / controle[tkt]['qtd']

            # --- LÓGICA SWING TRADE (VENDAS QUE NÃO FORAM DAY TRADE) ---
            sobra_venda = q_venda_dia - qtd_dt
            if sobra_venda > 0:
                v_venda_medio = ops_tkt_dia[ops_tkt_dia['tipo'] == 'Venda']['valor'].mean()
                resultado_st = (v_venda_medio - controle[tkt]['pm']) * sobra_venda
                vendas_realizadas.append({
                    'Data': pd.Timestamp(data), 'Ticket': tkt, 'Tipo': 'Swing Trade',
                    'Qtd': sobra_venda, 'Resultado': resultado_st,
                    'Volume Venda': sobra_venda * v_venda_medio, 'Mês/Ano': data.strftime('%Y-%m')
                })
                controle[tkt]['qtd'] -= sobra_venda

    df_res = pd.DataFrame(vendas_realizadas)
    df_pos = pd.DataFrame([{'Ticket': t, 'Quantidade': d['qtd'], 'Preço Médio': d['pm'], 'Total': d['qtd']*d['pm']} 
                           for t, d in controle.items() if d['qtd'] > 0])
    return df_pos, df_res, df

@st.dialog("Sucesso")
def modal_sucesso(msg):
    st.success(msg)
    if st.button("OK"): st.rerun()

# --- LÓGICA DE INTERFACE ---
init_db()
if 'autenticado' not in st.session_state: st.session_state['autenticado'] = False

if not st.session_state['autenticado']:
    st.title("🔐 Login")
    u, p = st.text_input("Usuário"), st.text_input("Senha", type="password")
    if st.button("Entrar"):
        if u == "admin" and p == "1234": st.session_state['autenticado'] = True; st.rerun()
else:
    pag = st.sidebar.radio("Menu", ["Home", "Registrar", "Posição", "Resultados & IR", "Gestão de Dados"])
    df_pos, df_res, df_raw = calcular_tudo()

    if pag == "Home":
        st.header("🏠 Painel de Controle")
        montante = df_pos['Total'].sum() if not df_pos.empty else 0.0
        lucro_tot = df_res['Resultado'].sum() if not df_res.empty else 0.0
        mes_atual = datetime.now().strftime('%Y-%m')
        lucro_mes = df_res[df_res['Mês/Ano'] == mes_atual]['Resultado'].sum() if not df_res.empty else 0.0
        
        c1, c2, c3 = st.columns(3)
        c1.metric("Montante Aplicado", f"R$ {montante:,.2f}")
        c2.metric("Lucro Acumulado Histórico", f"R$ {lucro_tot:,.2f}")
        c3.metric(f"Lucro em {datetime.now().strftime('%m/%Y')}", f"R$ {lucro_mes:,.2f}")
        
        st.divider()
        if not df_res.empty:
            res_mensal = df_res.groupby('Mês/Ano')['Resultado'].sum().reset_index()
            fig = px.bar(res_mensal, x='Mês/Ano', y='Resultado', title="Evolução de Lucros", color='Resultado', color_continuous_scale=['red', 'green'])
            st.plotly_chart(fig, use_container_width=True)

    elif pag == "Registrar":
        st.header("📝 Registrar Operação")
        hora_sugerida = datetime.now().time()
        with st.form("reg_form", clear_on_submit=True):
            c1, c2, c3 = st.columns([2,1,1])
            tkt = c1.selectbox("Ticket", [""] + get_tickers_da_base())
            tipo = c2.selectbox("Tipo", ["Compra", "Venda"])
            d = c3.date_input("Data", datetime.now())
            c4, c5, c6 = st.columns(3)
            h = c4.time_input("Hora", hora_sugerida)
            q = c5.number_input("Quantidade", min_value=1)
            v = c6.number_input("Preço", min_value=0.0)
            if st.form_submit_button("Salvar Registro"):
                ok, msg = validar_ticket(tkt)
                if ok:
                    salvar_operacao(d.strftime('%Y-%m-%d'), h.strftime('%H:%M:%S'), tkt, tipo, q, v)
                    modal_sucesso(f"Operação em {tkt} salva!")
                else: st.error(msg)

    elif pag == "Posição":
        st.header("🏢 Carteira Atual")
        if not df_pos.empty:
            st.dataframe(df_pos.style.format({'Preço Médio': 'R$ {:.2f}', 'Total': 'R$ {:.2f}'}), use_container_width=True, hide_index=True)
        else: st.info("Nenhuma posição aberta.")

    elif pag == "Resultados & IR":
        st.header("📊 Performance e Apuração de IR")
        if not df_res.empty:
            # Monitor de Isenção Mensal (Apenas para Swing Trade)
            mes_atual = datetime.now().strftime('%Y-%m')
            df_mes_atual = df_res[df_res['Mês/Ano'] == mes_atual]
            vendas_st_mes = df_mes_atual[df_mes_atual['Tipo'] == 'Swing Trade']['Volume Venda'].sum()
            
            progresso = min(vendas_st_mes / LIMITE_ISENCAO, 1.0)
            
            with st.container(border=True):
                st.subheader(f"Monitor de Isenção (Swing Trade): {datetime.now().strftime('%m/%Y')}")
                st.write(f"Volume Vendido ST: **R$ {vendas_st_mes:,.2f}** / R$ 20.000,00")
                if vendas_st_mes >= LIMITE_ISENCAO:
                    st.error("🚨 Limite de R$ 20k ultrapassado! Lucros de Swing Trade serão tributados (15%).")
                elif vendas_st_mes >= 15000:
                    st.warning("⚠️ Atenção! Você está se aproximando do limite de isenção.")
                st.progress(progresso)

            st.divider()

            # Tabela detalhada com identificação de tipo
            st.subheader("Histórico de Vendas e Tributação")
            
            # Formatação para exibição
            df_display = df_res.copy()
            df_display['Data'] = df_display['Data'].dt.strftime('%d/%m/%Y')
            
            # Lógica de imposto simplificada para visualização
            def calc_ir(row):
                if row['Tipo'] == 'Day Trade':
                    return row['Resultado'] * 0.20 if row['Resultado'] > 0 else 0
                else: # Swing Trade
                    vendas_st_total_mes = df_res[(df_res['Mês/Ano'] == row['Mês/Ano']) & (df_res['Tipo'] == 'Swing Trade')]['Volume Venda'].sum()
                    if vendas_st_total_mes > LIMITE_ISENCAO and row['Resultado'] > 0:
                        return row['Resultado'] * 0.15
                    return 0

            df_display['Imposto Est.'] = df_display.apply(calc_ir, axis=1)
            
            st.dataframe(
                df_display.sort_values(['Mês/Ano', 'Data'], ascending=False).style.format({
                    'Resultado': 'R$ {:.2f}', 
                    'Volume Venda': 'R$ {:.2f}',
                    'Imposto Est.': 'R$ {:.2f}'
                }), 
                use_container_width=True, 
                hide_index=True
            )
        else: st.info("Nenhuma venda realizada.")

    elif pag == "Gestão de Dados":
        st.header("⚙️ Gestão de Dados")
        df_edit = st.data_editor(df_raw, num_rows="dynamic", use_container_width=True)
        if st.button("Confirmar Alterações"):
            atualizar_banco_pelo_editor(df_edit, df_raw)
            st.rerun()

    if st.sidebar.button("Sair"): 
        st.session_state['autenticado'] = False
        st.rerun()