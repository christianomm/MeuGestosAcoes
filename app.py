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
    except: return BLUE_CHIPS

def validar_ticket(tkt):
    if not tkt or str(tkt).strip() == "": return False, "Ticket vazio"
    return bool(re.match(r'^[A-Z]{4}[0-9]{1,2}$', str(tkt).upper().strip())), ""

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
        df_editado.to_sql('operacoes', conn, if_exists='replace', index=False)
        conn.commit()
    finally: conn.close()

def calcular_tudo():
    conn = sqlite3.connect('investimentos.db')
    df = pd.read_sql_query("SELECT * FROM operacoes ORDER BY data ASC, hora ASC", conn)
    conn.close()
    if df.empty: return pd.DataFrame(), pd.DataFrame(), df

    df['data'] = pd.to_datetime(df['data'])
    vendas_realizadas = []
    controle = {} 

    for data_atual in sorted(df['data'].unique()):
        data_dt = pd.to_datetime(data_atual)
        df_dia = df[df['data'] == data_dt].sort_values('hora')
        
        for tkt in df_dia['ticket'].unique():
            if tkt not in controle: controle[tkt] = {'qtd': 0, 'pm': 0.0}
            
            ops_tkt_dia = df_dia[df_dia['ticket'] == tkt]
            q_compra_dia = ops_tkt_dia[ops_tkt_dia['tipo'] == 'Compra']['quantidade'].sum()
            q_venda_dia = ops_tkt_dia[ops_tkt_dia['tipo'] == 'Venda']['quantidade'].sum()
            
            # Hora da primeira venda do dia para registro
            hora_ref = ops_tkt_dia[ops_tkt_dia['tipo'] == 'Venda']['hora'].iloc[0] if q_venda_dia > 0 else "00:00:00"
            
            # --- LÓGICA DAY TRADE ---
            qtd_dt = min(q_compra_dia, q_venda_dia)
            if qtd_dt > 0:
                v_compra_medio = ops_tkt_dia[ops_tkt_dia['tipo'] == 'Compra']['valor'].mean()
                v_venda_medio = ops_tkt_dia[ops_tkt_dia['tipo'] == 'Venda']['valor'].mean()
                vendas_realizadas.append({
                    'Data': data_dt, 'Hora': hora_ref, 'Ticket': tkt, 'Tipo': 'Day Trade',
                    'Qtd': qtd_dt, 'Resultado': (v_venda_medio - v_compra_medio) * qtd_dt,
                    'Volume Venda': qtd_dt * v_venda_medio, 'Mês/Ano': data_dt.strftime('%Y-%m')
                })

            # --- ATUALIZAÇÃO PM ---
            sobra_compra = q_compra_dia - qtd_dt
            if sobra_compra > 0:
                v_compra_medio = ops_tkt_dia[ops_tkt_dia['tipo'] == 'Compra']['valor'].mean()
                total_fin = (controle[tkt]['qtd'] * controle[tkt]['pm']) + (sobra_compra * v_compra_medio)
                controle[tkt]['qtd'] += sobra_compra
                controle[tkt]['pm'] = total_fin / controle[tkt]['qtd']

            # --- SWING TRADE ---
            sobra_venda = q_venda_dia - qtd_dt
            if sobra_venda > 0:
                v_venda_medio = ops_tkt_dia[ops_tkt_dia['tipo'] == 'Venda']['valor'].mean()
                vendas_realizadas.append({
                    'Data': data_dt, 'Hora': hora_ref, 'Ticket': tkt, 'Tipo': 'Swing Trade',
                    'Qtd': sobra_venda, 'Resultado': (v_venda_medio - controle[tkt]['pm']) * sobra_venda,
                    'Volume Venda': sobra_venda * v_venda_medio, 'Mês/Ano': data_dt.strftime('%Y-%m')
                })
                controle[tkt]['qtd'] -= sobra_venda

    return pd.DataFrame([{'Ticket': t, 'Quantidade': d['qtd'], 'Preço Médio': d['pm'], 'Total': d['qtd']*d['pm']} for t, d in controle.items() if d['qtd'] > 0]), pd.DataFrame(vendas_realizadas), df

# --- INTERFACE ---
init_db()
if 'autenticado' not in st.session_state: st.session_state['autenticado'] = False

if not st.session_state['autenticado']:
    st.title("🔐 Login")
    u, p = st.text_input("Usuário"), st.text_input("Senha", type="password")
    if st.button("Entrar"):
        if u == "admin" and p == "1234": st.session_state['autenticado'] = True; st.rerun()
else:
    pag = st.sidebar.radio("Menu", ["Home", "Registrar", "Posição", "Resultados & IR", "Relatório Analítico", "Gestão de Dados"])
    df_pos, df_res, df_raw = calcular_tudo()

    if pag == "Resultados & IR":
        st.header("📊 Performance e Apuração de IR")
        if not df_res.empty:
            # Ordenação segura agora que a coluna 'Hora' existe em df_res
            df_res_sorted = df_res.sort_values(['Data', 'Hora'], ascending=False)
            
            mes_atual = datetime.now().strftime('%Y-%m')
            vendas_st_mes = df_res[df_res['Mês/Ano'] == mes_atual][df_res['Tipo'] == 'Swing Trade']['Volume Venda'].sum()
            
            with st.container(border=True):
                st.subheader(f"Monitor Swing Trade: {datetime.now().strftime('%m/%Y')}")
                st.write(f"Volume: **R$ {vendas_st_mes:,.2f}** / R$ 20.000,00")
                st.progress(min(vendas_st_mes / LIMITE_ISENCAO, 1.0))

            st.divider()
            st.dataframe(df_res_sorted, use_container_width=True, hide_index=True)
        else:
            st.info("Nenhuma venda realizada.")

    elif pag == "Relatório Analítico":
        st.header("📈 Relatório Analítico de Vendas")
        if not df_res.empty:
            tab1, tab2 = st.tabs(["Consolidado por Ticket", "Mensal por Ticket"])
            with tab1:
                consolidado = df_res.groupby('Ticket').agg({'Resultado': 'sum', 'Volume Venda': 'sum'}).reset_index().sort_values(by='Resultado', ascending=False)
                st.dataframe(consolidado, use_container_width=True, hide_index=True)
            with tab2:
                mensal = df_res.groupby(['Mês/Ano', 'Ticket']).agg({'Resultado': 'sum', 'Volume Venda': 'sum'}).reset_index().sort_values(by=['Mês/Ano', 'Resultado'], ascending=[False, False])
                st.dataframe(mensal, use_container_width=True, hide_index=True)
        else: st.info("Sem dados.")

    # ... (Módulos Home, Registrar, Posição e Gestão seguem a mesma lógica do seu código anterior)
    elif pag == "Home":
        st.header("🏠 Painel de Controle")
        montante = df_pos['Total'].sum() if not df_pos.empty else 0.0
        lucro_tot = df_res['Resultado'].sum() if not df_res.empty else 0.0
        c1, c2 = st.columns(2)
        c1.metric("Patrimônio Atual", f"R$ {montante:,.2f}")
        c2.metric("Lucro Total Vendas", f"R$ {lucro_tot:,.2f}")

    elif pag == "Registrar":
        st.header("📝 Registrar Operação")
        with st.form("reg"):
            tkt = st.selectbox("Ticket", get_tickers_da_base())
            tipo = st.selectbox("Tipo", ["Compra", "Venda"])
            q = st.number_input("Qtd", 1)
            v = st.number_input("Preço", 0.0)
            d = st.date_input("Data")
            h = st.time_input("Hora")
            if st.form_submit_button("Salvar"):
                salvar_operacao(d.strftime('%Y-%m-%d'), h.strftime('%H:%M:%S'), tkt, tipo, q, v)
                st.rerun()

    elif pag == "Gestão de Dados":
        st.header("⚙️ Gestão")
        df_edit = st.data_editor(df_raw, num_rows="dynamic", use_container_width=True)
        if st.button("Confirmar Alterações"):
            atualizar_banco_pelo_editor(df_edit, df_raw)
            st.rerun()

    if st.sidebar.button("Sair"): st.session_state['autenticado'] = False; st.rerun()