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
    c.execute('''CREATE TABLE IF NOT EXISTS proventos
                 (id INTEGER PRIMARY KEY AUTOINCREMENT,
                  data TEXT, ticket TEXT, tipo TEXT, valor REAL)''')
    conn.commit()
    conn.close()

def calcular_tudo():
    conn = sqlite3.connect('investimentos.db')
    df_ops = pd.read_sql_query("SELECT * FROM operacoes ORDER BY data ASC, hora ASC", conn)
    df_prov = pd.read_sql_query("SELECT * FROM proventos ORDER BY data ASC", conn)
    conn.close()
    
    if df_ops.empty: 
        return pd.DataFrame(), pd.DataFrame(), df_ops, df_prov

    df_ops['data'] = pd.to_datetime(df_ops['data'])
    vendas_realizadas = []
    controle = {} 

    for data_atual in sorted(df_ops['data'].unique()):
        data_dt = pd.to_datetime(data_atual)
        df_dia = df_ops[df_ops['data'] == data_dt].sort_values('hora')
        
        for tkt in df_dia['ticket'].unique():
            if tkt not in controle: 
                controle[tkt] = {'qtd': 0, 'pm': 0.0}
            
            ops_dia = df_dia[df_dia['ticket'] == tkt]
            q_c = ops_dia[ops_dia['tipo'] == 'Compra']['quantidade'].sum()
            q_v = ops_dia[ops_dia['tipo'] == 'Venda']['quantidade'].sum()
            hora_venda = ops_dia[ops_dia['tipo'] == 'Venda']['hora'].iloc[0] if q_v > 0 else "00:00:00"
            
            # 1. Day Trade
            qtd_dt = min(q_c, q_v)
            if qtd_dt > 0:
                v_compra_m = ops_dia[ops_dia['tipo'] == 'Compra']['valor'].mean()
                v_venda_m = ops_dia[ops_dia['tipo'] == 'Venda']['valor'].mean()
                vendas_realizadas.append({
                    'Data': data_dt, 'Hora': hora_venda, 'Ticket': tkt, 'Tipo': 'Day Trade', 
                    'Resultado': (v_venda_m - v_compra_m) * qtd_dt, 
                    'Volume Venda': qtd_dt * v_venda_m, 'Mês/Ano': data_dt.strftime('%Y-%m')
                })

            # 2. Atualizar Preço Médio
            sobra_c = q_c - qtd_dt
            if sobra_c > 0:
                v_compra_m = ops_dia[ops_dia['tipo'] == 'Compra']['valor'].mean()
                novo_total = (controle[tkt]['qtd'] * controle[tkt]['pm']) + (sobra_c * v_compra_m)
                controle[tkt]['qtd'] += sobra_c
                controle[tkt]['pm'] = novo_total / controle[tkt]['qtd']

            # 3. Swing Trade
            sobra_v = q_v - qtd_dt
            if sobra_v > 0:
                v_venda_m = ops_dia[ops_dia['tipo'] == 'Venda']['valor'].mean()
                vendas_realizadas.append({
                    'Data': data_dt, 'Hora': hora_venda, 'Ticket': tkt, 'Tipo': 'Swing Trade', 
                    'Resultado': (v_venda_m - controle[tkt]['pm']) * sobra_v, 
                    'Volume Venda': sobra_v * v_venda_m, 'Mês/Ano': data_dt.strftime('%Y-%m')
                })
                controle[tkt]['qtd'] -= sobra_v

    df_pos = pd.DataFrame([{'Ticket': t, 'Quantidade': d['qtd'], 'Preço Médio': d['pm'], 'Total': d['qtd']*d['pm']} 
                           for t, d in controle.items() if d['qtd'] > 0])
    df_res = pd.DataFrame(vendas_realizadas)
    
    return df_pos, df_res, df_ops, df_prov

# --- INTERFACE ---
init_db()
if 'autenticado' not in st.session_state: st.session_state['autenticado'] = False

if not st.session_state['autenticado']:
    st.title("🔐 Login")
    u, p = st.text_input("Usuário"), st.text_input("Senha", type="password")
    if st.button("Entrar"):
        if u == "admin" and p == "1234": st.session_state['autenticado'] = True; st.rerun()
else:
    pag = st.sidebar.radio("Menu", ["Home", "Registrar Operação", "Registrar Proventos", "Posição", "Resultados & IR", "Relatório Analítico", "Gestão de Dados"])
    df_pos, df_res, df_raw, df_prov = calcular_tudo()

    # --- MÓDULO: POSIÇÃO (CORRIGIDO) ---
    if pag == "Posição":
        st.header("🏢 Carteira Atual (Ações em Custódia)")
        if not df_pos.empty:
            st.dataframe(df_pos.style.format({'Preço Médio': 'R$ {:.2f}', 'Total': 'R$ {:.2f}'}), 
                         use_container_width=True, hide_index=True)
        else:
            st.info("Nenhuma ação em custódia no momento.")

    # --- MÓDULO: REGISTRAR PROVENTOS (CORRIGIDO) ---
    elif pag == "Registrar Proventos":
        st.header("💰 Registrar Dividendos / JCP")
        # Busca tickets das operações para facilitar a seleção
        lista_tickets = sorted(list(set(df_raw['ticket'].tolist() + BLUE_CHIPS))) if not df_raw.empty else BLUE_CHIPS
        
        with st.form("form_prov", clear_on_submit=True):
            tkt = st.selectbox("Ticket", lista_tickets)
            tipo_p = st.selectbox("Tipo", ["Dividendo", "JCP"])
            val_p = st.number_input("Valor Líquido Recebido", min_value=0.01, step=0.01)
            data_p = st.date_input("Data do Pagamento")
            
            if st.form_submit_button("Salvar Provento"):
                conn = sqlite3.connect('investimentos.db')
                conn.execute("INSERT INTO proventos (data, ticket, tipo, valor) VALUES (?,?,?,?)", 
                             (data_p.strftime('%Y-%m-%d'), tkt, tipo_p, val_p))
                conn.commit()
                conn.close()
                st.success(f"Provento de {tkt} registrado!")
                st.rerun()

    # --- MÓDULO: RESULTADOS & IR (REVISADO) ---
    elif pag == "Resultados & IR":
        st.header("📊 Performance e Tributação")
        if not df_res.empty:
            mes_at = datetime.now().strftime('%Y-%m')
            vendas_st = df_res[(df_res['Mês/Ano'] == mes_at) & (df_res['Tipo'] == 'Swing Trade')]['Volume Venda'].sum()
            
            st.metric("Volume Venda Swing Trade (Mês Atual)", f"R$ {vendas_st:,.2f}", 
                      delta=f"{LIMITE_ISENCAO - vendas_st:,.2f} p/ Isenção", delta_color="normal")
            
            df_res_view = df_res.sort_values(['Data', 'Hora'], ascending=False).copy()
            df_res_view['Data'] = df_res_view['Data'].dt.strftime('%d/%m/%Y')
            st.dataframe(df_res_view.style.format({'Resultado': 'R$ {:.2f}', 'Volume Venda': 'R$ {:.2f}'}), 
                         use_container_width=True, hide_index=True)
        else:
            st.info("Sem histórico de vendas.")

    elif pag == "Home":
        st.header("🏠 Painel Geral")
        c1, c2, c3 = st.columns(3)
        c1.metric("Patrimônio Atual", f"R$ {df_pos['Total'].sum() if not df_pos.empty else 0:,.2f}")
        c2.metric("Lucro Vendas", f"R$ {df_res['Resultado'].sum() if not df_res.empty else 0:,.2f}")
        c3.metric("Total Proventos", f"R$ {df_prov['valor'].sum() if not df_prov.empty else 0:,.2f}")

    elif pag == "Registrar Operação":
        st.header("📝 Nova Operação")
        with st.form("f_op", clear_on_submit=True):
            c1, c2, c3 = st.columns(3)
            tkt = c1.text_input("Ticket").upper().strip()
            tipo = c2.selectbox("Tipo", ["Compra", "Venda"])
            data = c3.date_input("Data")
            c4, c5, c6 = st.columns(3)
            qtd = c4.number_input("Quantidade", min_value=1)
            val = c5.number_input("Preço Unitário", min_value=0.01)
            hora = c6.time_input("Hora")
            if st.form_submit_button("Salvar Operação"):
                if len(tkt) < 5: st.error("Ticket inválido"); st.stop()
                conn = sqlite3.connect('investimentos.db')
                conn.execute("INSERT INTO operacoes (data, ticket, tipo, quantidade, valor, hora) VALUES (?,?,?,?,?,?)",
                             (data.strftime('%Y-%m-%d'), tkt, tipo, qtd, val, hora.strftime('%H:%M:%S')))
                conn.commit(); conn.close()
                st.rerun()

    elif pag == "Relatório Analítico":
        st.header("📈 Desempenho Consolidado por Ativo")
        if not df_res.empty or not df_prov.empty:
            res_v = df_res.groupby('Ticket')['Resultado'].sum() if not df_res.empty else pd.Series(dtype=float)
            res_p = df_prov.groupby('ticket')['valor'].sum() if not df_prov.empty else pd.Series(dtype=float)
            analise = pd.concat([res_v, res_p], axis=1).fillna(0)
            analise.columns = ['Lucro Vendas', 'Proventos']
            analise['Total'] = analise['Lucro Vendas'] + analise['Proventos']
            st.dataframe(analise.sort_values('Total', ascending=False).style.format('R$ {:.2f}'), use_container_width=True)

    elif pag == "Gestão de Dados":
        st.header("⚙️ Editor do Banco de Dados")
        st.subheader("Operações de Compra/Venda")
        st.data_editor(df_raw, key="ed_ops", use_container_width=True, hide_index=True)
        st.subheader("Registros de Proventos")
        st.data_editor(df_prov, key="ed_prov", use_container_width=True, hide_index=True)

    if st.sidebar.button("Sair"): 
        st.session_state['autenticado'] = False; st.rerun()