import streamlit as st
import pandas as pd
import sqlite3
from datetime import datetime

# --- CONFIGURAÇÕES INICIAIS ---
st.set_page_config(page_title="Gestor B3 - Trader Pro", layout="wide")

BLUE_CHIPS = ["PETR4", "VALE3", "ITUB4", "BBDC4", "BBAS3", "ABEV3", "MGLU3"]

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

    df_ops['data'] = pd.to_datetime(df_ops['data'], format='mixed')
    vendas_realizadas = []
    controle = {} 

    for data_atual in sorted(df_ops['data'].unique()):
        data_dt = pd.to_datetime(data_atual)
        df_dia = df_ops[df_ops['data'] == data_dt].sort_values('hora')
        
        for tkt in df_dia['ticket'].unique():
            if tkt not in controle: 
                controle[tkt] = {'qtd': 0, 'pm': 0.0}
            
            ops_dia = df_dia[df_dia['ticket'] == tkt]
            vendas_dia = ops_dia[ops_dia['tipo'] == 'Venda']
            compras_dia = ops_dia[ops_dia['tipo'] == 'Compra']
            
            q_c = compras_dia['quantidade'].sum()
            q_v = vendas_dia['quantidade'].sum()
            hora_venda = vendas_dia['hora'].iloc[0] if not vendas_dia.empty else "00:00:00"
            
            qtd_dt = min(q_c, q_v)
            if qtd_dt > 0:
                v_compra_m = compras_dia['valor'].mean()
                v_venda_m = vendas_dia['valor'].mean()
                vendas_realizadas.append({
                    'Data': data_dt, 'Hora': hora_venda, 'Ticket': tkt, 'Tipo': 'Day Trade', 
                    'Resultado': (v_venda_m - v_compra_m) * qtd_dt, 
                    'Volume Venda': qtd_dt * v_venda_m, 'Mês/Ano': data_dt.strftime('%Y-%m')
                })

            sobra_c = q_c - qtd_dt
            if sobra_c > 0:
                v_compra_m = compras_dia['valor'].mean()
                novo_total = (controle[tkt]['qtd'] * controle[tkt]['pm']) + (sobra_c * v_compra_m)
                controle[tkt]['qtd'] += sobra_c
                controle[tkt]['pm'] = novo_total / controle[tkt]['qtd']

            sobra_v = q_v - qtd_dt
            if sobra_v > 0:
                v_venda_m = vendas_dia['valor'].mean()
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

# --- LOGIN ---
init_db()
if 'autenticado' not in st.session_state: st.session_state['autenticado'] = False

if not st.session_state['autenticado']:
    st.title("🔐 Login")
    u, p = st.text_input("Usuário"), st.text_input("Senha", type="password")
    if st.button("Entrar"):
        if u == "admin" and p == "1234": 
            st.session_state['autenticado'] = True; st.rerun()
else:
    pag = st.sidebar.radio("Menu", ["Home", "Registrar Operação", "Registrar Proventos", "Posição", "Resultados & IR", "Histórico por Ticket", "Relatório Analítico", "Gestão de Dados"])
    df_pos, df_res, df_raw, df_prov = calcular_tudo()

    # --- HISTÓRICO POR TICKET (RESTAURADO) ---
    if pag == "Histórico por Ticket":
        st.header("🔍 Consultar Ativo Específico")
        todos_tkts = sorted(df_raw['ticket'].unique().tolist()) if not df_raw.empty else []
        if todos_tkts:
            escolha = st.selectbox("Selecione o Ticket", todos_tkts)
            t1, t2 = st.tabs(["Operações", "Proventos"])
            with t1:
                st.dataframe(df_raw[df_raw['ticket'] == escolha].sort_values(['data', 'hora'], ascending=False), use_container_width=True, hide_index=True)
            with t2:
                if not df_prov.empty:
                    st.dataframe(df_prov[df_prov['ticket'] == escolha], use_container_width=True, hide_index=True)
                else: st.info("Sem proventos para este ticket.")
        else: st.info("Nenhuma operação registrada.")

    # --- REGISTRAR OPERAÇÃO (RESTAURADO COM HORA E ID) ---
    elif pag == "Registrar Operação":
        st.header("📝 Nova Operação")
        tkts_e = sorted(list(set(df_raw['ticket'].tolist() if not df_raw.empty else [] + BLUE_CHIPS)))
        tkts_e.insert(0, "DIGITAR NOVO...")
        with st.form("f_op", clear_on_submit=True):
            c1, c2, c3 = st.columns(3)
            s1 = c1.selectbox("Ticket", tkts_e)
            s2 = c1.text_input("Novo Ticket").upper().strip()
            tipo = c2.selectbox("Tipo", ["Compra", "Venda"])
            data = c3.date_input("Data", datetime.now())
            c4, c5, c6 = st.columns(3)
            qtd = c4.number_input("Quantidade", min_value=1)
            val = c5.number_input("Preço", min_value=0.01)
            hora = c6.time_input("Hora", datetime.now().time())
            if st.form_submit_button("Salvar Operação"):
                t_f = s2 if s1 == "DIGITAR NOVO..." else s1
                if t_f:
                    conn = sqlite3.connect('investimentos.db')
                    conn.execute("INSERT INTO operacoes (data, ticket, tipo, quantidade, valor, hora) VALUES (?,?,?,?,?,?)",
                                 (data.strftime('%Y-%m-%d'), t_f, tipo, qtd, val, hora.strftime('%H:%M:%S')))
                    conn.commit(); conn.close(); st.success("Salvo!"); st.rerun()

    # --- GESTÃO DE DADOS (PRESERVANDO IDs) ---
    elif pag == "Gestão de Dados":
        st.header("⚙️ Central de Edição")
        st.subheader("Operações")
        ed_ops = st.data_editor(df_raw, use_container_width=True, num_rows="dynamic", key="e1", hide_index=False)
        if st.button("Salvar Alterações em Operações"):
            conn = sqlite3.connect('investimentos.db')
            conn.execute("DELETE FROM operacoes")
            ed_ops.to_sql('operacoes', conn, index=False, if_exists='append')
            conn.commit(); conn.close(); st.success("Atualizado!"); st.rerun()

    # --- MÓDULOS RESTANTES (POSIÇÃO, IR, RELATÓRIO) ---
    elif pag == "Home":
        st.header("🏠 Painel Geral")
        c1, c2, c3 = st.columns(3)
        c1.metric("Patrimônio", f"R$ {df_pos['Total'].sum() if not df_pos.empty else 0:,.2f}")
        c2.metric("Lucro Vendas", f"R$ {df_res['Resultado'].sum() if not df_res.empty else 0:,.2f}")
        c3.metric("Proventos", f"R$ {df_prov['valor'].sum() if not df_prov.empty else 0:,.2f}")

    elif pag == "Posição":
        st.header("🏢 Carteira Atual")
        if not df_pos.empty: st.dataframe(df_pos.style.format({'Preço Médio': 'R$ {:.2f}', 'Total': 'R$ {:.2f}'}), use_container_width=True, hide_index=True)
        else: st.info("Sem posições.")

    elif pag == "Resultados & IR":
        st.header("📊 Resultados")
        if not df_res.empty: st.dataframe(df_res.style.format({'Resultado': 'R$ {:.2f}'}), use_container_width=True, hide_index=True)
        else: st.info("Sem vendas.")

    elif pag == "Relatório Analítico":
        st.header("📈 Relatório por Ativo")
        if not df_res.empty or not df_prov.empty:
            v = df_res.groupby('Ticket')['Resultado'].sum() if not df_res.empty else pd.Series(dtype=float)
            p = df_prov.groupby('ticket')['valor'].sum() if not df_prov.empty else pd.Series(dtype=float)
            df_an = pd.concat([v, p], axis=1).fillna(0)
            df_an.columns = ['Ganhos Capital', 'Proventos']
            st.dataframe(df_an.style.format('R$ {:.2f}'), use_container_width=True)

    if st.sidebar.button("Sair"): 
        st.session_state['autenticado'] = False; st.rerun()