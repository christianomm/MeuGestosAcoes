import streamlit as st
import pandas as pd
import sqlite3
from datetime import datetime
import plotly.express as px
import re
import io

# --- CONFIGURAÇÕES INICIAIS ---
st.set_page_config(page_title="Gestor B3 - Trader Pro", layout="wide")

BLUE_CHIPS = ["PETR4", "VALE3", "ITUB4", "BBDC4", "BBAS3", "ABEV3", "MGLU3"]

def init_db():
    conn = sqlite3.connect('investimentos.db')
    c = conn.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS operacoes
                 (id INTEGER PRIMARY KEY AUTOINCREMENT,
                  data TEXT, ticket TEXT, tipo TEXT, 
                  quantidade INTEGER, valor REAL)''')
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

def salvar_operacao(data, ticket, tipo, qtd, valor):
    conn = sqlite3.connect('investimentos.db')
    c = conn.cursor()
    c.execute("INSERT INTO operacoes (data, ticket, tipo, quantidade, valor) VALUES (?,?,?,?,?)",
              (data, ticket.upper().strip(), tipo, qtd, valor))
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
            ok, _ = validar_ticket(row['ticket'])
            if ok:
                data_str = pd.to_datetime(row['data']).strftime('%Y-%m-%d')
                if pd.notna(row['id']):
                    conn.execute('''UPDATE operacoes SET data=?, ticket=?, tipo=?, quantidade=?, valor=? 
                                    WHERE id=?''', (data_str, str(row['ticket']).upper().strip(), 
                                                   row['tipo'], int(row['quantidade']), float(row['valor']), int(row['id'])))
                else:
                    conn.execute('''INSERT INTO operacoes (data, ticket, tipo, quantidade, valor) 
                                    VALUES (?,?,?,?,?)''', (data_str, str(row['ticket']).upper().strip(), 
                                                           row['tipo'], int(row['quantidade']), float(row['valor'])))
        conn.commit()
    finally:
        conn.close()

def calcular_tudo():
    conn = sqlite3.connect('investimentos.db')
    df = pd.read_sql_query("SELECT * FROM operacoes ORDER BY data ASC", conn)
    conn.close()
    if df.empty: return pd.DataFrame(), pd.DataFrame(), df

    df['data'] = pd.to_datetime(df['data'])
    vendas_realizadas = []
    controle = {} 

    for data in sorted(df['data'].dt.date.unique()):
        df_dia = df[df['data'].dt.date == data]
        for tkt in df_dia['ticket'].unique():
            if tkt not in controle: controle[tkt] = {'qtd': 0, 'pm': 0.0}
            ops = df_dia[df_dia['ticket'] == tkt]
            qc = ops[ops['tipo']=='Compra']['quantidade'].sum()
            qv = ops[ops['tipo']=='Venda']['quantidade'].sum()
            q_dt = min(qc, qv)
            
            if q_dt > 0:
                v_c, v_v = ops[ops['tipo']=='Compra']['valor'].mean(), ops[ops['tipo']=='Venda']['valor'].mean()
                vendas_realizadas.append({'Data': data, 'Ticket': tkt, 'Qtd': q_dt, 'Tipo': 'Day Trade', 'Resultado': (v_v - v_c)*q_dt, 'Volume Venda': q_dt*v_v})

            sqc, sqv = qc - q_dt, qv - q_dt
            if sqc > 0:
                v_compra_m = ops[ops['tipo']=='Compra']['valor'].mean()
                total_financeiro = (controle[tkt]['qtd'] * controle[tkt]['pm']) + (sqc * v_compra_m)
                controle[tkt]['qtd'] += sqc
                controle[tkt]['pm'] = total_financeiro / controle[tkt]['qtd']
            if sqv > 0:
                v_venda_m = ops[ops['tipo']=='Venda']['valor'].mean()
                lucro = (v_venda_m - controle[tkt]['pm']) * sqv
                vendas_realizadas.append({'Data': data, 'Ticket': tkt, 'Qtd': sqv, 'Tipo': 'Swing Trade', 'Resultado': lucro, 'Volume Venda': sqv * v_venda_m})
                controle[tkt]['qtd'] -= sqv

    df_res = pd.DataFrame(vendas_realizadas)
    if not df_res.empty:
        df_res['Data'] = pd.to_datetime(df_res['Data'])
        df_res['Mês/Ano'] = df_res['Data'].dt.to_period('M').astype(str)
    
    df_pos = pd.DataFrame([{'Ticket': t, 'Quantidade': d['qtd'], 'Preço Médio': d['pm'], 'Total': d['qtd']*d['pm']} for t, d in controle.items() if d['qtd'] > 0])
    return df_pos, df_res, df

@st.dialog("Sucesso")
def modal_sucesso(msg):
    st.success(msg)
    if st.button("OK"): st.rerun()

# --- INTERFACE ---
init_db()
if 'autenticado' not in st.session_state: st.session_state['autenticado'] = False

if not st.session_state['autenticado']:
    st.title("🔐 Login")
    u, p = st.text_input("Usuário"), st.text_input("Senha", type="password")
    if st.button("Entrar"):
        if u == "admin" and p == "1234": st.session_state['autenticado'] = True; st.rerun()
else:
    # --- NOVO MENU HOME ---
    pag = st.sidebar.radio("Menu", ["Home", "Registrar", "Posição", "Resultados & IR", "Gestão de Dados"])
    df_pos, df_res, df_raw = calcular_tudo()

    if pag == "Home":
        st.header("🏠 Painel de Controle")
        
        # Cálculos de indicadores
        montante_aplicado = df_pos['Total'].sum() if not df_pos.empty else 0.0
        lucro_acumulado = df_res['Resultado'].sum() if not df_res.empty else 0.0
        
        # Lucro do mês corrente
        mes_atual = datetime.now().strftime('%Y-%m')
        if not df_res.empty:
            lucro_mes = df_res[df_res['Mês/Ano'] == mes_atual]['Resultado'].sum()
        else:
            lucro_mes = 0.0

        # Layout de Cards
        c1, c2, c3 = st.columns(3)
        c1.metric("Montante Aplicado (Custo)", f"R$ {montante_aplicado:,.2f}")
        c2.metric("Lucro Acumulado Histórico", f"R$ {lucro_acumulado:,.2f}", 
                  delta=f"{((lucro_acumulado/montante_aplicado)*100 if montante_aplicado > 0 else 0):.2f}%")
        c3.metric(f"Lucro em {datetime.now().strftime('%B/%Y')}", f"R$ {lucro_mes:,.2f}")

        st.divider()
        
        # Gráficos Rápidos na Home
        col_graf1, col_graf2 = st.columns(2)
        if not df_res.empty:
            res_mensal = df_res.groupby('Mês/Ano')['Resultado'].sum().reset_index()
            fig_evol = px.bar(res_mensal, x='Mês/Ano', y='Resultado', title="Evolução Mensal de Lucros/Prejuízos",
                              color='Resultado', color_continuous_scale=['red', 'green'])
            col_graf1.plotly_chart(fig_evol, use_container_width=True)
        
        if not df_pos.empty:
            fig_pos = px.pie(df_pos, values='Total', names='Ticket', title="Distribuição de Patrimônio")
            col_graf2.plotly_chart(fig_pos, use_container_width=True)
        else:
            st.info("Adicione operações para visualizar os gráficos.")

    elif pag == "Registrar":
        # ... (Mantido exatamente como na versão anterior)
        st.header("📝 Registrar Operação")
        tab_sugestao, tab_manual = st.tabs(["🔎 Rápido", "⌨️ Novo Ativo"])
        with tab_sugestao:
            with st.form("f1", clear_on_submit=True):
                tkt = st.selectbox("Ticket", [""] + get_tickers_da_base())
                tipo = st.selectbox("Tipo", ["Compra", "Venda"])
                q, v = st.number_input("Qtd", 1), st.number_input("Preço", 0.0)
                d = st.date_input("Data", datetime.now())
                if st.form_submit_button("Salvar"):
                    if validar_ticket(tkt)[0]:
                        salvar_operacao(d.strftime('%Y-%m-%d'), tkt, tipo, q, v)
                        modal_sucesso(f"{tkt} Registrado!")
        with tab_manual:
            with st.form("f2", clear_on_submit=True):
                tkt = st.text_input("Novo Ticket")
                tipo = st.selectbox("Tipo", ["Compra", "Venda"])
                q, v = st.number_input("Qtd", 1), st.number_input("Preço", 0.0)
                d = st.date_input("Data", datetime.now())
                if st.form_submit_button("Cadastrar"):
                    ok, msg = validar_ticket(tkt)
                    if ok:
                        salvar_operacao(d.strftime('%Y-%m-%d'), tkt, tipo, q, v)
                        modal_sucesso(f"{tkt} Cadastrado!")
                    else: st.error(msg)

    elif pag == "Posição":
        st.header("🏢 Carteira Atual")
        if not df_pos.empty:
            st.dataframe(df_pos.style.format({'Preço Médio': 'R$ {:.2f}', 'Total': 'R$ {:.2f}'}), use_container_width=True, hide_index=True)
        else: st.info("Sua carteira está vazia.")

    elif pag == "Resultados & IR":
        st.header("📊 Performance e Detalhamento")
        if not df_res.empty:
            res_m = df_res.groupby(['Mês/Ano', 'Tipo']).agg({'Resultado': 'sum', 'Volume Venda': 'sum'}).reset_index()
            for mes in sorted(res_m['Mês/Ano'].unique(), reverse=True):
                with st.expander(f"📅 Relatório Mensal: {mes}"):
                    d_mes = res_m[res_m['Mês/Ano'] == mes]
                    v_st = d_mes[d_mes['Tipo']=='Swing Trade']['Volume Venda'].sum()
                    l_st = d_mes[d_mes['Tipo']=='Swing Trade']['Resultado'].sum()
                    l_dt = d_mes[d_mes['Tipo']=='Day Trade']['Resultado'].sum()
                    ir_st = (l_st * 0.15) if (v_st > 20000 and l_st > 0) else 0
                    ir_dt = max(0, l_dt * 0.20)
                    st.write(f"**Volume ST:** R$ {v_st:,.2f} | **Lucro Total:** R$ {l_st+l_dt:,.2f} | **DARF:** R$ {ir_st+ir_dt:,.2f}")
                    detalhe = df_res[df_res['Mês/Ano'] == mes].copy()
                    st.dataframe(detalhe[['Data', 'Ticket', 'Tipo', 'Qtd', 'Resultado']], use_container_width=True, hide_index=True)
        else: st.info("Sem vendas realizadas.")

    elif pag == "Gestão de Dados":
        st.header("⚙️ Gestão de Dados")
        df_edit = st.data_editor(df_raw, num_rows="dynamic", use_container_width=True)
        if st.button("Confirmar Alterações"):
            atualizar_banco_pelo_editor(df_edit, df_raw)
            st.rerun()

    if st.sidebar.button("Sair"): 
        st.session_state['autenticado'] = False
        st.rerun()