import streamlit as st
import pandas as pd
import sqlite3
from datetime import datetime
import shutil
import os
import io
import hashlib
from pathlib import Path

# Verificar e instalar dependências opcionais
try:
    import plotly.graph_objects as go
    import plotly.express as px
    PLOTLY_AVAILABLE = True
except ImportError:
    PLOTLY_AVAILABLE = False

# --- CONFIGURAÇÕES INICIAIS ---
st.set_page_config(
    page_title="Gestor B3 - Trader Pro", 
    layout="wide",
    initial_sidebar_state="expanded"
)

BLUE_CHIPS = ["PETR4", "VALE3", "ITUB4", "BBDC4", "BBAS3", "ABEV3", "MGLU3"]
FIIS = ["HGLG11", "KNRI11", "MXRF11", "VISC11", "XPML11", "BTLG11"]
SENHA_HASH = hashlib.sha256("1234".encode()).hexdigest()

# --- MIGRAÇÃO DE BANCO DE DADOS ---
def migrar_banco():
    """Adiciona colunas novas se não existirem."""
    conn = sqlite3.connect('investimentos.db')
    c = conn.cursor()
    
    # Verificar se colunas existem
    c.execute("PRAGMA table_info(operacoes)")
    colunas_existentes = [col[1] for col in c.fetchall()]
    
    # Adicionar taxa_corretagem se não existir
    if 'taxa_corretagem' not in colunas_existentes:
        try:
            c.execute("ALTER TABLE operacoes ADD COLUMN taxa_corretagem REAL DEFAULT 0")
            conn.commit()
            print("✅ Coluna taxa_corretagem adicionada")
        except Exception as e:
            print(f"Aviso: {e}")
    
    # Adicionar taxa_emolumentos se não existir
    if 'taxa_emolumentos' not in colunas_existentes:
        try:
            c.execute("ALTER TABLE operacoes ADD COLUMN taxa_emolumentos REAL DEFAULT 0")
            conn.commit()
            print("✅ Coluna taxa_emolumentos adicionada")
        except Exception as e:
            print(f"Aviso: {e}")
    
    conn.close()

# --- FUNÇÕES DE VALIDAÇÃO ---
def validar_operacao(ticket, tipo, quantidade, valor, data):
    """Valida dados antes de salvar operação."""
    erros = []
    
    if not ticket or len(ticket) < 4:
        erros.append("❌ Ticket inválido (mínimo 4 caracteres)")
    
    if tipo not in ['Compra', 'Venda']:
        erros.append("❌ Tipo deve ser Compra ou Venda")
    
    if quantidade <= 0:
        erros.append("❌ Quantidade deve ser positiva")
    
    if valor <= 0:
        erros.append("❌ Preço deve ser positivo")
    
    if data > datetime.now().date():
        erros.append("❌ Data não pode ser futura")
    
    return erros

def identificar_tipo_ativo(ticket):
    """Identifica se é ação ou FII."""
    if ticket.upper().endswith('11'):
        return 'FII'
    return 'ACAO'

def verificar_venda_descoberto(ticket, quantidade, df_ops):
    """Verifica se a venda seria a descoberto."""
    if df_ops.empty:
        return {'descoberto': True, 'qtd_disponivel': 0, 'qtd_faltante': quantidade}
    
    ops_ticket = df_ops[df_ops['ticket'] == ticket]
    compras = ops_ticket[ops_ticket['tipo'] == 'Compra']['quantidade'].sum()
    vendas = ops_ticket[ops_ticket['tipo'] == 'Venda']['quantidade'].sum()
    qtd_disponivel = compras - vendas
    
    if quantidade > qtd_disponivel:
        return {
            'descoberto': True,
            'qtd_disponivel': qtd_disponivel,
            'qtd_faltante': quantidade - qtd_disponivel
        }
    
    return {'descoberto': False}

# --- FUNÇÕES DE BACKUP ---
def fazer_backup():
    """Cria backup do banco de dados."""
    try:
        backup_dir = Path('backups')
        backup_dir.mkdir(exist_ok=True)
        
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        backup_file = backup_dir / f'investimentos_backup_{timestamp}.db'
        
        if Path('investimentos.db').exists():
            shutil.copy2('investimentos.db', backup_file)
            
            # Manter apenas últimos 10 backups
            backups = sorted(backup_dir.glob('*.db'))
            if len(backups) > 10:
                for old in backups[:-10]:
                    old.unlink()
            
            return True, str(backup_file)
        else:
            return False, "Banco de dados não encontrado"
    except Exception as e:
        return False, f"Erro: {str(e)}"

def listar_backups():
    """Lista backups disponíveis."""
    backup_dir = Path('backups')
    if not backup_dir.exists():
        return []
    
    backups = sorted(backup_dir.glob('*.db'), reverse=True)
    return [b.name for b in backups]

def restaurar_backup(backup_name):
    """Restaura um backup."""
    try:
        backup_file = Path('backups') / backup_name
        if backup_file.exists():
            shutil.copy2(backup_file, 'investimentos.db')
            return True, "Backup restaurado com sucesso!"
        return False, "Arquivo de backup não encontrado"
    except Exception as e:
        return False, f"Erro: {str(e)}"

# --- FUNÇÕES DE BANCO DE DADOS ---
def init_db():
    """Inicializa banco de dados com índices para performance."""
    conn = sqlite3.connect('investimentos.db')
    c = conn.cursor()
    
    # Tabela de operações
    c.execute('''CREATE TABLE IF NOT EXISTS operacoes
                 (id INTEGER PRIMARY KEY AUTOINCREMENT,
                  data TEXT, ticket TEXT, tipo TEXT, 
                  quantidade INTEGER, valor REAL, 
                  taxa_corretagem REAL DEFAULT 0,
                  taxa_emolumentos REAL DEFAULT 0,
                  hora TEXT DEFAULT '00:00:00')''')
    
    # Tabela de proventos
    c.execute('''CREATE TABLE IF NOT EXISTS proventos
                 (id INTEGER PRIMARY KEY AUTOINCREMENT,
                  data TEXT, ticket TEXT, tipo TEXT, valor REAL)''')
    
    conn.commit()
    conn.close()
    
    # Executar migração
    migrar_banco()
    
    # Criar índices
    conn = sqlite3.connect('investimentos.db')
    c = conn.cursor()
    try:
        c.execute('CREATE INDEX IF NOT EXISTS idx_data ON operacoes(data)')
        c.execute('CREATE INDEX IF NOT EXISTS idx_ticket ON operacoes(ticket)')
        c.execute('CREATE INDEX IF NOT EXISTS idx_tipo ON operacoes(tipo)')
        c.execute('CREATE INDEX IF NOT EXISTS idx_prov_ticket ON proventos(ticket)')
    except:
        pass
    
    conn.commit()
    conn.close()

@st.cache_data(ttl=30)
def carregar_dados():
    """Carrega dados do banco com cache."""
    conn = sqlite3.connect('investimentos.db')
    df_ops = pd.read_sql_query("SELECT * FROM operacoes ORDER BY data ASC, hora ASC", conn)
    df_prov = pd.read_sql_query("SELECT * FROM proventos ORDER BY data ASC", conn)
    conn.close()
    
    # Garantir que as colunas existam no DataFrame
    if not df_ops.empty:
        if 'taxa_corretagem' not in df_ops.columns:
            df_ops['taxa_corretagem'] = 0.0
        if 'taxa_emolumentos' not in df_ops.columns:
            df_ops['taxa_emolumentos'] = 0.0
    
    return df_ops, df_prov

# --- CÁLCULO DE POSIÇÕES E RESULTADOS ---
def calcular_tudo():
    """Calcula posições, resultados e IR com prejuízos acumulados."""
    df_ops, df_prov = carregar_dados()
    
    if df_ops.empty: 
        return pd.DataFrame(), pd.DataFrame(), df_ops, df_prov, pd.DataFrame()

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
            
            # Calcular custos (com fallback para 0 se coluna não existir)
            custo_compra = (
                compras_dia['taxa_corretagem'].sum() + 
                compras_dia['taxa_emolumentos'].sum()
            ) if 'taxa_corretagem' in compras_dia.columns else 0
            
            custo_venda = (
                vendas_dia['taxa_corretagem'].sum() + 
                vendas_dia['taxa_emolumentos'].sum()
            ) if 'taxa_corretagem' in vendas_dia.columns else 0
            
            # Day Trade
            qtd_dt = min(q_c, q_v)
            if qtd_dt > 0:
                v_compra_m = compras_dia['valor'].mean()
                v_venda_m = vendas_dia['valor'].mean()
                resultado_bruto = (v_venda_m - v_compra_m) * qtd_dt
                resultado_liquido = resultado_bruto - (custo_compra + custo_venda) if qtd_dt == q_c else resultado_bruto - custo_venda
                
                vendas_realizadas.append({
                    'Data': data_dt, 
                    'Hora': hora_venda, 
                    'Ticket': tkt, 
                    'Tipo': 'Day Trade', 
                    'Tipo Ativo': identificar_tipo_ativo(tkt),
                    'Resultado': resultado_liquido, 
                    'Volume Venda': qtd_dt * v_venda_m, 
                    'Mês/Ano': data_dt.strftime('%Y-%m')
                })

            # Swing Trade - Compras
            sobra_c = q_c - qtd_dt
            if sobra_c > 0:
                v_compra_m = compras_dia['valor'].mean()
                custo_medio_unitario = (custo_compra / sobra_c) if sobra_c > 0 else 0
                novo_total = (controle[tkt]['qtd'] * controle[tkt]['pm']) + (sobra_c * (v_compra_m + custo_medio_unitario))
                controle[tkt]['qtd'] += sobra_c
                controle[tkt]['pm'] = novo_total / controle[tkt]['qtd'] if controle[tkt]['qtd'] > 0 else 0

            # Swing Trade - Vendas
            sobra_v = q_v - qtd_dt
            if sobra_v > 0:
                v_venda_m = vendas_dia['valor'].mean()
                custo_medio_unitario = (custo_venda / sobra_v) if sobra_v > 0 else 0
                resultado_liquido = (v_venda_m - custo_medio_unitario - controle[tkt]['pm']) * sobra_v
                
                vendas_realizadas.append({
                    'Data': data_dt, 
                    'Hora': hora_venda, 
                    'Ticket': tkt, 
                    'Tipo': 'Swing Trade',
                    'Tipo Ativo': identificar_tipo_ativo(tkt), 
                    'Resultado': resultado_liquido, 
                    'Volume Venda': sobra_v * v_venda_m, 
                    'Mês/Ano': data_dt.strftime('%Y-%m')
                })
                controle[tkt]['qtd'] -= sobra_v

    df_pos = pd.DataFrame([
        {
            'Ticket': t, 
            'Tipo': identificar_tipo_ativo(t),
            'Quantidade': d['qtd'], 
            'Preço Médio': d['pm'], 
            'Total': d['qtd']*d['pm']
        } 
        for t, d in controle.items() if d['qtd'] > 0
    ])
    
    df_res = pd.DataFrame(vendas_realizadas)
    
    # Calcular IR
    df_ir = calcular_ir_completo(df_res) if not df_res.empty else pd.DataFrame()
    
    return df_pos, df_res, df_ops, df_prov, df_ir

def calcular_ir_completo(df_res):
    """Calcula IR conforme regras da Receita Federal com prejuízos acumulados."""
    
    res_mensal = []
    
    # Controle de prejuízos acumulados por tipo
    prejuizos = {
        'DT': 0,
        'ST_ACAO': 0,
        'ST_FII': 0
    }
    
    for mes in sorted(df_res['Mês/Ano'].unique()):
        df_m = df_res[df_res['Mês/Ano'] == mes]
        
        # === DAY TRADE ===
        dt_lucro = df_m[df_m['Tipo'] == 'Day Trade']['Resultado'].sum()
        dt_lucro_tributavel = dt_lucro + prejuizos['DT']
        
        if dt_lucro_tributavel > 0:
            dt_imposto = dt_lucro_tributavel * 0.20
            prejuizos['DT'] = 0
        else:
            dt_imposto = 0
            prejuizos['DT'] = dt_lucro_tributavel
        
        # === SWING TRADE - AÇÕES ===
        st_acao = df_m[(df_m['Tipo'] == 'Swing Trade') & (df_m['Tipo Ativo'] == 'ACAO')]
        st_acao_lucro = st_acao['Resultado'].sum()
        st_acao_volume = st_acao['Volume Venda'].sum()
        
        if st_acao_volume <= 20000:
            st_acao_imposto = 0
            st_acao_lucro_comp = st_acao_lucro
        else:
            st_acao_lucro_tributavel = st_acao_lucro + prejuizos['ST_ACAO']
            
            if st_acao_lucro_tributavel > 0:
                st_acao_imposto = st_acao_lucro_tributavel * 0.15
                prejuizos['ST_ACAO'] = 0
            else:
                st_acao_imposto = 0
                prejuizos['ST_ACAO'] = st_acao_lucro_tributavel
            
            st_acao_lucro_comp = st_acao_lucro_tributavel
        
        # === SWING TRADE - FII ===
        st_fii = df_m[(df_m['Tipo'] == 'Swing Trade') & (df_m['Tipo Ativo'] == 'FII')]
        st_fii_lucro = st_fii['Resultado'].sum()
        st_fii_volume = st_fii['Volume Venda'].sum()
        st_fii_lucro_tributavel = st_fii_lucro + prejuizos['ST_FII']
        
        if st_fii_lucro_tributavel > 0:
            st_fii_imposto = st_fii_lucro_tributavel * 0.20
            prejuizos['ST_FII'] = 0
        else:
            st_fii_imposto = 0
            prejuizos['ST_FII'] = st_fii_lucro_tributavel
        
        res_mensal.append({
            'Mês/Ano': mes,
            'Lucro DT': dt_lucro,
            'Prej. DT Acum.': prejuizos['DT'],
            'Imposto DT (20%)': dt_imposto,
            'Lucro ST Ações': st_acao_lucro,
            'Volume ST Ações': st_acao_volume,
            'Isento?': 'Sim' if st_acao_volume <= 20000 else 'Não',
            'Prej. ST Ações': prejuizos['ST_ACAO'],
            'Imposto ST Ações (15%)': st_acao_imposto,
            'Lucro ST FII': st_fii_lucro,
            'Volume ST FII': st_fii_volume,
            'Prej. ST FII': prejuizos['ST_FII'],
            'Imposto ST FII (20%)': st_fii_imposto,
            'Total IR': dt_imposto + st_acao_imposto + st_fii_imposto
        })
    
    return pd.DataFrame(res_mensal)

# --- FUNÇÕES DE EXPORTAÇÃO ---
def gerar_relatorio_excel(df_pos, df_res, df_ir, df_prov):
    """Gera relatório completo em Excel."""
    output = io.BytesIO()
    
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        if not df_ir.empty:
            df_ir.to_excel(writer, sheet_name='Resumo IR', index=False)
        
        if not df_pos.empty:
            df_pos.to_excel(writer, sheet_name='Posição Atual', index=False)
        
        if not df_res.empty:
            df_res.to_excel(writer, sheet_name='Operações Realizadas', index=False)
        
        if not df_prov.empty:
            df_prov.to_excel(writer, sheet_name='Proventos', index=False)
    
    return output.getvalue()

# --- SISTEMA DE ALERTAS ---
def gerar_alertas(df_pos, df_ir, df_res):
    """Gera alertas automáticos."""
    alertas = []
    
    # Alerta de IR a pagar
    if not df_ir.empty:
        ir_atual = df_ir.iloc[-1]['Total IR']
        if ir_atual > 10:
            alertas.append({
                'tipo': 'warning',
                'mensagem': f'💰 **IR a Pagar:** Imposto estimado este mês: R$ {ir_atual:.2f}'
            })
    
    # Alerta de concentração
    if not df_pos.empty and len(df_pos) > 1:
        total = df_pos['Total'].sum()
        max_valor = df_pos['Total'].max()
        percentual_max = (max_valor / total) * 100
        ticket_max = df_pos.loc[df_pos['Total'].idxmax(), 'Ticket']
        
        if percentual_max > 30:
            alertas.append({
                'tipo': 'info',
                'mensagem': f'⚠️ **Concentração:** {ticket_max} representa {percentual_max:.1f}% da carteira'
            })
    
    # Alerta de prejuízo acumulado
    if not df_ir.empty:
        prejuizos_totais = (
            df_ir.iloc[-1]['Prej. DT Acum.'] +
            df_ir.iloc[-1]['Prej. ST Ações'] +
            df_ir.iloc[-1]['Prej. ST FII']
        )
        
        if prejuizos_totais < -1000:
            alertas.append({
                'tipo': 'error',
                'mensagem': f'📉 **Prejuízos Acumulados:** R$ {abs(prejuizos_totais):.2f}'
            })
    
    # Alerta de diversificação
    if not df_pos.empty and len(df_pos) < 5:
        alertas.append({
            'tipo': 'info',
            'mensagem': f'📊 **Diversificação:** Carteira com apenas {len(df_pos)} ativos. Considere diversificar.'
        })
    
    return alertas

# --- LOGIN ---
init_db()

if 'autenticado' not in st.session_state: 
    st.session_state['autenticado'] = False

if not st.session_state['autenticado']:
    col1, col2, col3 = st.columns([1, 2, 1])
    
    with col2:
        st.markdown("# 🔐 Login - Gestor B3")
        st.markdown("---")
        
        with st.form("login_form"):
            usuario = st.text_input("👤 Usuário", placeholder="admin")
            senha = st.text_input("🔑 Senha", type="password", placeholder="••••")
            
            login_btn = st.form_submit_button("Entrar", use_container_width=True)
            
            if login_btn:
                senha_hash = hashlib.sha256(senha.encode()).hexdigest()
                if usuario == "admin" and senha_hash == SENHA_HASH:
                    st.session_state['autenticado'] = True
                    st.rerun()
                else:
                    st.error("❌ Usuário ou senha incorretos")
        
        st.markdown("---")
        st.info("💡 **Padrão:** admin / 1234")

else:
    # --- MENU LATERAL ---
    with st.sidebar:
        st.markdown("# 📊 Menu")
        pag = st.radio(
            "Navegação",
            ["🏠 Home", "📝 Registrar Operação", "💰 Registrar Proventos", 
             "🏢 Posição", "📊 Resultados & IR", "🔍 Histórico por Ticket", 
             "⚙️ Gestão de Dados"],
            label_visibility="collapsed"
        )
        
        st.markdown("---")
        
        # Backup manual
        if st.button("💾 Backup Manual", use_container_width=True):
            sucesso, mensagem = fazer_backup()
            if sucesso:
                st.success(f"✅ {mensagem}")
            else:
                st.error(f"❌ {mensagem}")
        
        st.markdown("---")
        
        if st.button("🚪 Sair", use_container_width=True):
            st.session_state['autenticado'] = False
            st.rerun()
    
    # Carregar dados
    df_pos, df_res, df_ops, df_prov, df_ir = calcular_tudo()
    
    # --- PÁGINAS ---
    
    if pag == "🏠 Home":
        st.title("🏠 Painel Geral")
        
        # Métricas principais
        col1, col2, col3, col4 = st.columns(4)
        
        patrimonio = df_pos['Total'].sum() if not df_pos.empty else 0
        lucro_vendas = df_res['Resultado'].sum() if not df_res.empty else 0
        proventos = df_prov['valor'].sum() if not df_prov.empty else 0
        ir_mes = df_ir.iloc[-1]['Total IR'] if not df_ir.empty else 0
        
        col1.metric("💼 Patrimônio", f"R$ {patrimonio:,.2f}")
        col2.metric("📈 Lucro em Vendas", f"R$ {lucro_vendas:,.2f}")
        col3.metric("💰 Proventos", f"R$ {proventos:,.2f}")
        col4.metric("🧾 IR Mês Atual", f"R$ {ir_mes:,.2f}")
        
        st.markdown("---")
        
        # Alertas
        alertas = gerar_alertas(df_pos, df_ir, df_res)
        if alertas:
            st.subheader("🔔 Alertas e Notificações")
            for alerta in alertas:
                if alerta['tipo'] == 'warning':
                    st.warning(alerta['mensagem'])
                elif alerta['tipo'] == 'error':
                    st.error(alerta['mensagem'])
                else:
                    st.info(alerta['mensagem'])
            
            st.markdown("---")
        
        # Tabela resumida
        if not df_pos.empty:
            st.subheader("📋 Resumo da Carteira")
            df_display = df_pos.copy()
            total_cart = df_pos['Total'].sum()
            df_display['% Carteira'] = (df_pos['Total'] / total_cart * 100).round(2)
            
            st.dataframe(
                df_display.style.format({
                    'Preço Médio': 'R$ {:.2f}',
                    'Total': 'R$ {:.2f}',
                    '% Carteira': '{:.2f}%'
                }),
                use_container_width=True,
                hide_index=True
            )
    
    elif pag == "📝 Registrar Operação":
        st.header("📝 Nova Operação")
        
        tickets_existentes = sorted(list(set(
            df_ops['ticket'].tolist() if not df_ops.empty else [] + BLUE_CHIPS + FIIS
        )))
        tickets_existentes.insert(0, "➕ DIGITAR NOVO...")
        
        with st.form("form_operacao", clear_on_submit=True):
            col1, col2, col3 = st.columns(3)
            
            ticket_select = col1.selectbox("🎫 Ticket", tickets_existentes)
            ticket_novo = col1.text_input("Novo Ticket (se selecionou ➕)").upper().strip()
            tipo_op = col2.selectbox("📊 Tipo", ["Compra", "Venda"])
            data_op = col3.date_input("📅 Data", datetime.now())
            
            col4, col5, col6 = st.columns(3)
            qtd_op = col4.number_input("🔢 Quantidade", min_value=1, value=100)
            val_op = col5.number_input("💵 Preço Unitário", min_value=0.01, value=10.0, step=0.01)
            hora_op = col6.time_input("🕐 Hora", datetime.now().time())
            
            col7, col8 = st.columns(2)
            taxa_corretagem = col7.number_input("💸 Corretagem", min_value=0.0, value=0.0, step=0.01)
            taxa_emolumentos = col8.number_input("💸 Emolumentos", min_value=0.0, value=0.0, step=0.01)
            
            st.markdown("---")
            
            submit_btn = st.form_submit_button("💾 Salvar Operação", use_container_width=True)
            
            if submit_btn:
                ticket_final = ticket_novo if ticket_select == "➕ DIGITAR NOVO..." else ticket_select
                
                # Validar
                erros = validar_operacao(ticket_final, tipo_op, qtd_op, val_op, data_op)
                
                if erros:
                    for erro in erros:
                        st.error(erro)
                else:
                    # Verificar venda a descoberto
                    if tipo_op == "Venda":
                        check = verificar_venda_descoberto(ticket_final, qtd_op, df_ops)
                        if check['descoberto']:
                            st.warning(f"⚠️ **ATENÇÃO: Venda a Descoberto!**")
                            st.info(f"📊 Disponível: {check['qtd_disponivel']} | Faltante: {check['qtd_faltante']}")
                            st.error("⛔ Operação bloqueada. Verifique sua posição.")
                            st.stop()
                    
                    # Salvar
                    conn = sqlite3.connect('investimentos.db')
                    conn.execute(
                        """INSERT INTO operacoes 
                           (data, ticket, tipo, quantidade, valor, taxa_corretagem, taxa_emolumentos, hora) 
                           VALUES (?,?,?,?,?,?,?,?)""",
                        (data_op.strftime('%Y-%m-%d'), ticket_final, tipo_op, qtd_op, val_op,
                         taxa_corretagem, taxa_emolumentos, hora_op.strftime('%H:%M:%S'))
                    )
                    conn.commit()
                    conn.close()
                    
                    # Limpar cache
                    carregar_dados.clear()
                    
                    st.success(f"✅ Operação registrada: {tipo_op} de {qtd_op} {ticket_final} @ R$ {val_op:.2f}")
                    st.balloons()
                    st.rerun()
    
    elif pag == "💰 Registrar Proventos":
        st.header("💰 Registrar Proventos")
        
        tickets_proventos = sorted(list(set(
            df_ops['ticket'].tolist() if not df_ops.empty else [] + BLUE_CHIPS + FIIS
        )))
        
        with st.form("form_provento", clear_on_submit=True):
            col1, col2 = st.columns(2)
            
            ticket_prov = col1.selectbox("🎫 Ticket", tickets_proventos)
            tipo_prov = col2.selectbox("📊 Tipo", ["Dividendo", "JCP", "Rendimento"])
            valor_prov = col1.number_input("💵 Valor Recebido", min_value=0.01, step=0.01)
            data_prov = col2.date_input("📅 Data do Pagamento", datetime.now())
            
            st.markdown("---")
            
            if st.form_submit_button("💾 Salvar Provento", use_container_width=True):
                conn = sqlite3.connect('investimentos.db')
                conn.execute(
                    "INSERT INTO proventos (data, ticket, tipo, valor) VALUES (?,?,?,?)",
                    (data_prov.strftime('%Y-%m-%d'), ticket_prov, tipo_prov, valor_prov)
                )
                conn.commit()
                conn.close()
                
                carregar_dados.clear()
                
                st.success(f"✅ Provento registrado: {tipo_prov} de R$ {valor_prov:.2f} - {ticket_prov}")
                st.rerun()
    
    elif pag == "🏢 Posição":
        st.header("🏢 Carteira Atual")
        
        if not df_pos.empty:
            total_patrimonio = df_pos['Total'].sum()
            df_display = df_pos.copy()
            df_display['% Carteira'] = (df_pos['Total'] / total_patrimonio * 100).round(2)
            
            st.dataframe(
                df_display.style.format({
                    'Preço Médio': 'R$ {:.2f}',
                    'Total': 'R$ {:.2f}',
                    '% Carteira': '{:.2f}%'
                }),
                use_container_width=True,
                hide_index=True
            )
            
            st.metric("💼 Total da Carteira", f"R$ {total_patrimonio:,.2f}")
        else:
            st.info("📭 Nenhuma posição em aberto")
    
    elif pag == "📊 Resultados & IR":
        st.header("📊 Resultados e Imposto de Renda")
        
        if not df_ir.empty:
            st.subheader("📋 Resumo Mensal de IR")
            
            st.dataframe(
                df_ir.style.format({
                    'Lucro DT': 'R$ {:.2f}',
                    'Prej. DT Acum.': 'R$ {:.2f}',
                    'Imposto DT (20%)': 'R$ {:.2f}',
                    'Lucro ST Ações': 'R$ {:.2f}',
                    'Volume ST Ações': 'R$ {:.2f}',
                    'Prej. ST Ações': 'R$ {:.2f}',
                    'Imposto ST Ações (15%)': 'R$ {:.2f}',
                    'Lucro ST FII': 'R$ {:.2f}',
                    'Volume ST FII': 'R$ {:.2f}',
                    'Prej. ST FII': 'R$ {:.2f}',
                    'Imposto ST FII (20%)': 'R$ {:.2f}',
                    'Total IR': 'R$ {:.2f}'
                }),
                use_container_width=True,
                hide_index=True
            )
            
            st.markdown("---")
            
            # Exportação
            excel_data = gerar_relatorio_excel(df_pos, df_res, df_ir, df_prov)
            
            st.download_button(
                label="📥 Baixar Relatório Completo (Excel)",
                data=excel_data,
                file_name=f"relatorio_ir_{datetime.now().strftime('%Y%m')}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                use_container_width=True
            )
            
            st.markdown("---")
            st.subheader("📝 Detalhamento das Operações")
            
            st.dataframe(
                df_res.style.format({
                    'Resultado': 'R$ {:.2f}',
                    'Volume Venda': 'R$ {:.2f}'
                }),
                use_container_width=True,
                hide_index=True
            )
        else:
            st.info("📭 Sem operações de venda realizadas")
    
    elif pag == "🔍 Histórico por Ticket":
        st.header("🔍 Consultar Ativo Específico")
        
        todos_tickets = sorted(df_ops['ticket'].unique().tolist()) if not df_ops.empty else []
        
        if todos_tickets:
            ticket_escolhido = st.selectbox("🎫 Selecione o Ticket", todos_tickets)
            
            tab1, tab2, tab3 = st.tabs(["📝 Operações", "💰 Proventos", "📊 Resumo"])
            
            with tab1:
                ops_ticket = df_ops[df_ops['ticket'] == ticket_escolhido].sort_values(
                    ['data', 'hora'], 
                    ascending=False
                )
                st.dataframe(ops_ticket, use_container_width=True, hide_index=True)
            
            with tab2:
                if not df_prov.empty:
                    prov_ticket = df_prov[df_prov['ticket'] == ticket_escolhido]
                    if not prov_ticket.empty:
                        st.dataframe(prov_ticket, use_container_width=True, hide_index=True)
                        st.metric("💰 Total Proventos", f"R$ {prov_ticket['valor'].sum():.2f}")
                    else:
                        st.info("📭 Sem proventos para este ticket")
                else:
                    st.info("📭 Sem proventos registrados")
            
            with tab3:
                pos_ticket = df_pos[df_pos['Ticket'] == ticket_escolhido]
                
                if not pos_ticket.empty:
                    col1, col2, col3 = st.columns(3)
                    col1.metric("🔢 Quantidade", int(pos_ticket.iloc[0]['Quantidade']))
                    col2.metric("💵 Preço Médio", f"R$ {pos_ticket.iloc[0]['Preço Médio']:.2f}")
                    col3.metric("💼 Total Investido", f"R$ {pos_ticket.iloc[0]['Total']:.2f}")
                else:
                    st.info("📭 Sem posição atual neste ativo")
        else:
            st.info("📭 Nenhuma operação registrada")
    
    elif pag == "⚙️ Gestão de Dados":
        st.header("⚙️ Central de Gestão")
        
        tab1, tab2, tab3 = st.tabs(["📝 Editar Operações", "💾 Backup & Restore", "🗑️ Limpeza"])
        
        with tab1:
            st.subheader("Editar Operações")
            st.warning("⚠️ Edições afetarão todos os cálculos. Use com cautela!")
            
            if not df_ops.empty:
                edited_ops = st.data_editor(
                    df_ops,
                    use_container_width=True,
                    num_rows="dynamic",
                    key="editor_ops",
                    hide_index=False
                )
                
                if st.button("💾 Salvar Alterações", type="primary"):
                    conn = sqlite3.connect('investimentos.db')
                    conn.execute("DELETE FROM operacoes")
                    edited_ops.to_sql('operacoes', conn, index=False, if_exists='append')
                    conn.commit()
                    conn.close()
                    
                    carregar_dados.clear()
                    
                    st.success("✅ Operações atualizadas!")
                    st.rerun()
            else:
                st.info("📭 Sem operações para editar")
        
        with tab2:
            st.subheader("💾 Backup e Restauração")
            
            col1, col2 = st.columns(2)
            
            with col1:
                st.markdown("#### Criar Backup")
                if st.button("📦 Criar Backup Agora", use_container_width=True):
                    sucesso, mensagem = fazer_backup()
                    if sucesso:
                        st.success(f"✅ {mensagem}")
                    else:
                        st.error(f"❌ {mensagem}")
            
            with col2:
                st.markdown("#### Restaurar Backup")
                backups = listar_backups()
                
                if backups:
                    backup_escolhido = st.selectbox("Selecione o backup", backups)
                    
                    if st.button("♻️ Restaurar Backup", use_container_width=True):
                        sucesso, mensagem = restaurar_backup(backup_escolhido)
                        if sucesso:
                            carregar_dados.clear()
                            st.success(f"✅ {mensagem}")
                            st.rerun()
                        else:
                            st.error(f"❌ {mensagem}")
                else:
                    st.info("📭 Nenhum backup disponível")
        
        with tab3:
            st.subheader("🗑️ Limpeza de Dados")
            st.error("⚠️ **ATENÇÃO:** Ações irreversíveis! Faça backup antes.")
            
            col1, col2 = st.columns(2)
            
            with col1:
                if st.button("🗑️ Limpar Todas Operações", use_container_width=True):
                    if st.checkbox("Confirmo que quero limpar TODAS as operações"):
                        conn = sqlite3.connect('investimentos.db')
                        conn.execute("DELETE FROM operacoes")
                        conn.commit()
                        conn.close()
                        carregar_dados.clear()
                        st.success("✅ Operações limpas!")
                        st.rerun()
            
            with col2:
                if st.button("🗑️ Limpar Todos Proventos", use_container_width=True):
                    if st.checkbox("Confirmo que quero limpar TODOS os proventos"):
                        conn = sqlite3.connect('investimentos.db')
                        conn.execute("DELETE FROM proventos")
                        conn.commit()
                        conn.close()
                        carregar_dados.clear()
                        st.success("✅ Proventos limpos!")
                        st.rerun()