import streamlit as st
import pandas as pd
import psycopg2
from datetime import datetime, timedelta

# Configuração da página
st.set_page_config(
    page_title="Clientes sem Compra - Rede Biz",
    page_icon="💤",
    layout="wide"
)

# Custom CSS
st.markdown("""
<style>
    .metric-card {
        background-color: #ffffff;
        padding: 20px;
        border-radius: 10px;
        box-shadow: 0 4px 6px rgba(0,0,0,0.1);
        text-align: center;
    }
</style>
""", unsafe_allow_html=True)

# Função para conectar ao PostgreSQL
@st.cache_resource
def get_connection():
    """Estabelece conexão com PostgreSQL"""
    try:
        # Usando st.secrets para credenciais
        db_config = st.secrets["postgres"]
        
        conn = psycopg2.connect(
            host=db_config["host"],
            database=db_config["database"],
            user=db_config["user"],
            password=db_config["password"],
            port=db_config["port"]
        )
        return conn
    except Exception as e:
        st.error(f"Erro ao conectar ao banco de dados: {e}")
        return None

# Função para carregar vendedores
@st.cache_data(ttl=3600)
def get_vendedores(_conn):
    query = """
    SELECT vendedor, nome 
    FROM vendedores 
    ORDER BY nome
    """
    try:
        df = pd.read_sql(query, _conn)
        if df.empty:
            st.warning("Nenhum vendedor encontrado no banco de dados.")
            return {}
        # Retorna dict com código: nome
        return dict(zip(df['vendedor'].astype(str), df['nome']))
    except Exception as e:
        st.error(f"Erro ao carregar vendedores: {e}")
        return {}

@st.cache_data(ttl=3600)
def get_fornecedores(_conn):
    # IDs solicitados: 10263, 11588, 11585, 11392
    query = """
    SELECT DISTINCT nome_fornecedor 
    FROM mercadorias 
    WHERE fornecedor IN (10263, 11058, 11098, 11300, 11392, 11449, 11459, 11523, 11536, 11580, 11581, 11585, 11588) 
    ORDER BY nome_fornecedor
    """
    try:
        df = pd.read_sql(query, _conn)
        return df['nome_fornecedor'].tolist()
    except Exception as e:
        st.error(f"Erro ao carregar fornecedores: {e}")
        return []

@st.cache_data(ttl=3600)
def get_cidades(_conn):
    query = "SELECT DISTINCT cidade FROM clientes WHERE cidade IS NOT NULL ORDER BY cidade"
    try:
        df = pd.read_sql(query, _conn)
        return df['cidade'].tolist()
    except Exception as e:
        st.error(f"Erro ao carregar cidades: {e}")
        return []

@st.cache_data(ttl=600)
def get_clientes_sem_compra(_conn, meses_sem_compra, fornecedores_sel, cidades_sel, vendedores_sel):
    """
    Clientes que não compram há X meses (meses fechados).
    Exclui vendedor 2.
    """
    
    filtro_vendedor_vendas = ""
    filtro_cidade = ""
    filtro_fornecedor = ""
    
    # Filtros de Vendedor
    if 'Todos' not in vendedores_sel and vendedores_sel:
        lista_vendedores = "', '".join(vendedores_sel)
        filtro_vendedor_vendas = f"AND v.vendedor IN ('{lista_vendedores}')"
    
    # Filtros de Cidade
    if 'Todas' not in cidades_sel and cidades_sel:
        lista_cidades = "', '".join(cidades_sel)
        filtro_cidade = f"AND c.cidade IN ('{lista_cidades}')"
        
    # Filtros de Fornecedor (na tabela mercadorias)
    if 'Todos' not in fornecedores_sel and fornecedores_sel:
        lista_fornecedores = "', '".join(fornecedores_sel)
        filtro_fornecedor = f"AND m.nome_fornecedor IN ('{lista_fornecedores}')"

    query = f"""
    WITH ultimas_vendas AS (
        SELECT 
            v.cliente,
            MAX(v.data_emissao) as data_ultima_compra,
            (SELECT v2.vendedor FROM vendas v2 WHERE v2.cliente = v.cliente AND v2.data_emissao = MAX(v.data_emissao) LIMIT 1) as ultimo_vendedor_cod
        FROM vendas v
        JOIN mercadorias m ON v.mercadoria = m.mercadoria
        WHERE v.vendedor != '2'
        {filtro_vendedor_vendas}
        {filtro_fornecedor}
        GROUP BY v.cliente
    )
    SELECT 
        c.cliente::int as cliente,
        c.raz_social,
        c.cidade,
        uv.ultimo_vendedor_cod::int as ultimo_vendedor_cod,
        ven.nome as nome_vendedor,
        c.situacao,
        c.antecipado,
        CAST(REPLACE(REGEXP_REPLACE(c.limite_aberto, '[^0-9,]', '', 'g'), ',', '.') AS NUMERIC) as limite,
        uv.data_ultima_compra,
        (EXTRACT(YEAR FROM CURRENT_DATE) - EXTRACT(YEAR FROM uv.data_ultima_compra)) * 12 + 
        (EXTRACT(MONTH FROM CURRENT_DATE) - EXTRACT(MONTH FROM uv.data_ultima_compra)) AS meses_sem_compra
    FROM ultimas_vendas uv
    JOIN clientes c ON uv.cliente = c.cliente
    LEFT JOIN vendedores ven ON uv.ultimo_vendedor_cod::text = ven.vendedor::text
    WHERE ((EXTRACT(YEAR FROM CURRENT_DATE) - EXTRACT(YEAR FROM uv.data_ultima_compra)) * 12 + 
           (EXTRACT(MONTH FROM CURRENT_DATE) - EXTRACT(MONTH FROM uv.data_ultima_compra))) >= {meses_sem_compra}
    {filtro_cidade}
    ORDER BY meses_sem_compra DESC
    """
    try:
        return pd.read_sql(query, _conn)
    except Exception as e:
        st.error(f"Erro ao buscar clientes sem compra: {e}")
        return pd.DataFrame()

def main():
    st.title("💤 Clientes sem Compra")
    st.markdown("---")

    conn = get_connection()
    if not conn:
        return

    # Carregar dados auxiliares
    vendedores_dict = get_vendedores(conn)
    vendedor_opcoes = ['Todos'] + list(vendedores_dict.values())

    with st.container():
        col_f1, col_f2, col_f3, col_f4 = st.columns(4)
        
        with col_f1:
            meses_sem_compra = st.number_input("Meses sem compra (mínimo)", min_value=0, value=3, step=1)
            
        with col_f2:
            fornecedores_lista = get_fornecedores(conn)
            fornecedores_sel = st.multiselect("Fornecedor", ['Todos'] + fornecedores_lista, default=['Todos'])
            if 'Todos' in fornecedores_sel: fornecedores_sel = ['Todos']
            
        with col_f3:
            cidades_lista = get_cidades(conn)
            cidades_sel = st.multiselect("Cidade", ['Todas'] + cidades_lista, default=['Todas'])
            if 'Todas' in cidades_sel: cidades_sel = ['Todas']
            
        with col_f4:
            vendedores_sel_report = st.multiselect("Vendedor", vendedor_opcoes, default=['Todos'])
            
            if 'Todos' in vendedores_sel_report:
                vendedores_cod_report = ['Todos']
            else:
                nome_para_codigo = {v: k for k, v in vendedores_dict.items()}
                vendedores_cod_report = [nome_para_codigo[nome] for nome in vendedores_sel_report if nome in nome_para_codigo]

    if st.button("Gerar Relatório", type="primary"):
        with st.spinner("Buscando clientes..."):
            df_sem_compra = get_clientes_sem_compra(conn, meses_sem_compra, fornecedores_sel, cidades_sel, vendedores_cod_report)
            
        if not df_sem_compra.empty:
            st.success(f"Encontrados {len(df_sem_compra)} clientes.")
            
            # Botão de Exportação
            import io
            buffer = io.BytesIO()
            with pd.ExcelWriter(buffer, engine='xlsxwriter') as writer:
                df_sem_compra.to_excel(writer, index=False, sheet_name='Clientes Sem Compra')
            
            st.download_button(
                label="📥 Exportar para Excel",
                data=buffer.getvalue(),
                file_name=f"clientes_sem_compra_{datetime.now().strftime('%Y%m%d')}.xlsx",
                mime="application/vnd.ms-excel"
            )
            
            st.dataframe(
                df_sem_compra.style.format({
                    'data_ultima_compra': lambda x: x.strftime('%d/%m/%Y') if pd.notnull(x) else '',
                    'meses_sem_compra': '{:.0f}',
                    'limite': 'R$ {:,.2f}',
                    'cliente': '{:.0f}',
                    'ultimo_vendedor_cod': '{:.0f}'
                }),
                use_container_width=True,
                column_config={
                    "cliente": st.column_config.NumberColumn("Cliente", format="%d"),
                    "raz_social": "Razão Social",
                    "cidade": "Cidade",
                    "ultimo_vendedor_cod": st.column_config.NumberColumn("Cód. Vend.", format="%d"),
                    "nome_vendedor": "Último vendedor",
                    "situacao": "Situação",
                    "antecipado": "Antecipado",
                    "limite": "Limite",
                    "data_ultima_compra": "Última Compra",
                    "meses_sem_compra": "Meses s/ Compra"
                }
            )
        else:
            st.info("Nenhum cliente encontrado com os critérios selecionados.")

    st.markdown("---")
    st.caption("📌 Utilize os filtros acima para refinar sua busca.")

if __name__ == "__main__":
    main()
