import streamlit as st
import pandas as pd
import psycopg2
import plotly.graph_objects as go
import plotly.express as px
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
    SELECT DISTINCT ven.vendedor, ven.nome 
    FROM vendedores ven
    INNER JOIN vendas v ON v.vendedor::text = ven.vendedor::text
    WHERE v.vendedor != '2'
    ORDER BY ven.nome
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
    WHERE fornecedor IN (10124, 10275, 10263, 11058, 11098, 11300, 11392, 11449, 11459, 11523, 11536, 11580, 11581, 11585, 11588) 
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
    # query = "SELECT DISTINCT cidade FROM clientes WHERE cidade IS NOT NULL ORDER BY cidade"
    query = "SELECT c.cidade, SUM(REPLACE(REPLACE(REPLACE(v.valor_liq, 'R$ ', ''),'.', ''),',', '.')::NUMERIC) AS vv FROM clientes c INNER JOIN vendas v ON v.cliente = c.cliente WHERE c.cidade IS NOT NULL AND UF = 'MG' GROUP BY c.cidade ORDER BY c.cidade"
    try:
        df = pd.read_sql(query, _conn)
        return df['cidade'].tolist()
    except Exception as e:
        st.error(f"Erro ao carregar cidades: {e}")
        return []

@st.cache_data(ttl=3600)
def get_periodo_vendas(_conn):
    """Retorna a data mínima e máxima da base de vendas"""
    query = """
    SELECT 
        MIN(data_emissao) as data_inicial,
        MAX(data_emissao) as data_final
    FROM vendas
    """
    try:
        df = pd.read_sql(query, _conn)
        return df.iloc[0]['data_inicial'], df.iloc[0]['data_final']
    except Exception as e:
        st.error(f"Erro ao buscar período de vendas: {e}")
        return None, None

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
            v.data_emissao as data_ultima_compra,
            v.vendedor as ultimo_vendedor_cod,
            ROW_NUMBER() OVER (PARTITION BY v.cliente ORDER BY v.data_emissao DESC) as rn
        FROM vendas v
        JOIN mercadorias m ON v.mercadoria = m.mercadoria
        WHERE v.vendedor != '2'
        {filtro_vendedor_vendas}
        {filtro_fornecedor}
    )
    SELECT 
        c.cliente::int as cliente,
        c.raz_social,
        c.cidade,
        uv.ultimo_vendedor_cod::int as ultimo_vendedor_cod,
        ven.nome as nome_vendedor,
        ven.data_desligamento,
        CASE
            WHEN c.situacao = 'S' THEN 'Suspenso'
            WHEN c.antecipado = 'A28' THEN 'Antecipado'
            WHEN c.limite_aberto = 0 THEN 'Suspenso'
            ELSE 'Liberado'
        END AS situacao,
        c.limite_aberto as limite,
        uv.data_ultima_compra,
        (EXTRACT(YEAR FROM CURRENT_DATE) - EXTRACT(YEAR FROM uv.data_ultima_compra)) * 12 + 
        (EXTRACT(MONTH FROM CURRENT_DATE) - EXTRACT(MONTH FROM uv.data_ultima_compra)) AS meses_sem_compra
    FROM ultimas_vendas uv
    JOIN clientes c ON uv.cliente = c.cliente
    LEFT JOIN vendedores ven ON uv.ultimo_vendedor_cod::text = ven.vendedor::text
    WHERE uv.rn = 1
    AND ((EXTRACT(YEAR FROM CURRENT_DATE) - EXTRACT(YEAR FROM uv.data_ultima_compra)) * 12 + 
           (EXTRACT(MONTH FROM CURRENT_DATE) - EXTRACT(MONTH FROM uv.data_ultima_compra))) >= {meses_sem_compra}
    {filtro_cidade}
    ORDER BY meses_sem_compra DESC
    """
    try:
        return pd.read_sql(query, _conn)
    except Exception as e:
        st.error(f"Erro ao buscar clientes sem compra: {e}")
        return pd.DataFrame()
@st.cache_data(ttl=600)
def get_evolucao_clientes(_conn, fornecedores_sel, cidades_sel, vendedores_sel):
    """
    Busca a quantidade de clientes atendidos (com compras) por mês no último ano.
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
    WITH vendas_por_cliente AS (
        SELECT
            DATE_TRUNC('month', v.data_emissao) as mes_ref,
            v.cliente,
            SUM(CASE 
                WHEN v.tipo = 'V' THEN 
                    REPLACE(REPLACE(REPLACE(v.valor_liq, 'R$ ', ''), '.', ''), ',', '.')::NUMERIC
                WHEN v.tipo = 'D' THEN 
                    -REPLACE(REPLACE(REPLACE(v.valor_liq, 'R$ ', ''), '.', ''), ',', '.')::NUMERIC
                ELSE 0
            END) as total_venda
        FROM vendas v
        JOIN mercadorias m ON v.mercadoria = m.mercadoria
        JOIN clientes c ON v.cliente = c.cliente
        WHERE v.data_emissao >= DATE_TRUNC('year', CURRENT_DATE - INTERVAL '1 year')
        AND v.vendedor != '2'
        {filtro_vendedor_vendas}
        {filtro_fornecedor}
        {filtro_cidade}
        GROUP BY DATE_TRUNC('month', v.data_emissao), v.cliente
    )
    SELECT
        TO_CHAR(mes_ref, 'YYYY-MM-DD') as mes_ref,
        COUNT(DISTINCT cliente) as qtd_clientes_total,
        COUNT(DISTINCT CASE WHEN total_venda > 0 THEN cliente END) as qtd_clientes_com_venda
    FROM vendas_por_cliente
    GROUP BY mes_ref
    ORDER BY mes_ref;
    """
    try:
        return pd.read_sql(query, _conn)
    except Exception as e:
        st.error(f"Erro ao buscar evolução de clientes: {e}")
        return pd.DataFrame()

def main():
    st.title("💤 Clientes sem Compra")
    st.markdown("---")

    conn = get_connection()
    if not conn:
        return

    # Buscar período de dados
    data_inicial, data_final = get_periodo_vendas(conn)
    
    # Card informativo sobre a base de dados
    if data_inicial and data_final:
        st.info(f"""
        📊 **Informações sobre a Base de Dados**
        
        Este aplicativo analisa **apenas pedidos faturados** da equipe externa, televendas e supervisores.
        
        **Período disponível na base:** {data_inicial.strftime('%d/%m/%Y')} até {data_final.strftime('%d/%m/%Y')}
        
        ℹ️ Pedidos em aberto ou não faturados não são considerados nesta análise.

        💡💡 Caso selecione algum filtro abaixo, desmarque a opção "Todos", para que o filtro seja aplicado.

        Observação: O vendedor exibido nas análises é o último vendedor que atendeu ao cliente, não o vendedor atual.
        """)
    
    st.markdown("---")

    # Carregar dados auxiliares
    vendedores_dict = get_vendedores(conn)
    vendedor_opcoes = ['Todos'] + list(vendedores_dict.values())


    with st.container():
        col_f1, col_f2, col_f3 = st.columns(3)
        
        # meses_sem_compra fixo em 0 conforme solicitado
        meses_sem_compra = 0
            
        with col_f1:
            fornecedores_lista = get_fornecedores(conn)
            fornecedores_sel = st.multiselect("Fornecedor", ['Todos'] + fornecedores_lista, default=['Todos'])
            if 'Todos' in fornecedores_sel: fornecedores_sel = ['Todos']
            
        with col_f2:
            cidades_lista = get_cidades(conn)
            cidades_sel = st.multiselect("Cidade", ['Todas'] + cidades_lista, default=['Todas'])
            if 'Todas' in cidades_sel: cidades_sel = ['Todas']
            
        with col_f3:
            vendedores_sel_report = st.multiselect("Vendedor", vendedor_opcoes, default=['Todos'])
            
            if 'Todos' in vendedores_sel_report:
                vendedores_cod_report = ['Todos']
            else:
                nome_para_codigo = {v: k for k, v in vendedores_dict.items()}
                vendedores_cod_report = [nome_para_codigo[nome] for nome in vendedores_sel_report if nome in nome_para_codigo]

    if st.button("Gerar Relatório", type="primary"):
        with st.spinner("Buscando dados..."):
            df_sem_compra = get_clientes_sem_compra(conn, meses_sem_compra, fornecedores_sel, cidades_sel, vendedores_cod_report)
            df_evolucao = get_evolucao_clientes(conn, fornecedores_sel, cidades_sel, vendedores_cod_report)
            
        if not df_sem_compra.empty:
            st.success(f"Encontrados {len(df_sem_compra)} clientes sem compra.")
            
            # Gráfico de Evolução de Clientes Atendidos
            if not df_evolucao.empty:
                # Formatar mês para exibição (jan-25, etc)
                df_evolucao['mes_dt'] = pd.to_datetime(df_evolucao['mes_ref'])
                
                # Separar dados por ano
                ano_atual = datetime.now().year
                df_ano_atual = df_evolucao[df_evolucao['mes_dt'].dt.year == ano_atual].copy()
                df_ano_anterior = df_evolucao[df_evolucao['mes_dt'].dt.year == (ano_atual - 1)].copy()
                
                # Mapeamento de meses para PT-BR
                meses_pt = {1: 'jan', 2: 'fev', 3: 'mar', 4: 'abr', 5: 'mai', 6: 'jun', 
                           7: 'jul', 8: 'ago', 9: 'set', 10: 'out', 11: 'nov', 12: 'dez'}
                
                # --- Gráfico Ano Anterior ---
                if not df_ano_anterior.empty:
                    df_ano_anterior['mes_label'] = df_ano_anterior['mes_dt'].apply(lambda x: f"{meses_pt[x.month]}-{str(x.year)[-2:]}")
                    
                    st.markdown("### 📉 Evolução de Clientes Atendidos (Ano Anterior)")
                    fig_ev_ant = go.Figure()
                    fig_ev_ant.add_trace(go.Scatter(
                        x=df_ano_anterior['mes_label'],
                        y=df_ano_anterior['qtd_clientes_com_venda'],
                        mode='lines+markers+text',
                        text=df_ano_anterior['qtd_clientes_com_venda'],
                        textposition="top center",
                        line=dict(color='gray', width=3, dash='dot'),
                        marker=dict(size=8)
                    ))
                    
                    fig_ev_ant.update_layout(
                        title=f"Quantidade de Clientes Atendidos por Mês ({ano_atual - 1})",
                        xaxis_title="Mês",
                        yaxis_title="Clientes Atendidos",
                        height=400,
                        showlegend=False,
                        plot_bgcolor='rgba(0,0,0,0)',
                        yaxis=dict(showgrid=True, gridcolor='lightgray'),
                        xaxis=dict(showgrid=False)
                    )
                    st.plotly_chart(fig_ev_ant, use_container_width=True)
                    st.markdown("---")

                # --- Gráfico Ano Atual ---
                if not df_ano_atual.empty:
                    df_ano_atual['mes_label'] = df_ano_atual['mes_dt'].apply(lambda x: f"{meses_pt[x.month]}-{str(x.year)[-2:]}")
                    
                    st.markdown("### 📈 Evolução de Clientes Atendidos (Ano Atual)")
                    fig_ev = go.Figure()
                    fig_ev.add_trace(go.Scatter(
                        x=df_ano_atual['mes_label'],
                        y=df_ano_atual['qtd_clientes_com_venda'],
                        mode='lines+markers+text',
                        text=df_ano_atual['qtd_clientes_com_venda'],
                        textposition="top center",
                        line=dict(color='royalblue', width=3),
                        marker=dict(size=8)
                    ))
                    
                    fig_ev.update_layout(
                        title=f"Quantidade de Clientes Atendidos por Mês ({ano_atual})",
                        xaxis_title="Mês",
                        yaxis_title="Clientes Atendidos",
                        height=400,
                        showlegend=False,
                        plot_bgcolor='rgba(0,0,0,0)',
                        yaxis=dict(showgrid=True, gridcolor='lightgray'),
                        xaxis=dict(showgrid=False)
                    )
                    st.plotly_chart(fig_ev, use_container_width=True)
                    st.markdown("---")

            # Gráfico de Barras - Distribuição por Meses sem Compra
            st.markdown("### 📊 Distribuição de Clientes por Meses sem Compra")
            
            # Agrupar clientes por meses sem compra
            distribuicao = df_sem_compra.groupby('meses_sem_compra').size().reset_index(name='quantidade')
            distribuicao = distribuicao.sort_values('meses_sem_compra')
            
            # Calcular percentuais
            distribuicao['percentual'] = (distribuicao['quantidade'] / distribuicao['quantidade'].sum() * 100).round(1)
            
            # Criar cores em gradiente (verde -> amarelo -> vermelho)
            import plotly.express as px
            max_meses = distribuicao['meses_sem_compra'].max()
            colors = px.colors.sample_colorscale(
                "RdYlGn_r",  # Reversed: Red-Yellow-Green (vermelho para mais meses)
                [i/max_meses for i in distribuicao['meses_sem_compra']]
            )
            
            # Criar gráfico de barras horizontais
            fig = go.Figure(go.Bar(
                y=[f"{int(row['meses_sem_compra'])} {'mês' if row['meses_sem_compra'] == 1 else 'meses'}" 
                   for _, row in distribuicao.iterrows()],
                x=distribuicao['quantidade'],
                orientation='h',
                text=[f"{int(row['quantidade'])} clientes ({row['percentual']}%)" 
                      for _, row in distribuicao.iterrows()],
                textposition='auto',
                marker=dict(
                    color=colors,
                    line=dict(color='white', width=1)
                ),
                hovertemplate='<b>%{y}</b><br>Clientes: %{x}<br><extra></extra>'
            ))
            
            fig.update_layout(
                title={
                    'text': "Quantidade de Clientes por Período sem Compra",
                    'x': 0.5,
                    'xanchor': 'center'
                },
                xaxis_title="Quantidade de Clientes",
                yaxis_title="Período sem Compra",
                height=max(400, len(distribuicao) * 40),  # Altura dinâmica baseada no número de categorias
                showlegend=False,
                plot_bgcolor='rgba(0,0,0,0)',
                yaxis=dict(categoryorder='array', categoryarray=[f"{int(row['meses_sem_compra'])} {'mês' if row['meses_sem_compra'] == 1 else 'meses'}" 
                   for _, row in distribuicao.sort_values('meses_sem_compra', ascending=False).iterrows()])
            )
            
            st.plotly_chart(fig, use_container_width=True)
            
            # Tabela de Distribuição por Situação
            st.markdown("### 📋 Distribuição por Situação do Cliente")
            
            # Criar tabela pivot com meses x situação
            tabela_situacao = df_sem_compra.groupby(['meses_sem_compra', 'situacao']).size().reset_index(name='quantidade')
            pivot_situacao = tabela_situacao.pivot(index='meses_sem_compra', columns='situacao', values='quantidade').fillna(0).astype(int)
            
            # Adicionar totais
            pivot_situacao['Total'] = pivot_situacao.sum(axis=1)
            pivot_situacao.loc['Total'] = pivot_situacao.sum()
            
            # Renomear índice
            pivot_situacao.index = [f"{int(idx)} {'mês' if idx == 1 else 'meses'}" if idx != 'Total' else 'Total Geral' 
                                   for idx in pivot_situacao.index]
            
            # Criar função de estilo personalizada
            def color_columns(col):
                if col.name == 'Liberado':
                    return ['background-color: #90EE90; color: black' if val != 'Total Geral' else 'background-color: #90EE90; color: black; font-weight: bold' for val in col.index]
                elif col.name == 'Antecipado':
                    return ['background-color: #FFD700; color: black' if val != 'Total Geral' else 'background-color: #FFD700; color: black; font-weight: bold' for val in col.index]
                elif col.name == 'Suspenso':
                    return ['background-color: #FF6B6B; color: black' if val != 'Total Geral' else 'background-color: #FF6B6B; color: black; font-weight: bold' for val in col.index]
                elif col.name == 'Total':
                    return ['background-color: #E0E0E0; color: black; font-weight: bold' for _ in col.index]
                else:
                    return ['color: black' for _ in col.index]
            
            # Exibir tabela estilizada
            st.dataframe(
                pivot_situacao.style.apply(color_columns, axis=0)
                                    .format("{:,.0f}"),
                use_container_width=True
            )
            
            # Botão de Exportação - Distribuição por Situação
            import io
            buffer_situacao = io.BytesIO()
            with pd.ExcelWriter(buffer_situacao, engine='xlsxwriter') as writer:
                pivot_situacao.to_excel(writer, sheet_name='Distribuição por Situação')
            
            st.download_button(
                label="📥 Exportar Distribuição por Situação",
                data=buffer_situacao.getvalue(),
                file_name=f"distribuicao_situacao_{datetime.now().strftime('%Y%m%d')}.xlsx",
                mime="application/vnd.ms-excel",
                key="export_situacao"
            )
            
            st.markdown("---")
            
            # Tabela de Distribuição por Vendedor
            st.markdown("### 👨‍💼 Distribuição por Vendedor")
            
            # Adicionar coluna de status ao dataframe
            df_sem_compra['status_vendedor'] = df_sem_compra['data_desligamento'].apply(
                lambda x: 'Vendedor Desligado' if pd.notnull(x) else 'Vendedor Ativo'
            )
            
            # Criar nome completo com status
            df_sem_compra['vendedor_completo'] = df_sem_compra['nome_vendedor'] + ' (' + df_sem_compra['status_vendedor'] + ')'
            
            # Criar tabela pivot com vendedor x meses
            tabela_vendedor = df_sem_compra.groupby(['vendedor_completo', 'meses_sem_compra']).size().reset_index(name='quantidade')
            pivot_vendedor = tabela_vendedor.pivot(index='vendedor_completo', columns='meses_sem_compra', values='quantidade').fillna(0).astype(int)
            
            # Adicionar totais
            pivot_vendedor['Total'] = pivot_vendedor.sum(axis=1)
            pivot_vendedor.loc['Total Geral'] = pivot_vendedor.sum()
            
            # Renomear colunas (meses)
            pivot_vendedor.columns = [f"{int(col)} {'mês' if col == 1 else 'meses'}" if col != 'Total' else 'Total' 
                                     for col in pivot_vendedor.columns]
            
            # Ordenar por Total (decrescente), mantendo Total Geral no final
            if 'Total Geral' in pivot_vendedor.index:
                total_row = pivot_vendedor.loc[['Total Geral']]
                other_rows = pivot_vendedor.drop('Total Geral').sort_values('Total', ascending=False)
                pivot_vendedor = pd.concat([other_rows, total_row])
            
            # Exibir tabela estilizada
            st.dataframe(
                pivot_vendedor.style.background_gradient(cmap='Reds', axis=1, subset=[col for col in pivot_vendedor.columns if col != 'Total'])
                                    .format("{:,.0f}")
                                    .applymap(lambda x: 'font-weight: bold', subset=['Total'])
                                    .apply(lambda x: ['font-weight: bold' if x.name == 'Total Geral' else '' for _ in x], axis=1),
                use_container_width=True
            )
            
            # Botão de Exportação - Distribuição por Vendedor
            buffer_vendedor = io.BytesIO()
            with pd.ExcelWriter(buffer_vendedor, engine='xlsxwriter') as writer:
                pivot_vendedor.to_excel(writer, sheet_name='Distribuição por Vendedor')
            
            st.download_button(
                label="📥 Exportar Distribuição por Vendedor",
                data=buffer_vendedor.getvalue(),
                file_name=f"distribuicao_vendedor_{datetime.now().strftime('%Y%m%d')}.xlsx",
                mime="application/vnd.ms-excel",
                key="export_vendedor"
            )
            
            st.markdown("---")
            
            # Tabela de Distribuição por Cidade
            st.markdown("### 🏙️ Distribuição por Cidade")
            
            # Criar tabela pivot com cidade x meses
            tabela_cidade = df_sem_compra.groupby(['cidade', 'meses_sem_compra']).size().reset_index(name='quantidade')
            pivot_cidade = tabela_cidade.pivot(index='cidade', columns='meses_sem_compra', values='quantidade').fillna(0).astype(int)
            
            # Adicionar totais
            pivot_cidade['Total'] = pivot_cidade.sum(axis=1)
            pivot_cidade.loc['Total Geral'] = pivot_cidade.sum()
            
            # Renomear colunas (meses)
            pivot_cidade.columns = [f"{int(col)} {'mês' if col == 1 else 'meses'}" if col != 'Total' else 'Total' 
                                   for col in pivot_cidade.columns]
            
            # Ordenar por Total (decrescente), mantendo Total Geral no final
            if 'Total Geral' in pivot_cidade.index:
                total_row = pivot_cidade.loc[['Total Geral']]
                other_rows = pivot_cidade.drop('Total Geral').sort_values('Total', ascending=False)
                pivot_cidade = pd.concat([other_rows, total_row])
            
            # Exibir tabela estilizada
            st.dataframe(
                pivot_cidade.style.background_gradient(cmap='Blues', axis=1, subset=[col for col in pivot_cidade.columns if col != 'Total'])
                                    .format("{:,.0f}")
                                    .applymap(lambda x: 'font-weight: bold', subset=['Total'])
                                    .apply(lambda x: ['font-weight: bold' if x.name == 'Total Geral' else '' for _ in x], axis=1),
                use_container_width=True
            )
            
            # Botão de Exportação - Distribuição por Cidade
            buffer_cidade = io.BytesIO()
            with pd.ExcelWriter(buffer_cidade, engine='xlsxwriter') as writer:
                pivot_cidade.to_excel(writer, sheet_name='Distribuição por Cidade')
            
            st.download_button(
                label="📥 Exportar Distribuição por Cidade",
                data=buffer_cidade.getvalue(),
                file_name=f"distribuicao_cidade_{datetime.now().strftime('%Y%m%d')}.xlsx",
                mime="application/vnd.ms-excel",
                key="export_cidade"
            )
            
            st.markdown("---")
            
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
    
    # Botão de contato via WhatsApp
    st.markdown("---")
    whatsapp_number = "5534992182544"  # Formato: código do país + DDD + número
    whatsapp_message = "Olá! Tenho uma dúvida sobre o relatório de Clientes sem Compra."
    whatsapp_url = f"https://wa.me/{whatsapp_number}?text={whatsapp_message.replace(' ', '%20')}"
    
    col1, col2, col3 = st.columns([1, 2, 1])
    with col2:
        st.markdown(f"""
        <div style="text-align: center;">
            <a href="{whatsapp_url}" target="_blank">
                <button style="
                    background-color: #25D366;
                    color: white;
                    padding: 12px 24px;
                    border: none;
                    border-radius: 8px;
                    font-size: 16px;
                    font-weight: bold;
                    cursor: pointer;
                    display: inline-flex;
                    align-items: center;
                    gap: 8px;
                    box-shadow: 0 2px 4px rgba(0,0,0,0.2);
                ">
                    <span style="font-size: 20px;">💬</span>
                    Contato via WhatsApp
                </button>
            </a>
        </div>
        """, unsafe_allow_html=True)
    
    st.markdown("<p style='text-align: center; color: gray; font-size: 12px; margin-top: 10px;'>📱 +55 (34) 99218-2544</p>", unsafe_allow_html=True)

if __name__ == "__main__":
    main()
