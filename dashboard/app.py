#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Dashboard Streamlit Optimizado - Sistema Epidemiológico Tolima
Conectado directamente a PostgreSQL (no más archivos CSV!)
"""

import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import folium
from streamlit_folium import st_folium
from sqlalchemy import create_engine, text
import geopandas as gpd
from datetime import datetime, timedelta
import warnings
warnings.filterwarnings('ignore')

# Configuración de página
st.set_page_config(
    page_title="📊 Sistema Epidemiológico Tolima",
    page_icon="🏥",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Configuración base de datos
DATABASE_URL = "postgresql://tolima_admin:tolima2025!@localhost:5432/epidemiologia_tolima"

@st.cache_resource
def get_database_connection():
    """Conexión cached a PostgreSQL"""
    return create_engine(DATABASE_URL, pool_size=5, max_overflow=10)

@st.cache_data(ttl=3600)  # Cache 1 hora
def load_coberturas_dashboard():
    """Carga datos de coberturas desde PostgreSQL"""
    engine = get_database_connection()
    
    query = """
    SELECT * FROM v_coberturas_dashboard 
    ORDER BY municipio, año DESC, mes DESC, grupo_etario
    """
    
    return pd.read_sql(text(query), engine)

@st.cache_data(ttl=3600)
def load_mapa_coberturas():
    """Carga datos para mapa de coberturas"""
    engine = get_database_connection()
    
    query = """
    SELECT 
        codigo_municipio,
        municipio,
        region,
        total_vacunados,
        poblacion_total,
        cobertura_general,
        vacunados_urbano,
        vacunados_rural,
        ST_AsText(geometria) as wkt_geometry
    FROM v_mapa_coberturas
    """
    
    return pd.read_sql(text(query), engine)

@st.cache_data(ttl=3600)
def load_estadisticas_generales():
    """Estadísticas generales del sistema"""
    engine = get_database_connection()
    
    stats = {}
    
    # Vacunación
    vac_stats = pd.read_sql(text("""
        SELECT 
            COUNT(*) as total_vacunados,
            COUNT(DISTINCT codigo_municipio) as municipios_con_vacunacion,
            COUNT(DISTINCT institucion) as instituciones_activas,
            MAX(fecha_aplicacion) as ultima_actualizacion
        FROM vacunacion_fiebre_amarilla
    """), engine)
    stats['vacunacion'] = vac_stats.iloc[0] if len(vac_stats) > 0 else {}
    
    # Población
    pob_stats = pd.read_sql(text("""
        SELECT 
            SUM(poblacion_total) as poblacion_total_tolima,
            COUNT(DISTINCT codigo_municipio) as municipios_poblacion
        FROM poblacion
    """), engine)
    stats['poblacion'] = pob_stats.iloc[0] if len(pob_stats) > 0 else {}
    
    # Cobertura general
    try:
        cob_general = pd.read_sql(text("""
            SELECT 
                ROUND(
                    (SELECT COUNT(*) FROM vacunacion_fiebre_amarilla) * 100.0 / 
                    NULLIF((SELECT SUM(poblacion_total) FROM poblacion), 0), 2
                ) as cobertura_general_estimada
        """), engine)
        stats['cobertura_general'] = cob_general.iloc[0]['cobertura_general_estimada'] if len(cob_general) > 0 else 0
    except:
        stats['cobertura_general'] = 0
    
    return stats

def crear_grafico_cobertura_temporal(df):
    """Gráfico de evolución temporal de coberturas"""
    # Agrupar por período
    temporal = df.groupby(['año', 'mes']).agg({
        'vacunados': 'sum',
        'poblacion_total': 'sum'
    }).reset_index()
    
    temporal['periodo'] = temporal.apply(
        lambda x: f"{int(x['año'])}-{int(x['mes']):02d}", axis=1
    )
    temporal['cobertura'] = np.where(
        temporal['poblacion_total'] > 0,
        (temporal['vacunados'] / temporal['poblacion_total']) * 100,
        0
    )
    
    fig = px.line(
        temporal.tail(12),  # Últimos 12 meses
        x='periodo',
        y='cobertura',
        title='📈 Evolución Cobertura Vacunación (Últimos 12 Meses)',
        labels={
            'periodo': 'Período',
            'cobertura': 'Cobertura (%)'
        },
        line_shape='spline'
    )
    
    fig.update_layout(
        height=400,
        xaxis_tickangle=-45,
        showlegend=False
    )
    
    # Línea de meta 95%
    fig.add_hline(y=95, line_dash="dash", line_color="red", 
                  annotation_text="Meta 95%")
    
    return fig

def crear_grafico_por_region(df):
    """Gráfico de coberturas por región"""
    por_region = df.groupby('region').agg({
        'vacunados': 'sum',
        'poblacion_total': 'sum'
    }).reset_index()
    
    por_region['cobertura'] = np.where(
        por_region['poblacion_total'] > 0,
        (por_region['vacunados'] / por_region['poblacion_total']) * 100,
        0
    )
    
    # Colores según cobertura
    colores = ['red' if x < 70 else 'orange' if x < 85 else 'green' 
               for x in por_region['cobertura']]
    
    fig = px.bar(
        por_region.sort_values('cobertura', ascending=True),
        x='cobertura',
        y='region',
        orientation='h',
        title='🗺️ Cobertura por Región del Tolima',
        labels={
            'cobertura': 'Cobertura (%)',
            'region': 'Región'
        },
        color='cobertura',
        color_continuous_scale=['red', 'orange', 'green']
    )
    
    fig.update_layout(height=400, showlegend=False)
    
    # Línea de meta
    fig.add_vline(x=95, line_dash="dash", line_color="black",
                  annotation_text="Meta 95%")
    
    return fig

def crear_mapa_interactivo(df_mapa):
    """Mapa interactivo de coberturas por municipio"""
    # Crear mapa base centrado en Tolima
    m = folium.Map(
        location=[4.4389, -75.2322],  # Centro aproximado de Tolima
        zoom_start=8,
        tiles='OpenStreetMap'
    )
    
    # Añadir marcadores por municipio
    for _, row in df_mapa.iterrows():
        # Color según cobertura
        if row['cobertura_general'] >= 95:
            color = 'green'
            icon = 'ok-sign'
        elif row['cobertura_general'] >= 70:
            color = 'orange' 
            icon = 'warning-sign'
        else:
            color = 'red'
            icon = 'remove-sign'
        
        # Popup con información
        popup_html = f"""
        <b>{row['municipio']}</b><br>
        Región: {row['region']}<br>
        <hr>
        🏥 Vacunados: {row['total_vacunados']:,}<br>
        👥 Población: {row['poblacion_total']:,}<br>
        📊 Cobertura: {row['cobertura_general']:.1f}%<br>
        <hr>
        🏙️ Urbano: {row['vacunados_urbano']:,}<br>
        🌾 Rural: {row['vacunados_rural']:,}
        """
        
        folium.Marker(
            location=[4.4389 + np.random.uniform(-1, 1), 
                     -75.2322 + np.random.uniform(-1, 1)],  # Posición aproximada
            popup=folium.Popup(popup_html, max_width=300),
            tooltip=f"{row['municipio']}: {row['cobertura_general']:.1f}%",
            icon=folium.Icon(color=color, icon=icon)
        ).add_to(m)
    
    return m

def crear_distribucion_grupos_etarios(df):
    """Distribución por grupos etarios"""
    grupos = df.groupby('grupo_etario')['vacunados'].sum().sort_values(ascending=False)
    
    fig = px.pie(
        values=grupos.values,
        names=grupos.index,
        title='👥 Distribución Vacunación por Grupos Etarios',
        color_discrete_sequence=px.colors.qualitative.Set3
    )
    
    fig.update_traces(
        textposition='inside', 
        textinfo='percent+label',
        hovertemplate='<b>%{label}</b><br>' +
                      'Vacunados: %{value:,}<br>' +
                      'Porcentaje: %{percent}<br>' +
                      '<extra></extra>'
    )
    
    fig.update_layout(height=500)
    
    return fig

# ================================
# INTERFAZ PRINCIPAL
# ================================

def main():
    """Función principal del dashboard"""
    
    # Título principal
    st.title("📊 Sistema Epidemiológico Tolima")
    st.markdown("**Dashboard de Vigilancia Epidemiológica - Fiebre Amarilla**")
    
    # Sidebar con filtros
    with st.sidebar:
        st.header("🔧 Filtros y Configuración")
        
        # Test de conexión
        try:
            engine = get_database_connection()
            with engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            st.success("✅ Conectado a PostgreSQL")
        except Exception as e:
            st.error(f"❌ Error BD: {str(e)[:50]}...")
            st.stop()
        
        # Fecha de actualización
        st.info("📅 Datos actualizados automáticamente desde PostgreSQL")
        
        # Botón de actualizar cache
        if st.button("🔄 Actualizar Datos"):
            st.cache_data.clear()
            st.experimental_rerun()
    
    # Cargar datos
    with st.spinner("📊 Cargando datos desde PostgreSQL..."):
        stats = load_estadisticas_generales()
        df_coberturas = load_coberturas_dashboard()
        df_mapa = load_mapa_coberturas()
    
    # ================================
    # MÉTRICAS PRINCIPALES
    # ================================
    st.subheader("📈 Métricas Principales")
    
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        vacunados = stats.get('vacunacion', {}).get('total_vacunados', 0)
        st.metric(
            label="💉 Total Vacunados",
            value=f"{vacunados:,}",
            delta=f"Histórico completo"
        )
    
    with col2:
        municipios = stats.get('vacunacion', {}).get('municipios_con_vacunacion', 0)
        st.metric(
            label="📍 Municipios Activos",
            value=f"{municipios}/47",
            delta=f"{(municipios/47)*100:.1f}% cobertura territorial"
        )
    
    with col3:
        poblacion = stats.get('poblacion', {}).get('poblacion_total_tolima', 0)
        st.metric(
            label="👥 Población Tolima",
            value=f"{poblacion:,}",
            delta="Denominador SISBEN"
        )
    
    with col4:
        cobertura_gral = stats.get('cobertura_general', 0)
        delta_color = "normal" if cobertura_gral >= 95 else "inverse"
        st.metric(
            label="📊 Cobertura General",
            value=f"{cobertura_gral:.1f}%",
            delta=f"Meta: 95%",
            delta_color=delta_color
        )
    
    # ================================
    # GRÁFICOS PRINCIPALES
    # ================================
    st.subheader("📊 Análisis de Coberturas")
    
    # Gráficos en dos columnas
    col1, col2 = st.columns(2)
    
    with col1:
        if len(df_coberturas) > 0:
            fig_temporal = crear_grafico_cobertura_temporal(df_coberturas)
            st.plotly_chart(fig_temporal, use_container_width=True)
        else:
            st.warning("⚠️ No hay datos de cobertura disponibles")
    
    with col2:
        if len(df_coberturas) > 0:
            fig_regiones = crear_grafico_por_region(df_coberturas)
            st.plotly_chart(fig_regiones, use_container_width=True)
        else:
            st.warning("⚠️ No hay datos por región disponibles")
    
    # ================================
    # MAPA INTERACTIVO
    # ================================
    st.subheader("🗺️ Mapa Interactivo de Coberturas")
    
    if len(df_mapa) > 0:
        mapa = crear_mapa_interactivo(df_mapa)
        st_folium(mapa, width=1000, height=500)
    else:
        st.warning("⚠️ No hay datos geoespaciales disponibles")
    
    # ================================
    # ANÁLISIS DETALLADO
    # ================================
    st.subheader("📋 Análisis Detallado")
    
    # Tabs para diferentes análisis
    tab1, tab2, tab3, tab4 = st.tabs([
        "👥 Grupos Etarios", 
        "🏙️ Urbano vs Rural", 
        "📊 Top Municipios",
        "🔍 Datos Detallados"
    ])
    
    with tab1:
        if len(df_coberturas) > 0:
            fig_grupos = crear_distribucion_grupos_etarios(df_coberturas)
            st.plotly_chart(fig_grupos, use_container_width=True)
        else:
            st.warning("⚠️ No hay datos de grupos etarios")
    
    with tab2:
        if len(df_coberturas) > 0:
            urbano_rural = df_coberturas.groupby('tipo_ubicacion').agg({
                'vacunados': 'sum',
                'poblacion_total': 'sum'
            }).reset_index()
            
            urbano_rural['cobertura'] = np.where(
                urbano_rural['poblacion_total'] > 0,
                (urbano_rural['vacunados'] / urbano_rural['poblacion_total']) * 100,
                0
            )
            
            fig_ur = px.bar(
                urbano_rural,
                x='tipo_ubicacion',
                y='cobertura',
                title='🏙️ Cobertura Urbano vs Rural',
                labels={
                    'tipo_ubicacion': 'Tipo Ubicación',
                    'cobertura': 'Cobertura (%)'
                },
                color='cobertura',
                color_continuous_scale=['red', 'green']
            )
            
            st.plotly_chart(fig_ur, use_container_width=True)
        else:
            st.warning("⚠️ No hay datos urbano/rural")
    
    with tab3:
        if len(df_mapa) > 0:
            top_municipios = df_mapa.nlargest(10, 'total_vacunados')[
                ['municipio', 'region', 'total_vacunados', 'cobertura_general']
            ].reset_index(drop=True)
            
            st.subheader("🏆 Top 10 Municipios por Vacunación")
            
            # Formatear tabla
            top_municipios['Posición'] = range(1, len(top_municipios) + 1)
            top_municipios = top_municipios[[
                'Posición', 'municipio', 'region', 'total_vacunados', 'cobertura_general'
            ]].rename(columns={
                'municipio': 'Municipio',
                'region': 'Región', 
                'total_vacunados': 'Vacunados',
                'cobertura_general': 'Cobertura (%)'
            })
            
            st.dataframe(
                top_municipios,
                use_container_width=True,
                hide_index=True
            )
        else:
            st.warning("⚠️ No hay datos de municipios")
    
    with tab4:
        st.subheader("📊 Tabla de Datos Detallada")
        
        # Filtros para tabla detallada
        col1, col2, col3 = st.columns(3)
        
        with col1:
            regiones = ['Todas'] + sorted(df_coberturas['region'].unique().tolist())
            region_filtro = st.selectbox("Región", regiones)
        
        with col2:
            ubicaciones = ['Todas'] + sorted(df_coberturas['tipo_ubicacion'].unique().tolist())
            ubicacion_filtro = st.selectbox("Ubicación", ubicaciones)
        
        with col3:
            grupos = ['Todos'] + sorted(df_coberturas['grupo_etario'].unique().tolist())
            grupo_filtro = st.selectbox("Grupo Etario", grupos)
        
        # Aplicar filtros
        df_filtrado = df_coberturas.copy()
        
        if region_filtro != 'Todas':
            df_filtrado = df_filtrado[df_filtrado['region'] == region_filtro]
        
        if ubicacion_filtro != 'Todas':
            df_filtrado = df_filtrado[df_filtrado['tipo_ubicacion'] == ubicacion_filtro]
        
        if grupo_filtro != 'Todos':
            df_filtrado = df_filtrado[df_filtrado['grupo_etario'] == grupo_filtro]
        
        # Mostrar tabla
        if len(df_filtrado) > 0:
            st.dataframe(
                df_filtrado[[
                    'municipio', 'region', 'tipo_ubicacion', 'grupo_etario',
                    'vacunados', 'poblacion_total', 'cobertura_porcentaje'
                ]].rename(columns={
                    'municipio': 'Municipio',
                    'region': 'Región',
                    'tipo_ubicacion': 'Ubicación', 
                    'grupo_etario': 'Grupo Etario',
                    'vacunados': 'Vacunados',
                    'poblacion_total': 'Población',
                    'cobertura_porcentaje': 'Cobertura (%)'
                }),
                use_container_width=True,
                hide_index=True
            )
            
            # Botón de descarga
            csv = df_filtrado.to_csv(index=False, encoding='utf-8-sig')
            st.download_button(
                label="📥 Descargar datos filtrados (CSV)",
                data=csv,
                file_name=f"coberturas_tolima_{datetime.now().strftime('%Y%m%d')}.csv",
                mime="text/csv"
            )
        else:
            st.warning("⚠️ No hay datos que coincidan con los filtros seleccionados")
    
    # ================================
    # FOOTER CON INFO TÉCNICA
    # ================================
    st.markdown("---")
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.caption("🔄 **Actualización**: Automática desde PostgreSQL")
        
    with col2:
        ultima_act = stats.get('vacunacion', {}).get('ultima_actualizacion')
        if ultima_act:
            st.caption(f"📅 **Última carga**: {ultima_act}")
        else:
            st.caption("📅 **Última carga**: No disponible")
    
    with col3:
        st.caption("🔧 **Sistema**: Tolima Epidemiológico v2.0")


if __name__ == "__main__":
    main()