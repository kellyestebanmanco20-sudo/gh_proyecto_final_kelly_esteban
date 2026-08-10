import streamlit as st
import pandas as pd
import os
from datetime import datetime

# Configuración de la página
st.set_page_config(
    page_title="La Positiva - Gobierno de Datos",
    page_icon="🛡️",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Estilos CSS personalizados para estética premium (colores corporativos, bordes redondeados y tipografía limpia)
st.markdown("""
<style>
    .main {
        background-color: #f8f9fa;
    }
    h1 {
        color: #0b3c5d;
        font-family: 'Outfit', sans-serif;
        font-weight: 700;
    }
    .metric-card {
        background-color: #ffffff;
        padding: 20px;
        border-radius: 12px;
        box-shadow: 0 4px 6px rgba(0,0,0,0.05);
        border-left: 5px solid #00a8cc;
        margin-bottom: 20px;
    }
    .metric-title {
        color: #6c757d;
        font-size: 14px;
        text-transform: uppercase;
        font-weight: 600;
    }
    .metric-value {
        color: #0b3c5d;
        font-size: 32px;
        font-weight: bold;
    }
</style>
""", unsafe_allow_html=True)

# --- CONEXIÓN A DATABRICKS ---
@st.cache_resource
def get_spark():
    try:
        from pyspark.sql import SparkSession
        return SparkSession.builder.getOrCreate()
    except Exception:
        return None

WAREHOUSE_ID = "4dd2eee58725c00e"

def execute_query(query, is_write=False):
    # 1. Spark (funciona en clúster/notebook)
    spark = get_spark()
    if spark:
        if is_write:
            spark.sql(query)
            return None
        return spark.sql(query).toPandas()

    # 2. SDK de Databricks — auth automática dentro de Apps
    try:
        from databricks.sdk import WorkspaceClient
        from databricks.sdk.service.sql import StatementState, Disposition, Format
        w = WorkspaceClient()
        stmt = w.statement_execution.execute_statement(
            warehouse_id=WAREHOUSE_ID,
            statement=query,
            wait_timeout="50s",
            disposition=Disposition.INLINE,
            format=Format.JSON_ARRAY,
        )
        if stmt.status.state != StatementState.SUCCEEDED:
            raise Exception(f"SQL error: {stmt.status.error}")
        if is_write:
            return None
        if not stmt.result or not stmt.result.data_array:
            return pd.DataFrame()
        cols = [c.name for c in stmt.manifest.schema.columns]
        return pd.DataFrame(stmt.result.data_array, columns=cols)
    except Exception:
        pass

    # 3. SQL Connector con token env var
    try:
        from databricks import sql as dbsql
        host = os.getenv("DATABRICKS_HOST", "adbsmartdata010826ke.azuredatabricks.net")
        http_path = os.getenv("DATABRICKS_HTTP_PATH", "/sql/1.0/warehouses/4dd2eee58725c00e")
        token = os.getenv("DATABRICKS_TOKEN", "")
        if not token:
            raise Exception("sin token")
        with dbsql.connect(server_hostname=host, http_path=http_path, access_token=token) as conn:
            with conn.cursor() as cursor:
                cursor.execute(query)
                if is_write:
                    return None
                rows = cursor.fetchall()
                if not rows:
                    return pd.DataFrame()
                cols = [d[0] for d in cursor.description]
                return pd.DataFrame(rows, columns=cols)
    except Exception:
        pass

    # 4. Sin conexión — aviso y retorno vacío
    st.warning("⚠️ Sin conexión. Verifica permisos del service principal `app-4k1p2j gobierno-datos-positiva` sobre catalog_au.bronze.")
    return pd.DataFrame()

# --- HELPERS UI + SQL ---
st.sidebar.image("https://upload.wikimedia.org/wikipedia/commons/e/e4/Shield-flat.svg", width=70)
st.sidebar.title("Gobierno de Datos")
st.sidebar.markdown("**Catálogo:** `catalog_au.bronze`")
st.sidebar.markdown("**App:** Gestión CRUD Bronze")

for key in ["edit_concepto", "edit_regla", "edit_trazabilidad"]:
    if key not in st.session_state:
        st.session_state[key] = None

def sql_str(value):
    if value is None:
        return "NULL"
    value = str(value).strip()
    if value == "":
        return "NULL"
    return "'" + value.replace("'", "''") + "'"

def sql_num(value):
    if value is None or value == "":
        return "NULL"
    return str(value)

def run_write(query):
    execute_query(query, is_write=True)
    st.cache_data.clear()

def load_table(table_name, order_by="ingestion_date DESC"):
    query = f"SELECT * FROM catalog_au.bronze.{table_name}"
    if order_by:
        query += f" ORDER BY {order_by}"
    return execute_query(query)

@st.cache_data(ttl=20)
def get_kpis():
    conceptos = execute_query("SELECT COUNT(*) AS total FROM catalog_au.bronze.conceptos_negocio")
    reglas = execute_query("SELECT COUNT(*) AS total FROM catalog_au.bronze.reglas_calidad")
    traza = execute_query("SELECT COUNT(*) AS total FROM catalog_au.bronze.trazabilidad")
    return {
        "conceptos": int(conceptos.iloc[0]["total"]) if not conceptos.empty else 0,
        "reglas": int(reglas.iloc[0]["total"]) if not reglas.empty else 0,
        "trazabilidad": int(traza.iloc[0]["total"]) if not traza.empty else 0,
    }

def render_selectable_table(df, key, pk_col, edit_state_key, delete_sql_builder):
    if df.empty:
        st.info("No hay registros para mostrar.")
        return
    event = st.dataframe(
        df,
        use_container_width=True,
        hide_index=True,
        selection_mode="single-row",
        on_select="rerun",
        key=key,
    )
    rows = event.selection.rows if event and event.selection else []
    if rows:
        selected = df.iloc[rows[0]].to_dict()
        st.info(f"Fila seleccionada: {pk_col} = {selected.get(pk_col)}")
        c1, c2, _ = st.columns([1.2, 1.2, 6])
        with c1:
            if st.button("Editar", key=f"btn_edit_{key}", use_container_width=True):
                st.session_state[edit_state_key] = selected
                st.rerun()
        with c2:
            if st.button("Eliminar", key=f"btn_delete_{key}", use_container_width=True):
                run_write(delete_sql_builder(selected))
                st.success("Registro eliminado correctamente.")
                st.rerun()

@st.dialog("Insertar Concepto de Negocio", width="large")
def modal_insert_concepto():
    with st.form("form_insert_concepto", clear_on_submit=True):
        c1, c2 = st.columns(2)
        with c1:
            codigo = st.text_input("Código entidad del dato *", placeholder="CRM00001")
            concepto = st.text_input("Concepto de negocio *")
            termino = st.text_input("Término de negocio *")
            dominio = st.text_input("Nombre del dominio")
            subdominio = st.text_input("Nombre del subdominio")
            data_owner = st.text_input("Data owner")
        with c2:
            caso_uso = st.text_input("Caso de uso")
            tipo_dato = st.text_input("Tipo de dato")
            prioridad = st.selectbox("Prioridad", ["", "Alta", "Media", "Baja"])
            dato_critico = st.selectbox("Dato crítico", ["", "SI", "NO"])
            personal = st.selectbox("Personal", ["", "SI", "NO"])
            sensible = st.selectbox("Sensible", ["", "SI", "NO"])
        definicion = st.text_area("Definición de negocio")
        submitted = st.form_submit_button("Guardar", type="primary", use_container_width=True)
        if submitted:
            if not codigo or not concepto or not termino:
                st.error("Completa los campos obligatorios.")
            else:
                query = f"""
                INSERT INTO catalog_au.bronze.conceptos_negocio (
                    caso_de_uso, codigo_de_dominio, nombre_del_dominio, codigo_del_subdominio,
                    nombre_del_subdominio, nombre_del_subdominio_omg, concepto_de_negocio,
                    termino_de_negocio, codigo_de_entidad_del_dato, data_owner, dato_critico,
                    personal, sensible, definicion_de_negocio, tipo_de_dato,
                    indicadores_empleados_en_el_calculo, logica_de_calculo,
                    ejemplo_de_valores_del_entidad_del_dato, prioridad_del_entidad_de_dato,
                    indicador_dimension, periodicidad_de_generacion, areas_usuarias_del_dato,
                    validado_por_data_owner, responsable_de_la_actualizacion_en_el_diccionario_negocio,
                    ingestion_date
                ) VALUES (
                    {sql_str(caso_uso)}, NULL, {sql_str(dominio)}, NULL,
                    {sql_str(subdominio)}, NULL, {sql_str(concepto)},
                    {sql_str(termino)}, {sql_str(codigo)}, {sql_str(data_owner)}, {sql_str(dato_critico)},
                    {sql_str(personal)}, {sql_str(sensible)}, {sql_str(definicion)}, {sql_str(tipo_dato)},
                    NULL, NULL, NULL, {sql_str(prioridad)},
                    NULL, NULL, NULL,
                    NULL, NULL,
                    CURRENT_TIMESTAMP()
                )
                """
                run_write(query)
                st.success("Concepto insertado correctamente.")
                st.rerun()

@st.dialog("Editar Concepto de Negocio", width="large")
def modal_edit_concepto(row):
    with st.form("form_edit_concepto"):
        codigo = row.get("codigo_de_entidad_del_dato", "")
        c1, c2 = st.columns(2)
        with c1:
            concepto = st.text_input("Concepto de negocio", value=row.get("concepto_de_negocio", "") or "")
            termino = st.text_input("Término de negocio", value=row.get("termino_de_negocio", "") or "")
            dominio = st.text_input("Nombre del dominio", value=row.get("nombre_del_dominio", "") or "")
            subdominio = st.text_input("Nombre del subdominio", value=row.get("nombre_del_subdominio", "") or "")
        with c2:
            data_owner = st.text_input("Data owner", value=row.get("data_owner", "") or "")
            tipo_dato = st.text_input("Tipo de dato", value=row.get("tipo_de_dato", "") or "")
            prioridad = st.text_input("Prioridad", value=row.get("prioridad_del_entidad_de_dato", "") or "")
            dato_critico = st.text_input("Dato crítico", value=row.get("dato_critico", "") or "")
        definicion = st.text_area("Definición de negocio", value=row.get("definicion_de_negocio", "") or "")
        submitted = st.form_submit_button("Guardar cambios", type="primary", use_container_width=True)
        if submitted:
            query = f"""
            UPDATE catalog_au.bronze.conceptos_negocio
            SET concepto_de_negocio = {sql_str(concepto)},
                termino_de_negocio = {sql_str(termino)},
                nombre_del_dominio = {sql_str(dominio)},
                nombre_del_subdominio = {sql_str(subdominio)},
                data_owner = {sql_str(data_owner)},
                tipo_de_dato = {sql_str(tipo_dato)},
                prioridad_del_entidad_de_dato = {sql_str(prioridad)},
                dato_critico = {sql_str(dato_critico)},
                definicion_de_negocio = {sql_str(definicion)},
                ingestion_date = CURRENT_TIMESTAMP()
            WHERE codigo_de_entidad_del_dato = {sql_str(codigo)}
            """
            run_write(query)
            st.session_state.edit_concepto = None
            st.success("Concepto actualizado correctamente.")
            st.rerun()

@st.dialog("Insertar Regla de Calidad", width="large")
def modal_insert_regla():
    with st.form("form_insert_regla", clear_on_submit=True):
        c1, c2 = st.columns(2)
        with c1:
            id_regla = st.text_input("ID regla de calidad *")
            termino = st.text_input("Término de negocio *")
            descripcion = st.text_area("Descripción de regla")
            tipo_regla = st.text_input("Tipo de regla de calidad")
        with c2:
            principio = st.text_input("Principio de calidad asociado")
            umbral_superior = st.number_input("Umbral superior", value=None, format="%.4f")
            umbral_inferior = st.number_input("Umbral inferior", value=None, format="%.4f")
            data_owner = st.text_input("Data owner")
        submitted = st.form_submit_button("Guardar", type="primary", use_container_width=True)
        if submitted:
            if not id_regla or not termino:
                st.error("Completa los campos obligatorios.")
            else:
                query = f"""
                INSERT INTO catalog_au.bronze.reglas_calidad (
                    iniciativa, caso_de_uso, id_regla_de_calidad, termino_de_negocio,
                    tabla_del_entidad_de_dato_omg, campo_del_entidad_de_dato_omg,
                    nombre_del_entidad_de_dato, descripcion_de_regla_de_calidad,
                    tipo_de_regla_de_calidad, cumplimiento_regla_de_calidad_core,
                    principio_de_calidad_asociado, umbral_superior, umbral_inferior,
                    periodicidad, aplicacion, data_owner, validado_por_data_owner, ingestion_date
                ) VALUES (
                    NULL, NULL, {sql_str(id_regla)}, {sql_str(termino)},
                    NULL, NULL,
                    NULL, {sql_str(descripcion)},
                    {sql_str(tipo_regla)}, NULL,
                    {sql_str(principio)}, {sql_num(umbral_superior)}, {sql_num(umbral_inferior)},
                    NULL, NULL, {sql_str(data_owner)}, NULL, CURRENT_TIMESTAMP()
                )
                """
                run_write(query)
                st.success("Regla insertada correctamente.")
                st.rerun()

@st.dialog("Editar Regla de Calidad", width="large")
def modal_edit_regla(row):
    with st.form("form_edit_regla"):
        pk = row.get("id_regla_de_calidad", "")
        termino = st.text_input("Término de negocio", value=row.get("termino_de_negocio", "") or "")
        descripcion = st.text_area("Descripción", value=row.get("descripcion_de_regla_de_calidad", "") or "")
        tipo_regla = st.text_input("Tipo de regla", value=row.get("tipo_de_regla_de_calidad", "") or "")
        principio = st.text_input("Principio de calidad", value=row.get("principio_de_calidad_asociado", "") or "")
        us = st.text_input("Umbral superior", value="" if pd.isna(row.get("umbral_superior")) else str(row.get("umbral_superior")))
        ui = st.text_input("Umbral inferior", value="" if pd.isna(row.get("umbral_inferior")) else str(row.get("umbral_inferior")))
        submitted = st.form_submit_button("Guardar cambios", type="primary", use_container_width=True)
        if submitted:
            query = f"""
            UPDATE catalog_au.bronze.reglas_calidad
            SET termino_de_negocio = {sql_str(termino)},
                descripcion_de_regla_de_calidad = {sql_str(descripcion)},
                tipo_de_regla_de_calidad = {sql_str(tipo_regla)},
                principio_de_calidad_asociado = {sql_str(principio)},
                umbral_superior = {sql_num(us)},
                umbral_inferior = {sql_num(ui)},
                ingestion_date = CURRENT_TIMESTAMP()
            WHERE id_regla_de_calidad = {sql_str(pk)}
            """
            run_write(query)
            st.session_state.edit_regla = None
            st.success("Regla actualizada correctamente.")
            st.rerun()

@st.dialog("Insertar Trazabilidad", width="large")
def modal_insert_trazabilidad():
    with st.form("form_insert_trazabilidad", clear_on_submit=True):
        c1, c2 = st.columns(2)
        with c1:
            codigo = st.text_input("Código entidad del dato *")
            nombre_entidad = st.text_input("Nombre entidad del dato")
            aplicativos = st.text_input("Aplicativos")
            fuente_oficial = st.text_input("Fuente oficial")
        with c2:
            tabla_fuente = st.text_input("Tabla en fuente oficial")
            campo_fuente = st.text_input("Campo en fuente oficial")
            data_owner = st.text_input("Data owner")
            periodicidad = st.text_input("Periodicidad de actualización")
        submitted = st.form_submit_button("Guardar", type="primary", use_container_width=True)
        if submitted:
            if not codigo:
                st.error("Completa el código de entidad del dato.")
            else:
                query = f"""
                INSERT INTO catalog_au.bronze.trazabilidad (
                    iniciativa, caso_de_uso, codigo_de_entidad_del_dato,
                    tabla_del_entidad_de_dato_omg, campo_del_entidad_de_dato_omg,
                    nombre_de_entidad_del_dato, llave, aplicativos, fuente_oficial,
                    compania, esquema, tabla_en_fuente_oficial, campo_en_fuente_oficial,
                    personal, sensible, fuentes_y_campos_necesarias_para_el_calculo_o_generacion_del_dato,
                    ruta_u_origen_del_repositorio_servidor, data_entry, periodicidad_de_actualizacion,
                    profundidad_de_datos, formato_del_dato, longitud, es_llave_primaria,
                    data_owner, data_custodian, validado_por_data_custodian,
                    fecha_de_actualizacion_en_el_diccionario_tecnico,
                    actualizacion_realizada_en_el_diccionario_tecnico,
                    motivo_de_actualizacion_en_el_diccionario_tecnico,
                    responsable_de_la_actualizacion_en_el_diccionario_tecnico,
                    ingestion_date
                ) VALUES (
                    NULL, NULL, {sql_str(codigo)},
                    NULL, NULL,
                    {sql_str(nombre_entidad)}, NULL, {sql_str(aplicativos)}, {sql_str(fuente_oficial)},
                    NULL, NULL, {sql_str(tabla_fuente)}, {sql_str(campo_fuente)},
                    NULL, NULL, NULL,
                    NULL, NULL, {sql_str(periodicidad)},
                    NULL, NULL, NULL, NULL,
                    {sql_str(data_owner)}, NULL, NULL,
                    NULL, NULL, NULL, NULL,
                    CURRENT_TIMESTAMP()
                )
                """
                run_write(query)
                st.success("Registro de trazabilidad insertado correctamente.")
                st.rerun()

@st.dialog("Editar Trazabilidad", width="large")
def modal_edit_trazabilidad(row):
    with st.form("form_edit_trazabilidad"):
        pk = row.get("codigo_de_entidad_del_dato", "")
        nombre_entidad = st.text_input("Nombre entidad del dato", value=row.get("nombre_de_entidad_del_dato", "") or "")
        aplicativos = st.text_input("Aplicativos", value=row.get("aplicativos", "") or "")
        fuente_oficial = st.text_input("Fuente oficial", value=row.get("fuente_oficial", "") or "")
        tabla_fuente = st.text_input("Tabla en fuente oficial", value=row.get("tabla_en_fuente_oficial", "") or "")
        campo_fuente = st.text_input("Campo en fuente oficial", value=row.get("campo_en_fuente_oficial", "") or "")
        data_owner = st.text_input("Data owner", value=row.get("data_owner", "") or "")
        periodicidad = st.text_input("Periodicidad", value=row.get("periodicidad_de_actualizacion", "") or "")
        submitted = st.form_submit_button("Guardar cambios", type="primary", use_container_width=True)
        if submitted:
            query = f"""
            UPDATE catalog_au.bronze.trazabilidad
            SET nombre_de_entidad_del_dato = {sql_str(nombre_entidad)},
                aplicativos = {sql_str(aplicativos)},
                fuente_oficial = {sql_str(fuente_oficial)},
                tabla_en_fuente_oficial = {sql_str(tabla_fuente)},
                campo_en_fuente_oficial = {sql_str(campo_fuente)},
                data_owner = {sql_str(data_owner)},
                periodicidad_de_actualizacion = {sql_str(periodicidad)},
                ingestion_date = CURRENT_TIMESTAMP()
            WHERE codigo_de_entidad_del_dato = {sql_str(pk)}
            """
            run_write(query)
            st.session_state.edit_trazabilidad = None
            st.success("Trazabilidad actualizada correctamente.")
            st.rerun()

st.title("🛡️ Gobierno de Datos — Gestión Bronze")
st.caption("Dashboard y mantenimiento transaccional de Conceptos de Negocio, Reglas de Calidad y Trazabilidad.")

tab1, tab2, tab3, tab4 = st.tabs([
    "📊 Reporte",
    "📘 Conceptos de Negocio",
    "✅ Reglas de Calidad",
    "🧭 Trazabilidad",
])

with tab1:
    kpis = get_kpis()
    c1, c2, c3 = st.columns(3)
    c1.markdown(f'<div class="metric-card"><div class="metric-title">Conceptos de negocio</div><div class="metric-value">{kpis["conceptos"]}</div></div>', unsafe_allow_html=True)
    c2.markdown(f'<div class="metric-card" style="border-left-color:#00a8cc;"><div class="metric-title">Reglas de calidad</div><div class="metric-value">{kpis["reglas"]}</div></div>', unsafe_allow_html=True)
    c3.markdown(f'<div class="metric-card" style="border-left-color:#32e0c4;"><div class="metric-title">Trazabilidad</div><div class="metric-value">{kpis["trazabilidad"]}</div></div>', unsafe_allow_html=True)

    left, right = st.columns(2)
    with left:
        st.markdown("### Conceptos por dominio")
        df_dom = execute_query("""
            SELECT COALESCE(nombre_del_dominio,'Sin dominio') AS nombre_del_dominio, COUNT(*) AS total
            FROM catalog_au.bronze.conceptos_negocio
            GROUP BY COALESCE(nombre_del_dominio,'Sin dominio')
            ORDER BY total DESC
            LIMIT 10
        """)
        if not df_dom.empty:
            st.bar_chart(df_dom.set_index(df_dom.columns[0]))
        else:
            st.info("Sin datos para el gráfico.")
    with right:
        st.markdown("### Reglas por principio de calidad")
        df_pr = execute_query("""
            SELECT COALESCE(principio_de_calidad_asociado,'Sin principio') AS principio, COUNT(*) AS total
            FROM catalog_au.bronze.reglas_calidad
            GROUP BY COALESCE(principio_de_calidad_asociado,'Sin principio')
            ORDER BY total DESC
            LIMIT 10
        """)
        if not df_pr.empty:
            st.bar_chart(df_pr.set_index(df_pr.columns[0]))
        else:
            st.info("Sin datos para el gráfico.")

with tab2:
    h1, h2 = st.columns([8, 2])
    h1.subheader("Conceptos de Negocio")
    if h2.button("Insertar", key="insert_concepto", type="primary", use_container_width=True):
        modal_insert_concepto()
    if st.session_state.edit_concepto:
        modal_edit_concepto(st.session_state.edit_concepto)
    df_con = load_table("conceptos_negocio")
    render_selectable_table(
        df_con, "tbl_conceptos", "codigo_de_entidad_del_dato", "edit_concepto",
        lambda row: f"DELETE FROM catalog_au.bronze.conceptos_negocio WHERE codigo_de_entidad_del_dato = {sql_str(row.get('codigo_de_entidad_del_dato'))}"
    )

with tab3:
    h1, h2 = st.columns([8, 2])
    h1.subheader("Reglas de Calidad")
    if h2.button("Insertar", key="insert_regla", type="primary", use_container_width=True):
        modal_insert_regla()
    if st.session_state.edit_regla:
        modal_edit_regla(st.session_state.edit_regla)
    df_reg = load_table("reglas_calidad")
    render_selectable_table(
        df_reg, "tbl_reglas", "id_regla_de_calidad", "edit_regla",
        lambda row: f"DELETE FROM catalog_au.bronze.reglas_calidad WHERE id_regla_de_calidad = {sql_str(row.get('id_regla_de_calidad'))}"
    )

with tab4:
    h1, h2 = st.columns([8, 2])
    h1.subheader("Trazabilidad")
    if h2.button("Insertar", key="insert_trazabilidad", type="primary", use_container_width=True):
        modal_insert_trazabilidad()
    if st.session_state.edit_trazabilidad:
        modal_edit_trazabilidad(st.session_state.edit_trazabilidad)
    df_trz = load_table("trazabilidad")
    render_selectable_table(
        df_trz, "tbl_trazabilidad", "codigo_de_entidad_del_dato", "edit_trazabilidad",
        lambda row: f"DELETE FROM catalog_au.bronze.trazabilidad WHERE codigo_de_entidad_del_dato = {sql_str(row.get('codigo_de_entidad_del_dato'))}"
    )

st.markdown("---")
st.info("Los cambios se escriben directamente en las tablas Bronze y se reflejan automáticamente al recargar la app.")
