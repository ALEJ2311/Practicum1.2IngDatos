import sqlite3
import unicodedata
import pandas as pd

DB = 'macroentorno_silver.db'


# ---------------------------------------------------------------------------
# Utilidades
# ---------------------------------------------------------------------------
def tablas_existentes(con):
    q = "SELECT name FROM sqlite_master WHERE type='table'"
    return set(pd.read_sql(q, con)['name'].tolist())


def columnas(con, tabla):
    return [r[1] for r in con.execute(f"PRAGMA table_info('{tabla}')").fetchall()]


def normaliza_texto(s):
    """Mayúsculas, sin tildes y sin espacios extra — clave de cruce geográfico."""
    if pd.isna(s):
        return None
    s = str(s).strip().upper()
    s = ''.join(c for c in unicodedata.normalize('NFD', s)
                if unicodedata.category(c) != 'Mn')
    return ' '.join(s.split())


def cargar(con, df, nombre):
    df.to_sql(nombre, con, if_exists='replace', index=False)
    print(f"Éxito: '{nombre}' cargada con {len(df)} registros.")


# ---------------------------------------------------------------------------
# 1. dim_geografia  (dimensión compartida VAB + Censo + Supercias + MINEDUC)
# ---------------------------------------------------------------------------
def construir_dim_geografia(con):
    print("Construyendo dim_geografia...")
    tablas = tablas_existentes(con)
    frames = []

    fuentes = [
        ('fact_vab_geografico', 'provincia', 'canton', 'codigo_provincia', 'codigo_canton'),
        ('fact_directorio_companias', 'provincia', 'canton', None, None),
        ('fact_ranking_empresarial', 'provincia', None, None, None),
        ('fact_mineduc_amie', 'provincia', 'canton', 'cod_provincia', 'cod_canton'),
    ]

    for tabla, cp, cc, kp, kc in fuentes:
        if tabla not in tablas:
            continue
        cols_tabla = columnas(con, tabla)
        sel = [c for c in [cp, cc, kp, kc] if c and c in cols_tabla]
        if cp not in cols_tabla:
            continue
        df = pd.read_sql(f"SELECT DISTINCT {', '.join(sel)} FROM {tabla}", con)
        df = df.rename(columns={cp: 'provincia', cc: 'canton',
                                kp: 'cod_provincia', kc: 'cod_canton'})
        for col in ['provincia', 'canton', 'cod_provincia', 'cod_canton']:
            if col not in df.columns:
                df[col] = None
        frames.append(df[['provincia', 'canton', 'cod_provincia', 'cod_canton']])

    if not frames:
        print("  Aviso: no se encontraron fuentes con geografía.")
        return

    geo = pd.concat(frames, ignore_index=True)
    geo['provincia_norm'] = geo['provincia'].map(normaliza_texto)
    geo['canton_norm'] = geo['canton'].map(normaliza_texto)
    geo = geo.dropna(subset=['provincia_norm'])

    geo = geo[~geo['provincia_norm'].isin(['ZONA EN ESTUDIO', 'ZONA NO DELIMITADA',
                                           'NACIONAL', 'TOTAL', 'TOTAL NACIONAL'])]

    geo = (geo.sort_values(['cod_provincia', 'cod_canton'])
           .drop_duplicates(subset=['provincia_norm', 'canton_norm'], keep='first')
           .reset_index(drop=True))
    geo.insert(0, 'id_geo', range(1, len(geo) + 1))

    cargar(con, geo, 'dim_geografia')


# ---------------------------------------------------------------------------
# 2. dim_tiempo (dimensión temporal compartida)
# ---------------------------------------------------------------------------
def construir_dim_tiempo(con):
    print("Construyendo dim_tiempo...")
    tablas = tablas_existentes(con)
    fechas = set()
    anios = set()

    for tabla in ['fact_petroleo_diario', 'fact_riesgo_diario', 'fact_expectativas_iee']:
        if tabla in tablas:
            cols = columnas(con, tabla)
            col_f = 'fecha' if 'fecha' in cols else ('Fecha' if 'Fecha' in cols else None)
            if col_f:
                s = pd.read_sql(f"SELECT DISTINCT {col_f} AS fecha FROM {tabla}", con)['fecha']
                s = pd.to_datetime(s, errors='coerce').dropna()
                fechas.update(s.dt.normalize().tolist())

    for tabla in ['fact_macro_anual', 'fact_vab_geografico',
                  'fact_mineduc_bachilleres', 'fact_ranking_empresarial']:
        if tabla in tablas:
            cols = columnas(con, tabla)
            col_anio = next((c for c in cols if c.lower() in
                             ('anio', 'año', 'ano', 'year',
                              'anio_fiscal', 'año_fiscal')), None)
            if col_anio:
                s = pd.read_sql(f'SELECT DISTINCT "{col_anio}" AS anio FROM {tabla}', con)['anio']
                s = pd.to_numeric(s, errors='coerce').dropna().astype(int)
                anios.update(s.tolist())

    filas = []
    for f in sorted(fechas):
        filas.append({'fecha': f.strftime('%Y-%m-%d'), 'anio': f.year,
                      'mes': f.month, 'trimestre': (f.month - 1) // 3 + 1})

    solo_anios = anios - {f.year for f in fechas}
    for a in sorted(solo_anios):
        filas.append({'fecha': f'{a}-01-01', 'anio': a, 'mes': 1, 'trimestre': 1})

    dim = pd.DataFrame(filas).drop_duplicates(subset=['fecha']).sort_values('fecha').reset_index(drop=True)
    dim.insert(0, 'id_tiempo', range(1, len(dim) + 1))
    cargar(con, dim, 'dim_tiempo')


# ---------------------------------------------------------------------------
# 3. Vistas Gold (responden P1, P2 y P3 del dashboard)
# ---------------------------------------------------------------------------
def construir_vistas_gold(con):
    print("Construyendo vistas Gold...")
    tablas = tablas_existentes(con)
    cur = con.cursor()

    # -- P1: gold_pib_tendencia (CORREGIDO ULTRA SEGURO) ------------------
    if 'fact_macro_anual' in tablas:
        cols = columnas(con, 'fact_macro_anual')
        col_var = 'variacion_anual_pib' if 'variacion_anual_pib' in cols else 'variacion_pib_pct'

        # Escáner inteligente de columnas de dinero vs índice
        if 'pib_real_musd' in cols:
            col_pib = 'pib_real_musd'
        elif 'pib_real' in cols:
            col_pib = 'pib_real'
        else:
            col_pib = 'pib_real_indice'

        cur.execute("DROP VIEW IF EXISTS gold_pib_tendencia")
        cur.execute(f"""
            CREATE VIEW gold_pib_tendencia AS
            SELECT
                CAST(anio AS INTEGER)      AS anio,
                {col_pib}                  AS pib_real,
                {col_var}                  AS variacion_pib_pct,
                CASE
                    WHEN {col_var} > 2  THEN 'Crecimiento fuerte'
                    WHEN {col_var} > 0  THEN 'Crecimiento moderado'
                    WHEN {col_var} = 0  THEN 'Estancamiento'
                    ELSE 'Contracción'
                END                        AS clasificacion
            FROM fact_macro_anual
            WHERE anio IS NOT NULL
            ORDER BY anio
        """)
        print(f"  gold_pib_tendencia  -> P1 (Columna detectada y usada: '{col_pib}')")

    # -- P1 apoyo / P2: gold_petroleo_30dias ------------------------------
    if 'fact_petroleo_diario' in tablas:
        cols = columnas(con, 'fact_petroleo_diario')
        col_p = 'precio_wti' if 'precio_wti' in cols else cols[-1]
        cur.execute("DROP VIEW IF EXISTS gold_petroleo_30dias")
        cur.execute(f"""
            CREATE VIEW gold_petroleo_30dias AS
            SELECT
                fecha,
                {col_p} AS precio_wti,
                AVG({col_p}) OVER (
                    ORDER BY fecha ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
                ) AS promedio_movil_30d
            FROM fact_petroleo_diario
            WHERE fecha IS NOT NULL
            ORDER BY fecha
        """)
        print("  gold_petroleo_30dias -> P1/P2 (coyuntura)")

    # -- P2: gold_empleo_tendencia ----------------------------------------
    if 'fact_empleo_enemdu' in tablas:
        cols = columnas(con, 'fact_empleo_enemdu')
        col_area = 'area' if 'area' in cols else None
        col_val = 'valor_tasa' if 'valor_tasa' in cols else None
        col_ind = 'indicador' if 'indicador' in cols else None
        col_per = 'periodo' if 'periodo' in cols else None
        if all([col_area, col_val, col_ind, col_per]):
            cur.execute("DROP VIEW IF EXISTS gold_empleo_tendencia")
            cur.execute(f"""
                CREATE VIEW gold_empleo_tendencia AS
                SELECT
                    {col_per}  AS periodo,
                    {col_ind}  AS indicador,
                    {col_area} AS area,
                    {col_val}  AS valor_tasa
                FROM fact_empleo_enemdu
                WHERE {col_val} IS NOT NULL
                  AND {col_area} = 'nacional'
                ORDER BY {col_per}
            """)
            print("  gold_empleo_tendencia -> P2")

    # -- P2: gold_vab_provincia -------------------------------------------
    if 'fact_vab_geografico' in tablas:
        cur.execute("DROP VIEW IF EXISTS gold_vab_provincia")
        cur.execute("""
            CREATE VIEW gold_vab_provincia AS
            WITH geo_prov AS (
                SELECT UPPER(provincia) AS provincia_norm,
                       MIN(id_geo)      AS id_geo
                FROM dim_geografia
                GROUP BY UPPER(provincia)
            )
            SELECT
                v.anio,
                gp.id_geo,
                v.provincia AS provincia,
                SUM(v.valor) AS vab_total
            FROM fact_vab_geografico v
            LEFT JOIN geo_prov gp
                   ON UPPER(v.provincia) = gp.provincia_norm
            GROUP BY v.anio, v.provincia, gp.id_geo
            ORDER BY v.anio, vab_total DESC
        """)
        print("  gold_vab_provincia -> P2")

    # -- P3: gold_empresas_provincia --------------------------------------
    if 'fact_directorio_companias' in tablas:
        cols = columnas(con, 'fact_directorio_companias')
        col_sit = next((c for c in cols if 'situacion' in c.lower()), None)
        if 'provincia' in cols:
            filtro = (f"WHERE UPPER({col_sit}) LIKE '%ACTIV%'" if col_sit else "")
            cur.execute("DROP VIEW IF EXISTS gold_empresas_provincia")
            cur.execute(f"""
                CREATE VIEW gold_empresas_provincia AS
                SELECT
                    g.id_geo,
                    d.provincia,
                    COUNT(*) AS empresas_activas
                FROM fact_directorio_companias d
                INNER JOIN dim_geografia g
                       ON UPPER(d.provincia) = UPPER(g.provincia)
                {filtro}
                GROUP BY d.provincia, g.id_geo
                ORDER BY empresas_activas DESC
            """)
            print("  gold_empresas_provincia -> P3 (Filtro INNER JOIN aplicado)")

    # -- P3: gold_bachilleres_vs_empresas ---------------------------------
    if 'fact_mineduc_bachilleres' in tablas and 'fact_directorio_companias' in tablas:
        cols_dir = columnas(con, 'fact_directorio_companias')
        col_sit = next((c for c in cols_dir if 'situacion' in c.lower()), None)
        filtro = (f"AND UPPER({col_sit}) LIKE '%ACTIV%'" if col_sit else "")
        cur.execute("DROP VIEW IF EXISTS gold_bachilleres_vs_empresas")
        cur.execute(f"""
            CREATE VIEW gold_bachilleres_vs_empresas AS
            WITH ult AS (SELECT MAX(anio) AS anio FROM fact_mineduc_bachilleres),
            bach AS (
                SELECT UPPER(b.provincia) AS provincia_norm,
                       b.provincia,
                       b.bachilleres_3ro
                FROM fact_mineduc_bachilleres b, ult
                WHERE b.anio = ult.anio
            ),
            emp AS (
                SELECT UPPER(d.provincia) AS provincia_norm,
                       COUNT(*) AS empresas_activas
                FROM fact_directorio_companias d
                WHERE d.provincia IS NOT NULL {filtro}
                GROUP BY UPPER(d.provincia)
            )
            SELECT
                bach.provincia,
                bach.bachilleres_3ro,
                COALESCE(emp.empresas_activas, 0) AS empresas_activas,
                CASE WHEN COALESCE(emp.empresas_activas, 0) = 0 THEN NULL
                     ELSE ROUND(CAST(bach.bachilleres_3ro AS FLOAT)
                                / emp.empresas_activas, 3)
                END AS ratio_bachilleres_por_empresa
            FROM bach
            LEFT JOIN emp ON bach.provincia_norm = emp.provincia_norm
            ORDER BY ratio_bachilleres_por_empresa DESC
        """)
        print("  gold_bachilleres_vs_empresas -> P3")

    con.commit()


# ---------------------------------------------------------------------------
def main():
    con = sqlite3.connect(DB)
    try:
        construir_dim_geografia(con)
        construir_dim_tiempo(con)
        construir_vistas_gold(con)
        print("\nModulo 4 finalizado. Dimensiones y vistas Gold creadas exitosamente.")
    finally:
        con.close()


if __name__ == '__main__':
    main()