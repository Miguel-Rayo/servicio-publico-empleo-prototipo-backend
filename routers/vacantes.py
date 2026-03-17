# routers/vacantes.py
from fastapi import APIRouter, Query
from database import get_connection

router = APIRouter(prefix="/api/vacantes", tags=["vacantes"])


@router.get("/resumen")
def resumen():
    """KPIs principales para el dashboard"""
    conn = get_connection()
    cursor = conn.cursor(dictionary=True)

    cursor.execute("SELECT COUNT(*) AS total FROM vacantes")
    total = cursor.fetchone()["total"]

    cursor.execute("SELECT COUNT(*) AS total FROM vacantes WHERE teletrabajo = 1")
    teletrabajo = cursor.fetchone()["total"]

    cursor.execute("SELECT COUNT(*) AS total FROM vacantes WHERE discapacidad = 1")
    discapacidad = cursor.fetchone()["total"]

    cursor.execute("SELECT SUM(cantidad_vacantes) AS total FROM vacantes")
    plazas = cursor.fetchone()["total"]

    cursor.close()
    conn.close()

    return {
        "total_vacantes":       total,
        "total_plazas":         plazas,
        "con_teletrabajo":      teletrabajo,
        "para_discapacidad":    discapacidad,
    }


@router.get("/por-departamento")
def por_departamento():
    """Distribución de vacantes por departamento"""
    conn = get_connection()
    cursor = conn.cursor(dictionary=True)
    cursor.execute("""
        SELECT departamento, COUNT(*) AS total
        FROM vacantes
        WHERE departamento IS NOT NULL
        GROUP BY departamento
        ORDER BY total DESC
    """)
    data = cursor.fetchall()
    cursor.close()
    conn.close()
    return data


@router.get("/por-salario")
def por_salario():
    """Distribución por rango salarial"""
    conn = get_connection()
    cursor = conn.cursor(dictionary=True)
    cursor.execute("""
        SELECT rango_salarial, COUNT(*) AS total
        FROM vacantes
        WHERE rango_salarial IS NOT NULL
        GROUP BY rango_salarial
        ORDER BY total DESC
    """)
    data = cursor.fetchall()
    cursor.close()
    conn.close()
    return data


@router.get("/por-nivel-estudios")
def por_nivel_estudios():
    """Distribución por nivel de estudios requerido"""
    conn = get_connection()
    cursor = conn.cursor(dictionary=True)
    cursor.execute("""
        SELECT nivel_estudios, COUNT(*) AS total
        FROM vacantes
        WHERE nivel_estudios IS NOT NULL
        GROUP BY nivel_estudios
        ORDER BY total DESC
    """)
    data = cursor.fetchall()
    cursor.close()
    conn.close()
    return data


@router.get("/por-sector")
def por_sector():
    """Top 15 sectores económicos"""
    conn = get_connection()
    cursor = conn.cursor(dictionary=True)
    cursor.execute("""
        SELECT sector_economico, COUNT(*) AS total
        FROM vacantes
        WHERE sector_economico IS NOT NULL
        GROUP BY sector_economico
        ORDER BY total DESC
        LIMIT 15
    """)
    data = cursor.fetchall()
    cursor.close()
    conn.close()
    return data


@router.get("/por-contrato")
def por_contrato():
    """Distribución por tipo de contrato"""
    conn = get_connection()
    cursor = conn.cursor(dictionary=True)
    cursor.execute("""
        SELECT tipo_contrato, COUNT(*) AS total
        FROM vacantes
        WHERE tipo_contrato IS NOT NULL
        GROUP BY tipo_contrato
        ORDER BY total DESC
    """)
    data = cursor.fetchall()
    cursor.close()
    conn.close()
    return data


@router.get("/listado")
def listado(
    page:           int = Query(1, ge=1),
    limit:          int = Query(20, le=100),
    departamento:   str = Query(None),
    nivel_estudios: str = Query(None),
    tipo_contrato:  str = Query(None),
    rango_salarial: str = Query(None),
):
    """Listado paginado de vacantes con filtros opcionales"""
    conn = get_connection()
    cursor = conn.cursor(dictionary=True)

    filtros = []
    params  = []

    if departamento:
        filtros.append("departamento = %s")
        params.append(departamento)
    if nivel_estudios:
        filtros.append("nivel_estudios = %s")
        params.append(nivel_estudios)
    if tipo_contrato:
        filtros.append("tipo_contrato = %s")
        params.append(tipo_contrato)
    if rango_salarial:
        filtros.append("rango_salarial = %s")
        params.append(rango_salarial)

    where = ("WHERE " + " AND ".join(filtros)) if filtros else ""
    offset = (page - 1) * limit

    cursor.execute(f"SELECT COUNT(*) AS total FROM vacantes {where}", params)
    total = cursor.fetchone()["total"]

    cursor.execute(f"""
        SELECT codigo_vacante, titulo_vacante, cargo, departamento,
               municipio, rango_salarial, tipo_contrato, nivel_estudios,
               cantidad_vacantes, fecha_publicacion, fecha_vencimiento
        FROM vacantes {where}
        ORDER BY fecha_publicacion DESC
        LIMIT %s OFFSET %s
    """, params + [limit, offset])

    vacantes = cursor.fetchall()
    cursor.close()
    conn.close()

    return {
        "total":    total,
        "page":     page,
        "limit":    limit,
        "data":     vacantes
    }

@router.get("/tendencia-diaria")
def tendencia_diaria():
    """Vacantes publicadas por día en los últimos 60 días"""
    conn = get_connection()
    cursor = conn.cursor(dictionary=True)
    cursor.execute("""
        SELECT 
            DATE_FORMAT(fecha_publicacion, '%Y-%m-%d') AS fecha,
            COUNT(*)                                    AS total,
            SUM(cantidad_vacantes)                      AS plazas
        FROM vacantes
        WHERE fecha_publicacion >= DATE_SUB(CURDATE(), INTERVAL 60 DAY)
          AND fecha_publicacion IS NOT NULL
        GROUP BY fecha_publicacion
        ORDER BY fecha_publicacion ASC
    """)
    data = cursor.fetchall()
    cursor.close()
    conn.close()
    return data


@router.get("/por-municipio")
def por_municipio():
    """Top 30 municipios con más vacantes"""
    conn = get_connection()
    cursor = conn.cursor(dictionary=True)
    cursor.execute("""
        SELECT 
            municipio,
            departamento,
            COUNT(*)           AS total,
            SUM(cantidad_vacantes) AS plazas
        FROM vacantes
        WHERE municipio IS NOT NULL
        GROUP BY municipio, departamento
        ORDER BY total DESC
        LIMIT 30
    """)
    data = cursor.fetchall()
    cursor.close()
    conn.close()
    return data


@router.get("/salario-por-departamento")
def salario_por_departamento():
    """Distribución salarial cruzada por departamento"""
    conn = get_connection()
    cursor = conn.cursor(dictionary=True)
    cursor.execute("""
        SELECT 
            departamento,
            rango_salarial,
            COUNT(*) AS total
        FROM vacantes
        WHERE departamento IS NOT NULL
          AND rango_salarial IS NOT NULL
          AND rango_salarial != 'A Convenir'
        GROUP BY departamento, rango_salarial
        ORDER BY departamento, total DESC
    """)
    data = cursor.fetchall()
    cursor.close()
    conn.close()
    return data


@router.get("/experiencia-vs-estudios")
def experiencia_vs_estudios():
    """Cruce entre meses de experiencia requerida y nivel de estudios"""
    conn = get_connection()
    cursor = conn.cursor(dictionary=True)
    cursor.execute("""
        SELECT
            nivel_estudios,
            CASE
                WHEN meses_experiencia_cargo = 0              THEN 'Sin experiencia'
                WHEN meses_experiencia_cargo BETWEEN 1 AND 6  THEN '1-6 meses'
                WHEN meses_experiencia_cargo BETWEEN 7 AND 12 THEN '7-12 meses'
                WHEN meses_experiencia_cargo BETWEEN 13 AND 24 THEN '1-2 años'
                WHEN meses_experiencia_cargo BETWEEN 25 AND 60 THEN '2-5 años'
                ELSE 'Más de 5 años'
            END AS rango_experiencia,
            COUNT(*) AS total
        FROM vacantes
        WHERE nivel_estudios IS NOT NULL
          AND meses_experiencia_cargo IS NOT NULL
        GROUP BY nivel_estudios, rango_experiencia
        ORDER BY nivel_estudios, total DESC
    """)
    data = cursor.fetchall()
    cursor.close()
    conn.close()
    return data


@router.get("/top-prestadores")
def top_prestadores():
    """Top 20 prestadores con más vacantes publicadas"""
    conn = get_connection()
    cursor = conn.cursor(dictionary=True)
    cursor.execute("""
        SELECT
            p.nombre_prestador,
            COUNT(DISTINCT p.codigo_vacante)        AS total_vacantes,
            SUM(v.cantidad_vacantes)                AS total_plazas,
            COUNT(DISTINCT v.departamento)          AS departamentos_cubiertos
        FROM prestadores p
        JOIN vacantes v ON p.codigo_vacante = v.codigo_vacante
        WHERE p.nombre_prestador IS NOT NULL
        GROUP BY p.nombre_prestador
        ORDER BY total_vacantes DESC
        LIMIT 20
    """)
    data = cursor.fetchall()
    cursor.close()
    conn.close()
    return data


@router.get("/inclusion")
def inclusion():
    """Análisis de vacantes para población vulnerable"""
    conn = get_connection()
    cursor = conn.cursor(dictionary=True)

    # Totales generales
    cursor.execute("""
        SELECT
            SUM(discapacidad)                           AS vacantes_discapacidad,
            SUM(teletrabajo)                            AS vacantes_teletrabajo,
            SUM(hidrocarburos)                          AS vacantes_hidrocarburos,
            SUM(plaza_practica)                         AS plazas_practica,
            COUNT(*)                                    AS total
        FROM vacantes
    """)
    totales = cursor.fetchone()

    # Discapacidad por departamento
    cursor.execute("""
        SELECT departamento, COUNT(*) AS total
        FROM vacantes
        WHERE discapacidad = 1
        GROUP BY departamento
        ORDER BY total DESC
        LIMIT 15
    """)
    discapacidad_dep = cursor.fetchall()

    # Teletrabajo por sector
    cursor.execute("""
        SELECT sector_economico, COUNT(*) AS total
        FROM vacantes
        WHERE teletrabajo = 1
          AND sector_economico IS NOT NULL
        GROUP BY sector_economico
        ORDER BY total DESC
        LIMIT 10
    """)
    teletrabajo_sector = cursor.fetchall()

    # Nivel estudios en vacantes de discapacidad
    cursor.execute("""
        SELECT nivel_estudios, COUNT(*) AS total
        FROM vacantes
        WHERE discapacidad = 1
          AND nivel_estudios IS NOT NULL
        GROUP BY nivel_estudios
        ORDER BY total DESC
    """)
    discapacidad_estudios = cursor.fetchall()

    cursor.close()
    conn.close()

    return {
        "totales":              totales,
        "discapacidad_por_departamento": discapacidad_dep,
        "teletrabajo_por_sector":        teletrabajo_sector,
        "discapacidad_por_estudios":     discapacidad_estudios,
    }


@router.get("/brecha-sectorial")
def brecha_sectorial():
    """Sectores con mayor concentración de vacantes vs distribución geográfica"""
    conn = get_connection()
    cursor = conn.cursor(dictionary=True)
    cursor.execute("""
        SELECT
            sector_economico,
            COUNT(*)                        AS total_vacantes,
            SUM(cantidad_vacantes)          AS total_plazas,
            COUNT(DISTINCT departamento)    AS departamentos,
            COUNT(DISTINCT municipio)       AS municipios,
            ROUND(AVG(meses_experiencia_cargo), 1) AS experiencia_promedio
        FROM vacantes
        WHERE sector_economico IS NOT NULL
        GROUP BY sector_economico
        ORDER BY total_vacantes DESC
        LIMIT 20
    """)
    data = cursor.fetchall()
    cursor.close()
    conn.close()
    return data