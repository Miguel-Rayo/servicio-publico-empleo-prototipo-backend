# etl.py
import requests
import mysql.connector
from datetime import datetime
from tqdm import tqdm
import time
import random
import json
import os

# ─────────────────────────────────────────
# CONFIGURACIÓN
# ─────────────────────────────────────────
DB_CONFIG = {
    "host":     "localhost",
    "user":     "root",
    "password": "root",
    "database": "servicio_empleo",
    "charset":  "utf8mb4"
}

API_URL        = "https://www.buscadordeempleo.gov.co/backbue/v1//vacantes/resultados?page={page}&"
MAX_PAGES      = 5670
DELAY_MIN      = 0.3    # segundos mínimo entre peticiones ...... 266,052 fueron los registros procesados
DELAY_MAX      = 1.2    # segundos máximo entre peticiones
MAX_REINTENTOS = 3      # reintentos por página si falla la red
PROGRESO_FILE  = "progreso.json"  # archivo donde guardamos el avance

HEADERS = {
    "User-Agent":      "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Accept":          "application/json, text/plain, */*",
    "Accept-Language": "es-CO,es;q=0.9,en;q=0.8",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection":      "keep-alive",
    "Referer":         "https://www.buscadordeempleo.gov.co/",
}


# ─────────────────────────────────────────
# GESTIÓN DE PROGRESO
# ─────────────────────────────────────────
def cargar_progreso():
    """Lee el archivo de progreso. Si no existe, empieza desde página 1."""
    if os.path.exists(PROGRESO_FILE):
        with open(PROGRESO_FILE, "r") as f:
            data = json.load(f)
        print(f"  → Progreso anterior encontrado: última página completada = {data['ultima_pagina']}")
        print(f"  → Vacantes procesadas hasta ahora: {data['insertados']}")
        return data
    return {"ultima_pagina": 0, "insertados": 0, "errores": 0}

def guardar_progreso(ultima_pagina, insertados, errores):
    """Guarda el progreso después de cada página completada."""
    with open(PROGRESO_FILE, "w") as f:
        json.dump({
            "ultima_pagina": ultima_pagina,
            "insertados":    insertados,
            "errores":       errores,
            "actualizado":   datetime.now().isoformat()
        }, f, indent=2)

def borrar_progreso():
    """Limpia el archivo de progreso al finalizar exitosamente."""
    if os.path.exists(PROGRESO_FILE):
        os.remove(PROGRESO_FILE)


# ─────────────────────────────────────────
# LIMPIEZA DE DATOS
# ─────────────────────────────────────────
def limpiar_fecha(fecha_str):
    if not fecha_str:
        return None
    try:
        return datetime.strptime(fecha_str[:10], "%Y-%m-%d").date()
    except:
        return None

def limpiar_sector(sector):
    if not sector:
        return None
    reemplazos = {
        "EducaciÃ³n":      "Educación",
        "AdministraciÃ³n": "Administración",
        "pÃºblica":        "pública",
        "TransportaciÃ³n": "Transportación",
        "tuberÃ­as":       "tuberías",
        "Ã³":              "ó",
        "Ã­":              "í",
        "Ã©":              "é",
        "Ãº":              "ú",
        "Ã¡":              "á",
        "Ã":               "Í",
    }
    for mal, bien in reemplazos.items():
        sector = sector.replace(mal, bien)
    return sector.strip()


# ─────────────────────────────────────────
# UPSERT
# ─────────────────────────────────────────
SQL_VACANTE = """
    INSERT INTO vacantes (
        codigo_vacante, titulo_vacante, descripcion_vacante,
        nivel_estudios, rango_salarial, departamento, municipio,
        tipo_contrato, cantidad_vacantes, cargo, sector_economico,
        teletrabajo, discapacidad, hidrocarburos, plaza_practica,
        meses_experiencia_cargo, fecha_vencimiento, fecha_publicacion
    ) VALUES (
        %s, %s, %s, %s, %s, %s, %s,
        %s, %s, %s, %s,
        %s, %s, %s, %s,
        %s, %s, %s
    )
    ON DUPLICATE KEY UPDATE
        titulo_vacante          = VALUES(titulo_vacante),
        descripcion_vacante     = VALUES(descripcion_vacante),
        nivel_estudios          = VALUES(nivel_estudios),
        rango_salarial          = VALUES(rango_salarial),
        departamento            = VALUES(departamento),
        municipio               = VALUES(municipio),
        tipo_contrato           = VALUES(tipo_contrato),
        cantidad_vacantes       = VALUES(cantidad_vacantes),
        cargo                   = VALUES(cargo),
        sector_economico        = VALUES(sector_economico),
        teletrabajo             = VALUES(teletrabajo),
        discapacidad            = VALUES(discapacidad),
        hidrocarburos           = VALUES(hidrocarburos),
        plaza_practica          = VALUES(plaza_practica),
        meses_experiencia_cargo = VALUES(meses_experiencia_cargo),
        fecha_vencimiento       = VALUES(fecha_vencimiento),
        fecha_publicacion       = VALUES(fecha_publicacion),
        fecha_carga             = CURRENT_TIMESTAMP
"""

SQL_PRESTADOR        = "INSERT INTO prestadores (codigo_vacante, nombre_prestador, url_detalle_vacante) VALUES (%s, %s, %s)"
SQL_BORRAR_PRESTADORES = "DELETE FROM prestadores WHERE codigo_vacante = %s"


# ─────────────────────────────────────────
# PETICIÓN CON REINTENTOS
# ─────────────────────────────────────────
def fetch_pagina(session, page):
    """Intenta obtener una página hasta MAX_REINTENTOS veces."""
    for intento in range(1, MAX_REINTENTOS + 1):
        try:
            r = session.get(API_URL.format(page=page), headers=HEADERS, timeout=30)
            r.raise_for_status()
            return r.json()
        except Exception as e:
            if intento < MAX_REINTENTOS:
                espera = intento * 3  # espera 3s, 6s, 9s entre reintentos
                tqdm.write(f"  ⚠ Página {page} — intento {intento}/{MAX_REINTENTOS} falló: {e}. Reintentando en {espera}s...")
                time.sleep(espera)
            else:
                tqdm.write(f"  ✗ Página {page} — todos los reintentos fallaron: {e}")
                return None


# ─────────────────────────────────────────
# PROCESAR UNA VACANTE
# ─────────────────────────────────────────
def procesar_vacante(cursor, vacante):
    codigo = vacante.get("CODIGO_VACANTE")
    if not codigo:
        return

    cursor.execute(SQL_VACANTE, (
        codigo,
        vacante.get("TITULO_VACANTE"),
        vacante.get("DESCRIPCION_VACANTE"),
        vacante.get("NIVEL_ESTUDIOS"),
        vacante.get("RANGO_SALARIAL"),
        vacante.get("DEPARTAMENTO"),
        vacante.get("MUNICIPIO"),
        vacante.get("TIPO_CONTRATO"),
        vacante.get("CANTIDAD_VACANTES"),
        vacante.get("CARGO"),
        limpiar_sector(vacante.get("SECTOR_ECONOMICO")),
        int(vacante.get("TELETRABAJO") or 0),
        int(vacante.get("DISCAPACIDAD") or 0),
        int(vacante.get("HIDROCARBUROS") or 0),
        int(vacante.get("PLAZA_PRACTICA") or 0),
        vacante.get("MESES_EXPERIENCIA_CARGO"),
        limpiar_fecha(vacante.get("FECHA_VENCIMIENTO")),
        limpiar_fecha(vacante.get("FECHA_PUBLICACION")),
    ))

    cursor.execute(SQL_BORRAR_PRESTADORES, (codigo,))
    for p in vacante.get("DETALLES_PRESTADOR", []):
        cursor.execute(SQL_PRESTADOR, (
            codigo,
            p.get("NOMBRE_PRESTADOR"),
            p.get("URL_DETALLE_VACANTE"),
        ))


# ─────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────
def main():
    progreso   = cargar_progreso()
    desde      = progreso["ultima_pagina"] + 1
    insertados = progreso["insertados"]
    errores    = progreso["errores"]

    if desde > MAX_PAGES:
        print("✓ La descarga ya estaba completa. Borra progreso.json si quieres reiniciar.")
        return

    print(f"\nConectando a MySQL...")
    conn    = mysql.connector.connect(**DB_CONFIG)
    cursor  = conn.cursor()
    session = requests.Session()

    paginas_restantes = MAX_PAGES - desde + 1
    print(f"Descargando desde página {desde} hasta {MAX_PAGES} ({paginas_restantes} páginas restantes)...\n")

    try:
        for page in tqdm(range(desde, MAX_PAGES + 1), desc="Páginas", unit="pág"):
            data = fetch_pagina(session, page)

            if data is None:
                errores += 1
                guardar_progreso(page - 1, insertados, errores)
                continue

            vacantes = data.get("resultados", [])

            for vacante in vacantes:
                try:
                    procesar_vacante(cursor, vacante)
                    insertados += 1
                except Exception as e:
                    errores += 1
                    tqdm.write(f"  ⚠ Error en vacante {vacante.get('CODIGO_VACANTE')}: {e}")

            conn.commit()
            guardar_progreso(page, insertados, errores)

            # Delay aleatorio para no ser predecible
            time.sleep(random.uniform(DELAY_MIN, DELAY_MAX))

    except KeyboardInterrupt:
        # El usuario presionó Ctrl+C — guardar progreso y salir limpiamente
        print("\n\n  Interrupción manual detectada. Progreso guardado.")
        print(f"  Última página completada guardada. Ejecuta el script de nuevo para continuar.")

    finally:
        cursor.close()
        conn.close()
        session.close()

    # Si llegó hasta aquí sin interrupción, borrar el archivo de progreso
    if desde <= MAX_PAGES:
        pagina_final = progreso.get("ultima_pagina", MAX_PAGES)
        if pagina_final >= MAX_PAGES:
            borrar_progreso()
            print("\n✓ Descarga completa. Archivo de progreso eliminado.")

    print(f"\n{'─'*40}")
    print(f"  ✓ Vacantes procesadas : {insertados:,}")
    print(f"  ✗ Errores             : {errores:,}")
    print(f"{'─'*40}")


if __name__ == "__main__":
    main()