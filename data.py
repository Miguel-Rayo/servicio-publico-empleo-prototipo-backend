# explorar_campos.py
# ESTO NO ES NECESARIO PARA EL PROYECTO, SOLO ES UN SCRIPT DE EXPLORACIÓN DE LOS DATOS PRELIMINAR
import requests
import json
from collections import defaultdict

URL = "https://www.buscadordeempleo.gov.co/backbue/v1//vacantes/resultados?page={page}&"

# Traemos 3 páginas para ver variedad de datos
todos = []
for page in range(1, 4):
    print(f"Consultando página {page}...")
    r = requests.get(URL.format(page=page), timeout=30)
    data = r.json()
    todos.extend(data["resultados"])

print(f"\nTotal registros obtenidos: {len(todos)}")

# Analizar campos
print("\n=== CAMPOS Y TIPOS ===")
primera = todos[0]
for key, value in primera.items():
    tipo = type(value).__name__
    preview = str(value)[:80]
    print(f"  {key:<40} [{tipo:<10}]  →  {preview}")

# Detectar nulos o vacíos por campo
print("\n=== CAMPOS CON VALORES NULOS O VACÍOS ===")
nulos = defaultdict(int)
for v in todos:
    for key, val in v.items():
        if val is None or val == "" or val == 0:
            nulos[key] += 1

for key, count in sorted(nulos.items(), key=lambda x: -x[1]):
    pct = count / len(todos) * 100
    print(f"  {key:<40} {count:>4} nulos/vacíos  ({pct:.1f}%)")

# Ver un ejemplo de DETALLES_PRESTADOR
print("\n=== EJEMPLO DETALLES_PRESTADOR ===")
print(json.dumps(todos[0].get("DETALLES_PRESTADOR"), indent=2, ensure_ascii=False))

# Cardinalidad de campos categóricos clave
print("\n=== VALORES ÚNICOS EN CAMPOS CLAVE ===")
campos_categoricos = [
    "NIVEL_ESTUDIOS", "TIPO_CONTRATO", "DEPARTAMENTO",
    "SECTOR_ECONOMICO", "TELETRABAJO", "DISCAPACIDAD",
    "RANGO_SALARIAL", "HIDROCARBUROS"
]
for campo in campos_categoricos:
    valores = set(v.get(campo) for v in todos if v.get(campo))
    print(f"\n  {campo} ({len(valores)} únicos):")
    for val in sorted(valores):
        print(f"    - {val}")