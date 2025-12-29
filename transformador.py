# VERSION: 1.10
# DESCRIPCIÓN: Transformador Final - FID + CSV + GitHub (Mismo Repositorio)
import json
import requests
import pandas as pd
from datetime import datetime
import os
import subprocess

# CONFIGURACIÓN
TOKEN = "93a08dfed86745127ec4657503daddbc4112cc35170f15a1414c8c96"
URL_STATUS = f"https://gps.geoaustralchile.cl/api/devices/status?auth={TOKEN}"
PATH_BASE = "/home/benja/geoaustral_processor"
OUTPUT_FILE = os.path.join(PATH_BASE, "flota_geoaustral.geojson")

def cargar_csv_equipos():
    """Mapeo por patente desde tu equipos.csv"""
    mapeo = {}
    try:
        df = pd.read_csv(os.path.join(PATH_BASE, "equipos.csv"), sep=None, engine='python')
        df.columns = [c.strip().lower() for c in df.columns]
        for _, fila in df.iterrows():
            pat_limpia = str(fila.get('patente', '')).strip().upper()
            if pat_limpia:
                mapeo[pat_limpia] = {
                    "equipo": str(fila.get('equipo', 'Equipo')),
                    "interno": str(fila.get('num_interno', 'S/N'))
                }
        print(f"📖 CSV Leído: {len(mapeo)} equipos listos.")
    except Exception as e:
        print(f"⚠️ Aviso: No se pudo cruzar con CSV: {e}")
    return mapeo

def push_github():
    """Sube los cambios al repositorio existente"""
    try:
        os.chdir(PATH_BASE)
        # 1. Agregamos solo el GeoJSON para no ensuciar el repo con temporales
        subprocess.run(["git", "add", "flota_geoaustral.geojson"], check=True)
        # 2. Commit con timestamp
        fecha_msg = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        msg = f"Auto-update flota: {fecha_msg}"
        subprocess.run(["git", "commit", "-m", msg], check=True)
        # 3. Push a la rama principal
        subprocess.run(["git", "push"], check=True)
        print(f"🚀 Datos sincronizados en GitHub ({fecha_msg})")
    except Exception as e:
        print(f"❌ Error al subir a GitHub: {e}")
        print("💡 Tip: Verifica que tengas internet y que el token de Git siga activo.")

def procesar():
    base_datos = cargar_csv_equipos()
    try:
        print(f"📥 [{datetime.now().strftime('%H:%M:%S')}] Obteniendo datos de Geoaustral...")
        response = requests.get(URL_STATUS, timeout=15)
        data = response.json()
        
        geojson = {"type": "FeatureCollection", "features": []}

        for item in data.get('data', []):
            v_data = item.get('vehicle', {})
            # Nuestra llave FID y Enganche
            fid_tecnico = v_data.get('id', 0)
            patente_api = str(v_data.get('name', '')).strip().upper()
            
            gps = item.get('gpsknit', {})
            info = base_datos.get(patente_api, {"equipo": "Desconocido", "interno": "S/N"})

            if gps.get('lat') and gps.get('lon'):
                lat = float(gps.get('lat')) / 100000
                lon = float(gps.get('lon')) / 100000

                feature = {
                    "type": "Feature",
                    "id": fid_tecnico,
                    "geometry": {"type": "Point", "coordinates": [lon, lat]},
                    "properties": {
                        "fid": fid_tecnico,
                        "patente": patente_api,
                        "equipo": info["equipo"],
                        "interno": info["interno"],
                        "ignicion": "ENCENDIDO" if item.get('io_ignicion', {}).get('value') == 1 else "APAGADO",
                        "velocidad": f"{item.get('speed', 0)} km/h",
                        "ultima_rx": datetime.fromtimestamp(float(item.get('lastrx', {}).get('value', 0))).strftime('%H:%M %d/%m')
                    }
                }
                geojson["features"].append(feature)

        with open(OUTPUT_FILE, 'w', encoding='utf-8') as f:
            json.dump(geojson, f, indent=4, ensure_ascii=False)
        
        # Ejecutar la subida
        push_github()

    except Exception as e:
        print(f"❌ Error en el proceso: {e}")

if __name__ == "__main__":
    procesar()
