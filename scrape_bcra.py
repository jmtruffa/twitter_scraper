# bcra_daily_parser.py
import os, json, base64, requests, re
from pathlib import Path
from datetime import datetime
from zoneinfo import ZoneInfo
from openai import OpenAI

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

# --- Config global ---
BA_TZ = ZoneInfo("America/Argentina/Buenos_Aires")
X_API_BASE = "https://api.twitter.com/2"
USERNAME = "BancoCentral_AR"
SAVE_DIR = Path("./bcra_imagenes")
CACHE_FILE = Path(".bcra_x_cache.json")


# =========================================================
# === FUNCIONES DE X.COM =================================
# =========================================================

def auth_headers() -> dict:
    token = os.getenv("X_BEARER_TOKEN")
    if not token:
        raise RuntimeError("Falta X_BEARER_TOKEN en el entorno.")
    return {"Authorization": f"Bearer {token}"}

def ba_today():
    return datetime.now(BA_TZ).date()

def is_today_ba(iso_ts: str) -> bool:
    dt = datetime.fromisoformat(iso_ts.replace("Z", "+00:00")).astimezone(BA_TZ)
    return dt.date() == ba_today()

def matches_signature(text: str) -> bool:
    t = text.lower()
    return "#databcra" in t  # laxo pero suficiente

def get_user_id(username: str) -> str:
    if CACHE_FILE.exists():
        cache = json.loads(CACHE_FILE.read_text())
        if cache.get("username") == username and "user_id" in cache:
            return cache["user_id"]

    url = f"{X_API_BASE}/users/by/username/{username}"
    r = requests.get(url, headers=auth_headers(), timeout=30)
    r.raise_for_status()
    user_id = r.json()["data"]["id"]
    CACHE_FILE.write_text(json.dumps({"username": username, "user_id": user_id}))
    return user_id

def download_bcra_image() -> Path:
    """Descarga la imagen diaria del tweet #DataBCRA."""
    SAVE_DIR.mkdir(parents=True, exist_ok=True)
    user_id = get_user_id(USERNAME)

    url = f"{X_API_BASE}/users/{user_id}/tweets"
    params = {
        "max_results": "10",
        "tweet.fields": "created_at,text,attachments",
        "expansions": "attachments.media_keys",
        "media.fields": "url,type"
    }
    r = requests.get(url, headers=auth_headers(), params=params, timeout=30)
    r.raise_for_status()
    payload = r.json()

    tweets = payload.get("data", [])
    media_index = {m["media_key"]: m for m in payload.get("includes", {}).get("media", [])}

    chosen = None
    for tw in tweets:
        if not is_today_ba(tw.get("created_at", "")):
            continue
        if not matches_signature(tw.get("text", "")):
            continue
        media_keys = tw.get("attachments", {}).get("media_keys", [])
        photos = [
            media_index[mk]["url"]
            for mk in media_keys
            if mk in media_index and media_index[mk].get("type") == "photo" and media_index[mk].get("url")
        ]
        if photos:
            chosen = {"tweet": tw, "photos": photos}
            break

    if not chosen:
        raise RuntimeError("No encontré tuit de HOY con #DataBCRA y foto.")

    img_url = chosen["photos"][0]
    fname = f"bcra_{ba_today().isoformat()}.jpg"
    out_path = SAVE_DIR / fname

    ir = requests.get(img_url, headers={"User-Agent": "Mozilla/5.0"}, timeout=60)
    ir.raise_for_status()
    out_path.write_bytes(ir.content)

    print(f"✅ Imagen descargada: {out_path}")
    return out_path


# =========================================================
# === FUNCIONES DE PARSEO CON OPENAI ======================
# =========================================================

def parse_bcra_image_with_openai(img_path: Path) -> dict:
    """Envía la imagen a OpenAI y devuelve el JSON estructurado."""
    client = OpenAI()  # usa OPENAI_API_KEY del entorno

    # detectar tipo MIME
    ext = img_path.suffix.lower()
    mime = "image/jpeg" if ext in [".jpg", ".jpeg"] else "image/png"

    # convertir a base64
    b64 = base64.b64encode(img_path.read_bytes()).decode("utf-8")
    data_url = f"data:{mime};base64,{b64}"

    # --- cargar prompt desde archivo ./prompt.txt ---
    prompt_file = Path(__file__).parent / "prompt.txt"
    if not prompt_file.exists():
        raise FileNotFoundError("No se encontró prompt.txt en el mismo directorio del script.")
    prompt = prompt_file.read_text(encoding="utf-8").strip()

    # prompt = (
    #     "Procesá la imagen y devolvé SOLO un JSON con los campos: "
    #     "'fecha' (yyyy-mm-dd), 'reservas_millones_usd' (float), "
    #     "'compra_venta_divisas_millones_usd' (float). "
    #     "Si Compra/Venta dice 'Sin intervención', devolvé 0.0. "
    #     "Si dice Venta de divisas en millones de USD, usá ese valor negativo."
    #     "Si dice Compra de divisas en millones de USD, usá ese valor positivo."
    #     "No incluyas texto fuera del JSON."
    # )

    resp = client.chat.completions.create(
        model="gpt-4o-mini",
        temperature=0,
        response_format={"type": "json_object"},
        messages=[
            {"role": "system", "content": "Sos un extractor que devuelve JSON válido y nada más."},
            {
                "role": "user",
                "content": [
                    {"type": "text", "text": prompt},
                    {"type": "image_url", "image_url": {"url": data_url}},
                ],
            },
        ],
    )

    out = resp.choices[0].message.content.strip()
    try:
        data = json.loads(out)
    except Exception:
        raise RuntimeError(f"No se recibió JSON válido: {out}")
    return data


# =========================================================
# === DB: INSERCIÓN EN ambas tablas =======================
# =========================================================

def build_engine() -> Engine:
    db_user = os.environ.get('POSTGRES_USER')
    db_password = os.environ.get('POSTGRES_PASSWORD')
    db_host = os.environ.get('POSTGRES_HOST')
    db_port = os.environ.get('POSTGRES_PORT', '5432')
    db_name = os.environ.get('POSTGRES_DB')

    if not all([db_user, db_password, db_host, db_name]):
        raise RuntimeError("Faltan variables de entorno de Postgres (POSTGRES_USER/PASSWORD/HOST/DB).")

    DATABASE_URL = f"postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"
    return create_engine(DATABASE_URL)

def save_reservas_to_db(engine: Engine, parsed: dict) -> int:
    """
    Inserta en la tabla reservas_scrape (date, valor)
    el valor 'reservas_millones_usd' que vino del JSON.
    - date: parsed['fecha'] (yyyy-mm-dd)
    - valor: float
    Devuelve la cantidad de filas insertadas (1 si OK).
    """
    fecha = parsed.get("fecha")
    valor = parsed.get("reservas_millones_usd", None)

    if not fecha:
        raise ValueError("El JSON no trae 'fecha'.")
    if valor is None:
        raise ValueError("El JSON no trae 'reservas_millones_usd'.")

    # Validar formato de fecha
    try:
        _ = datetime.strptime(fecha, "%Y-%m-%d").date()
    except Exception:
        raise ValueError(f"Fecha inválida: {fecha}")

    # Forzar float
    try:
        valor = float(valor)
    except Exception:
        raise ValueError(f"Valor de reservas_millones_usd inválido: {valor}")

    insert_sql = text("""
        INSERT INTO public.reservas_scrape (date, valor)
        VALUES (:fecha, :valor)
    """)

    with engine.begin() as conn:
        res = conn.execute(insert_sql, {"fecha": fecha, "valor": valor})
        return res.rowcount if (res.rowcount is not None and res.rowcount >= 0) else 1


def save_compra_venta_to_db(engine: Engine, parsed: dict) -> int:
    """
    Inserta en la tabla "comprasMULCBCRA" (date, "comprasBCRA")
    el valor 'compra_venta_divisas_millones_usd' que vino del JSON.
    - date: parsed['fecha'] (yyyy-mm-dd)
    - "comprasBCRA": float (0.0 si no vino)
    Devuelve la cantidad de filas insertadas (1 si OK).
    """
    fecha = parsed.get("fecha")  # 'yyyy-mm-dd'
    valor = parsed.get("compra_venta_divisas_millones_usd", 0.0)

    if not fecha:
        raise ValueError("El JSON no trae 'fecha'.")

    try:
        # Validación básica de formato (yyyy-mm-dd)
        _ = datetime.strptime(fecha, "%Y-%m-%d").date()
    except Exception:
        raise ValueError(f"Fecha inválida: {fecha}")

    # Forzar float
    try:
        valor = float(valor)
    except Exception:
        raise ValueError(f"Valor de compra_venta_divisas_millones_usd inválido: {valor}")

    # IMPORTANTE: usar identificadores entre comillas para preservar mayúsculas
    # Tabla: "comprasMULCBCRA"; columna: "comprasBCRA"
    # ponemos upsert para evitar duplicados si se corre varias veces el mismo día
    insert_sql = text("""
        INSERT INTO "public"."comprasMULCBCRA" (date, "comprasBCRA")
        VALUES (:fecha, :valor)
        ON CONFLICT (date) DO UPDATE SET "comprasBCRA" = EXCLUDED."comprasBCRA";
    """)

    with engine.begin() as conn:
        res = conn.execute(insert_sql, {"fecha": fecha, "valor": valor})
        # res.rowcount puede venir como -1 en algunos drivers; consideramos 1 si no falla
        return res.rowcount if (res.rowcount is not None and res.rowcount >= 0) else 1


# =========================================================
# === MAIN ===============================================
# =========================================================

def main():
    print("=== Descargando imagen del BCRA desde X ===")
    img_path = download_bcra_image()

    print("=== Enviando imagen a OpenAI para parseo ===")
    parsed = parse_bcra_image_with_openai(img_path)
    print("JSON parseado:")
    print(json.dumps(parsed, indent=2, ensure_ascii=False))

    print("=== Guardando en Postgres ===")
    engine = build_engine()

    n1 = save_compra_venta_to_db(engine, parsed)
    print(f"✅ Inserted {n1} rows into \"comprasMULCBCRA\"")

    n2 = save_reservas_to_db(engine, parsed)
    print(f"✅ Inserted {n2} rows into reservas_scrape")
    print("=== Proceso finalizado ===")

if __name__ == "__main__":
    main()
