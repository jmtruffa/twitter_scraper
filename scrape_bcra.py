# bcra_daily_parser.py
import os, json, base64, requests, re
import sys, traceback
from pathlib import Path
from datetime import datetime
from zoneinfo import ZoneInfo
from openai import OpenAI
from PIL import Image

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

# --- Config global ---
BA_TZ = ZoneInfo("America/Argentina/Buenos_Aires")
X_API_BASE = "https://api.twitter.com/2"
USERNAME = "BancoCentral_AR"
SAVE_DIR = Path("./bcra_imagenes")
CACHE_FILE = Path(".bcra_x_cache.json")
DEFAULT_OCR_BACKENDS = ["huggingface", "openai"]
_HF_OCR_CACHE = None


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
    fname = f"bcra_{ba_today().isoformat()}.jpg"
    out_path = SAVE_DIR / fname

    # Si ya existe la imagen de hoy, reutilizarla para evitar golpear la API.
    if out_path.exists() and out_path.stat().st_size > 0:
        print(f"♻️ Imagen ya descargada, reusando: {out_path}", flush=True)
        return out_path

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

    ir = requests.get(img_url, headers={"User-Agent": "Mozilla/5.0"}, timeout=60)
    ir.raise_for_status()
    out_path.write_bytes(ir.content)

    print(f"✅ Imagen descargada: {out_path}", flush=True)
    return out_path


# =========================================================
# === FUNCIONES DE PARSEO CON OPENAI ======================
# =========================================================

def get_base_dir() -> Path:
    """
    Devuelve el directorio base donde buscar recursos (prompt.txt).
    - En modo normal: el directorio del .py
    - En modo PyInstaller (frozen): el directorio del ejecutable
    """
    if getattr(sys, "frozen", False):
        # Ejecutable de PyInstaller
        return Path(sys.executable).resolve().parent
    # Ejecución normal
    return Path(__file__).resolve().parent

def parse_bcra_image_with_openai(img_path: Path) -> dict:
    """Envía la imagen a OpenAI y devuelve el JSON estructurado."""
    client = OpenAI()  # usa OPENAI_API_KEY del entorno

    # detectar tipo MIME
    ext = img_path.suffix.lower()
    mime = "image/jpeg" if ext in [".jpg", ".jpeg"] else "image/png"

    # convertir a base64
    b64 = base64.b64encode(img_path.read_bytes()).decode("utf-8")
    data_url = f"data:{mime};base64,{b64}"

    # --- cargar prompt desde archivo prompt.txt al lado del ejecutable / script ---
    base_dir = get_base_dir()
    prompt_file = base_dir / "prompt.txt"

    if not prompt_file.exists():
        raise FileNotFoundError(f"No se encontró prompt.txt en {prompt_file}")

    prompt = prompt_file.read_text(encoding="utf-8").strip()

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
# === UTILIDADES DE PARSEO OCR ============================
# =========================================================

_DATE_PATTERNS = [
    (re.compile(r"\b(\d{1,2})[\/\-.](\d{1,2})[\/\-.](\d{4})\b"), "dmy"),
    (re.compile(r"\b(\d{4})[\/\-.](\d{1,2})[\/\-.](\d{1,2})\b"), "ymd"),
]


def _normalize_number_es(raw: str) -> float:
    """Convierte números estilo ES/EN (1.716,5 / 1,716.5 / 1716) a float."""
    cleaned = re.sub(r"[^\d,.\-]", "", raw.strip())
    if "," in cleaned and "." in cleaned:
        cleaned = cleaned.replace(".", "").replace(",", ".")
    elif "," in cleaned and "." not in cleaned:
        cleaned = cleaned.replace(",", ".")
    try:
        return float(cleaned)
    except Exception:
        raise ValueError(f"No pude parsear número: '{raw}' -> '{cleaned}'")


def _extract_fecha(texto: str) -> str:
    """Devuelve fecha yyyy-mm-dd encontrada en el texto; fallback: hoy BA."""
    for rx, fmt in _DATE_PATTERNS:
        match = rx.search(texto)
        if not match:
            continue
        if fmt == "dmy":
            d, mth, y = int(match.group(1)), int(match.group(2)), int(match.group(3))
        else:
            y, mth, d = int(match.group(1)), int(match.group(2)), int(match.group(3))
        try:
            return datetime(y, mth, d).strftime("%Y-%m-%d")
        except ValueError:
            continue
    return ba_today().strftime("%Y-%m-%d")


def _clean_text(texto: str) -> str:
    texto = texto.replace("\u2212", "-")
    return re.sub(r"[ \t]+", " ", texto)


def parse_bcra_text_to_json(texto_ocr: str) -> dict:
    """
    Parsea el texto OCR y devuelve {fecha, reservas_millones_usd, compra_venta_divisas_millones_usd}.
    """
    texto = _clean_text(texto_ocr)
    low = texto.lower()

    fecha = _extract_fecha(texto)

    reservas = None
    m_res = re.search(r"(reserva\w*.*?)([-+]?\s*[\d\.,]+)", texto, flags=re.IGNORECASE)
    if m_res:
        try:
            reservas = _normalize_number_es(m_res.group(2))
        except Exception:
            reservas = None
    if reservas is None:
        around = re.search(r"rese\w+.{0,40}?([-+]?\s*[\d\.,]+)", texto, flags=re.IGNORECASE)
        if around:
            try:
                reservas = _normalize_number_es(around.group(1))
            except Exception:
                reservas = None

    compra_venta = 0.0
    if "sin intervención" in low or "sin intervencion" in low:
        compra_venta = 0.0
    else:
        m_cv = re.search(r"(compra|venta|intervenci[oó]n|mulc)\D{0,40}([-+]?\s*[\d\.,]+)", texto, flags=re.IGNORECASE)
        if m_cv:
            try:
                compra_venta = _normalize_number_es(m_cv.group(2))
            except Exception:
                compra_venta = 0.0
        else:
            m_any = re.search(r"([-+]\s*[\d\.,]+)\s*(m|millones|usd|u\$s|us\$|d[oó]lares)", texto, flags=re.IGNORECASE)
            if m_any:
                try:
                    compra_venta = _normalize_number_es(m_any.group(1))
                except Exception:
                    compra_venta = 0.0

    if reservas is None:
        raise RuntimeError("No pude extraer 'reservas_millones_usd' del OCR.")

    return {
        "fecha": fecha,
        "reservas_millones_usd": float(reservas),
        "compra_venta_divisas_millones_usd": float(compra_venta),
    }


# =========================================================
# === OCR LOCAL CON HUGGING FACE ==========================
# =========================================================

def _resolve_hf_device(torch_mod):
    preference = os.environ.get("HF_OCR_DEVICE", "cpu").lower()
    if preference == "cuda" and torch_mod.cuda.is_available():
        return torch_mod.device("cuda")
    if preference == "mps" and getattr(torch_mod.backends, "mps", None):
        if torch_mod.backends.mps.is_available():
            return torch_mod.device("mps")
    return torch_mod.device("cpu")


def _ensure_hf_ocr():
    global _HF_OCR_CACHE
    if _HF_OCR_CACHE is not None:
        return _HF_OCR_CACHE
    try:
        from transformers import TrOCRProcessor, VisionEncoderDecoderModel
        import torch
    except ImportError as exc:
        raise RuntimeError(
            "Falta instalar 'transformers' y 'torch' para el backend local. Ejecutá pip install -r requirements.txt."
        ) from exc

    model_name = os.environ.get("HF_OCR_MODEL", "microsoft/trocr-base-printed")
    print(f"[OCR] Cargando modelo local {model_name}...", flush=True)
    processor = TrOCRProcessor.from_pretrained(model_name)
    model = VisionEncoderDecoderModel.from_pretrained(model_name)
    device = _resolve_hf_device(torch)
    model.to(device)
    model.eval()
    _HF_OCR_CACHE = {
        "processor": processor,
        "model": model,
        "device": device,
        "torch": torch,
    }
    return _HF_OCR_CACHE


def parse_bcra_image_with_huggingface(img_path: Path) -> dict:
    """OCR local usando TrOCR + parseo por regex."""
    cache = _ensure_hf_ocr()
    processor = cache["processor"]
    model = cache["model"]
    device = cache["device"]
    torch_mod = cache["torch"]

    image = Image.open(img_path).convert("RGB")
    pixel_values = processor(images=image, return_tensors="pt").pixel_values.to(device)

    with torch_mod.no_grad():
        generated_ids = model.generate(pixel_values, max_length=256)

    texto = processor.batch_decode(generated_ids, skip_special_tokens=True)[0]
    if not texto.strip():
        raise RuntimeError("OCR local no devolvió texto.")
    return parse_bcra_text_to_json(texto)


# =========================================================
# === SELECCIÓN DE BACKEND OCR ============================
# =========================================================

def _normalize_backend_name(name: str) -> str | None:
    if not name:
        return None
    lowered = name.lower().strip()
    if lowered in {"hf", "huggingface", "local"}:
        return "huggingface"
    if lowered in {"openai", "gpt"}:
        return "openai"
    return None


def _preferred_backend_order() -> list:
    env_value = os.environ.get("BCRA_OCR_BACKEND", "huggingface")
    requested = [v.strip() for v in env_value.split(",") if v.strip()]

    order = []
    for candidate in requested + DEFAULT_OCR_BACKENDS:
        normalized = _normalize_backend_name(candidate)
        if normalized and normalized not in order:
            order.append(normalized)
    return order or DEFAULT_OCR_BACKENDS


def parse_bcra_image(img_path: Path) -> dict:
    """Intenta parsear la imagen usando backends en orden preferido."""
    errors = []
    for backend in _preferred_backend_order():
        try:
            print(f"[OCR] Intentando backend '{backend}'...", flush=True)
            if backend == "huggingface":
                return parse_bcra_image_with_huggingface(img_path)
            if backend == "openai":
                return parse_bcra_image_with_openai(img_path)
        except Exception as exc:
            errors.append(f"{backend}: {exc}")
            print(f"[OCR] ⚠ Backend '{backend}' falló: {exc}", file=sys.stderr, flush=True)
            continue

    raise RuntimeError("Ningún backend OCR funcionó. Errores: " + " | ".join(errors))



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

    print(f"[DB] Conectando a Postgres host={db_host} port={db_port} db={db_name} user={db_user}", flush=True)
    DATABASE_URL = f"postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"

    # Sumar pre_ping y timeout
    engine = create_engine(DATABASE_URL, pool_pre_ping=True, connect_args={"connect_timeout": 10})

    # Verificación temprana de conexión para loguear errores en el acto
    try:
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        print("[DB] ✔ Conexión a Postgres verificada (SELECT 1)", flush=True)
    except Exception as e:
        print(f"[DB] ❌ Error conectando a Postgres: {e}", file=sys.stderr)
        traceback.print_exc()
        raise

    return engine

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

    print(f"[DB] Insert reservas_scrape fecha={fecha} valor={valor}", flush=True)

    insert_sql = text("""
        INSERT INTO public.reservas_scrape (date, valor)
        VALUES (:fecha, :valor)
    """)

    with engine.begin() as conn:
        try:
            res = conn.execute(insert_sql, {"fecha": fecha, "valor": valor})
            rc = res.rowcount if (res.rowcount is not None and res.rowcount >= 0) else 1
            print(f"[DB] reservas_scrape rowcount={rc}", flush=True)
            return rc
        except Exception as e:
            print(f"[DB] ❌ Error insertando en reservas_scrape: fecha={fecha} valor={valor} -> {e}", file=sys.stderr)
            traceback.print_exc()
            raise

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

    print(f"[DB] Upsert comprasMULCBCRA fecha={fecha} valor={valor}", flush=True)

    insert_sql = text("""
        INSERT INTO "public"."comprasMULCBCRA" (date, "comprasBCRA")
        VALUES (:fecha, :valor)
        ON CONFLICT (date) DO UPDATE SET "comprasBCRA" = EXCLUDED."comprasBCRA";
    """)

    with engine.begin() as conn:
        try:
            res = conn.execute(insert_sql, {"fecha": fecha, "valor": valor})
            rc = res.rowcount if (res.rowcount is not None and res.rowcount >= 0) else 1
            print(f"[DB] comprasMULCBCRA rowcount={rc}", flush=True)
            return rc
        except Exception as e:
            print(f"[DB] ❌ Error upsert en comprasMULCBCRA: fecha={fecha} valor={valor} -> {e}", file=sys.stderr)
            traceback.print_exc()
            raise


# =========================================================
# === MAIN ===============================================
# =========================================================

def main():
    print("=== Descargando imagen del BCRA desde X ===", flush=True)
    print(f"[RUN] CWD: {Path.cwd()}", flush=True)

    img_path = download_bcra_image()

    print("=== Parseando imagen (backends locales → OpenAI fallback) ===", flush=True)
    parsed = parse_bcra_image(img_path)
    print("JSON parseado:", flush=True)
    print(json.dumps(parsed, indent=2, ensure_ascii=False), flush=True)

    print("=== Guardando en Postgres ===", flush=True)
    try:
        engine = build_engine()
        print("[DB] Engine construido OK", flush=True)
    except Exception:
        return

    try:
        print("[DB] -> Guardando comprasMULCBCRA...", flush=True)
        n1 = save_compra_venta_to_db(engine, parsed)
        print(f"✅ Inserted {n1} rows into \"comprasMULCBCRA\"", flush=True)
    except Exception:
        return

    try:
        print("[DB] -> Guardando reservas_scrape...", flush=True)
        n2 = save_reservas_to_db(engine, parsed)
        print(f"✅ Inserted {n2} rows into reservas_scrape", flush=True)
    except Exception:
        return

    print("=== Proceso finalizado ===", flush=True)

if __name__ == "__main__":
    main()