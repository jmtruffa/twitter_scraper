# bcra_daily_parser.py
import os, json, base64, requests, re, argparse
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
# === UTILIDADES DE PARSEO (comunes a cualquier OCR) ======
# =========================================================

_DATE_PATTERNS = [
    # 11/10/2025, 11-10-2025, 11.10.2025
    (re.compile(r"\b(\d{1,2})[\/\-.](\d{1,2})[\/\-.](\d{4})\b"), "dmy"),
    # 2025-10-11
    (re.compile(r"\b(\d{4})[\/\-.](\d{1,2})[\/\-.](\d{1,2})\b"), "ymd"),
]

def _normalize_number_es(s: str) -> float:
    """
    Convierte '1.716,5' -> 1716.5 ; '1,716.5' -> 1716.5 ; '1.716' -> 1716.0
    Elimina símbolos y maneja comas/puntos como separadores locales.
    """
    s0 = s.strip()
    s0 = re.sub(r"[^\d,.\-]", "", s0)
    # Si tiene ambos, asumimos: punto = miles, coma = decimal (estilo ES)
    if "," in s0 and "." in s0:
        s0 = s0.replace(".", "").replace(",", ".")
    else:
        # si solo hay comas, tratarlas como decimales
        if "," in s0 and "." not in s0:
            s0 = s0.replace(",", ".")
        # si solo hay puntos, ya es decimal estilo EN o miles ES
        # (no tocamos)
    try:
        return float(s0)
    except Exception:
        raise ValueError(f"No pude parsear número: '{s}' -> '{s0}'")

def _extract_fecha(texto: str) -> str | None:
    # Intentamos encontrar la primer fecha plausible
    for rx, kind in _DATE_PATTERNS:
        m = rx.search(texto)
        if m:
            if kind == "dmy":
                d, mth, y = int(m.group(1)), int(m.group(2)), int(m.group(3))
            else:
                y, mth, d = int(m.group(1)), int(m.group(2)), int(m.group(3))
            try:
                return datetime(y, mth, d).strftime("%Y-%m-%d")
            except ValueError:
                continue
    # fallback: hoy BA
    return ba_today().strftime("%Y-%m-%d")

def _clean_text(t: str) -> str:
    # homogeneizar para regex (acentos mínimos)
    t = t.replace("\u2212", "-")  # minus sign
    t = re.sub(r"[ \t]+", " ", t)
    return t

def parse_bcra_text_to_json(texto_ocr: str) -> dict:
    """
    Parsea el texto OCR (de OpenAI o Docling) y devuelve el JSON requerido:
      {fecha, reservas_millones_usd, compra_venta_divisas_millones_usd}
    Reglas:
      - "Sin intervención" => 0.0 en compra/venta
      - Signo: respeta + / - si aparece
      - Unidades: asume que el gráfico informa en "millones de USD"
    """
    txt = _clean_text(texto_ocr)
    low = txt.lower()

    # --- fecha ---
    fecha = _extract_fecha(txt)

    # --- reservas ---
    # buscar líneas con "Reserva" o "Reservas internacionales"
    reservas = None
    # Ejemplos de OCR: "Reservas internacionales 1.716", "Reservas: 19.757", etc.
    m_res = re.search(r"(reserva\w*.*?)([-+]?\s*[\d\.,]+)", low, flags=re.IGNORECASE)
    if m_res:
        num_str = m_res.group(2)
        try:
            reservas = _normalize_number_es(num_str)
        except Exception:
            reservas = None

    # fallback: intentar capturar el mayor número cerca de "rese"
    if reservas is None:
        around_res = re.search(r"rese\w+.{0,40}?([-+]?\s*[\d\.,]+)", low, flags=re.IGNORECASE)
        if around_res:
            try:
                reservas = _normalize_number_es(around_res.group(1))
            except Exception:
                reservas = None

    # --- compra/venta MULC ---
    compra_venta = 0.0
    if "sin intervención" in low or "sin intervencion" in low:
        compra_venta = 0.0
    else:
        # buscar números con signo cerca de "compra", "venta", "intervención", "mulc"
        m_cv = re.search(r"(compra|venta|intervenci[oó]n|mulc)\D{0,40}([-+]?\s*[\d\.,]+)", low, flags=re.IGNORECASE)
        if m_cv:
            try:
                compra_venta = _normalize_number_es(m_cv.group(2))
            except Exception:
                compra_venta = 0.0
        else:
            # fallback: algún número aislado con signo +/- en contexto de 'usd' o 'millones'
            m_any = re.search(r"([-+]\s*[\d\.,]+)\s*(m|millones|usd|u\$s|us\$|d[oó]lares)", low, flags=re.IGNORECASE)
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
# === FUNCIONES DE PARSEO CON OPENAI (NO TOCAR) ===========
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

    prompt = (
        "Procesá la imagen y devolvé SOLO un JSON con los campos: "
        "'fecha' (yyyy-mm-dd), 'reservas_millones_usd' (float), "
        "'compra_venta_divisas_millones_usd' (float). "
        "Si Compra/Venta dice 'Sin intervención', devolvé 0.0. "
        "No incluyas texto fuera del JSON."
    )

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
# === NUEVO: PARSEO CON DOCLING (OCR LOCAL) ===============
# =========================================================

def parse_bcra_image_with_docling(img_path: Path,
                                  device: str = "cpu",
                                  ocr_langs: str = "es,en",
                                  use_tesseract: bool = True) -> dict:
    """
    OCR local con Docling para extraer texto y luego aplicar parseo por regex.
    - device: 'cpu' | 'mps' | 'cuda' | 'auto' (cpu recomendado en macOS)
    - ocr_langs: idiomas separados por coma (ej: 'es,en')
    - use_tesseract: True para forzar Tesseract; False para motor 'auto' de Docling
    """
    # Variables de entorno ANTES de importar docling/torch
    if device == "cpu":
        os.environ.setdefault("PYTORCH_ENABLE_MPS_FALLBACK", "1")
        os.environ.setdefault("PYTORCH_NO_MPS", "1")
    os.environ.setdefault("TOKENIZERS_PARALLELISM", "false")
    os.environ.setdefault("OMP_NUM_THREADS", "4")
    os.environ.setdefault("DOCLING_NUM_THREADS", "4")

    from docling.document_converter import DocumentConverter, ImageFormatOption
    from docling.datamodel.base_models import InputFormat
    from docling.datamodel.pipeline_options import (
        ImagePipelineOptions,
        TesseractOcrOptions,
        OcrOptions,  # genérico
        AcceleratorOptions,
        AcceleratorDevice,
    )

    pipeline = ImagePipelineOptions()
    pipeline.do_ocr = True

    langs = [x.strip() for x in ocr_langs.split(",") if x.strip()]
    if use_tesseract:
        pipeline.ocr_options = TesseractOcrOptions(lang="+".join(langs) if langs else "eng")
    else:
        # Motor auto; seteamos idiomas si está soportado por el motor
        pipeline.ocr_options = OcrOptions(lang=langs)

    device_map = {
        "cpu": AcceleratorDevice.CPU,
        "mps": AcceleratorDevice.MPS,
        "cuda": AcceleratorDevice.CUDA,
        "auto": AcceleratorDevice.AUTO,
    }
    pipeline.accelerator_options = AcceleratorOptions(
        num_threads=4,
        device=device_map.get(device, AcceleratorDevice.CPU),
    )

    converter = DocumentConverter(
        format_options={InputFormat.IMAGE: ImageFormatOption(pipeline_options=pipeline)}
    )

    res = converter.convert(str(img_path))
    doc = res.document

    # Intentar texto plano; si falla, markdown estricto
    try:
        texto = doc.export_to_text()
    except Exception:
        texto = doc.export_to_markdown(strict_text=True)

    # Parseo por regex al JSON final
    return parse_bcra_text_to_json(texto)


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
    Inserta en la tabla tmp_reservas (date, valor)
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
        INSERT INTO public.tmp_reservas (date, valor)
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
        _ = datetime.strptime(fecha, "%Y-%m-%d").date()
    except Exception:
        raise ValueError(f"Fecha inválida: {fecha}")

    try:
        valor = float(valor)
    except Exception:
        raise ValueError(f"Valor de compra_venta_divisas_millones_usd inválido: {valor}")

    insert_sql = text("""
        INSERT INTO "public"."comprasMULCBCRA" (date, "comprasBCRA")
        VALUES (:fecha, :valor)
        ON CONFLICT (date) DO UPDATE SET "comprasBCRA" = EXCLUDED."comprasBCRA";
    """)

    with engine.begin() as conn:
        res = conn.execute(insert_sql, {"fecha": fecha, "valor": valor})
        return res.rowcount if (res.rowcount is not None and res.rowcount >= 0) else 1


# =========================================================
# === MAIN ===============================================
# =========================================================

def main():
    parser = argparse.ArgumentParser(description="Descarga imagen #DataBCRA, la parsea y guarda en DB.")
    parser.add_argument("--ocr-backend", choices=["docling", "openai"], default="docling",
                        help="Backend de OCR: 'docling' (local) u 'openai' (API). Default: docling.")
    parser.add_argument("--docling-device", choices=["cpu", "mps", "cuda", "auto"], default="cpu",
                        help="Dispositivo Docling. En macOS se recomienda 'cpu'.")
    parser.add_argument("--docling-langs", type=str, default="es,en",
                        help="Idiomas OCR para Docling (coma sep.). Default: 'es,en'.")
    parser.add_argument("--docling-tesseract", action="store_true",
                        help="Forzar motor Tesseract en Docling (recomendado para español).")
    args = parser.parse_args()

    print("=== Descargando imagen del BCRA desde X ===")
    img_path = download_bcra_image()

    if args.ocr-backend == "openai":  # (guion en nombre CLI no es válido como atributo)
        backend = "openai"
    else:
        backend = "docling"

    print(f"=== Parseando imagen con {backend.upper()} ===")
    if backend == "docling":
        parsed = parse_bcra_image_with_docling(
            img_path,
            device=args.docling_device,
            ocr_langs=args.docling_langs,
            use_tesseract=args.docling_tesseract,
        )
    else:
        parsed = parse_bcra_image_with_openai(img_path)

    print("JSON parseado:")
    print(json.dumps(parsed, indent=2, ensure_ascii=False))

    print("=== Guardando en Postgres ===")
    engine = build_engine()

    n1 = save_compra_venta_to_db(engine, parsed)
    print(f"✅ Inserted {n1} rows into \"comprasMULCBCRA\"")

    n2 = save_reservas_to_db(engine, parsed)
    print(f"✅ Inserted {n2} rows into tmp_reservas")
    print("=== Proceso finalizado ===")

if __name__ == "__main__":
    main()
