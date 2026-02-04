import os, json, requests, re, argparse
import sys, traceback
from pathlib import Path
from datetime import datetime
from zoneinfo import ZoneInfo
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

# --- Config global ---
BA_TZ = ZoneInfo("America/Argentina/Buenos_Aires")
USERNAME = "BancoCentral_AR"
SAVE_DIR = Path("./bcra_imagenes")
REQUIRED_HASHTAGS = ("#databcra", "#reservasbcra")

def _read_cookies_file(path: Path) -> dict:
    if not path.exists():
        raise RuntimeError(f"No se encontró el archivo de cookies en {path}")
    try:
        data = json.loads(path.read_text())
    except Exception as exc:
        raise RuntimeError(f"No pude parsear {path} como JSON de cookies.") from exc
    return data


def ba_today():
    return datetime.now(BA_TZ).date()


def _parse_target_date(value: str | None):
    if not value:
        return ba_today()
    try:
        return datetime.strptime(value, "%Y-%m-%d").date()
    except ValueError as exc:
        raise RuntimeError("La fecha objetivo debe tener formato YYYY-MM-DD.") from exc


def _cookie_domains() -> list[str]:
    custom = os.environ.get("X_COOKIE_DOMAIN")
    domains: list[str] = []
    if custom:
        domains.append(custom)
    for default_domain in [".x.com", ".twitter.com"]:
        if default_domain not in domains:
            domains.append(default_domain)
    return domains


_COOKIE_HEADER_KEYS = {"user-agent", "authorization", "Authorization"}


def _cookies_to_playwright_list(cookies: dict) -> list[dict]:
    cookie_values = {k: v for k, v in cookies.items() if k not in _COOKIE_HEADER_KEYS}
    result = []
    for domain in _cookie_domains():
        for name, value in cookie_values.items():
            if value is None:
                continue
            result.append(
                {
                    "name": str(name),
                    "value": str(value),
                    "domain": domain,
                    "path": "/",
                    "secure": True,
                }
            )
    return result


def _resolve_cookies_file() -> Path:
    env_path = os.environ.get("X_COOKIES_FILE")
    if env_path:
        return Path(env_path).expanduser()
    return Path("cookies.json")


def _get_login_credentials() -> tuple[str, str] | None:
    """Returns (username, password) from env vars, or None if not set."""
    username = os.environ.get("X_USERNAME")
    password = os.environ.get("X_PASSWORD")
    if username and password:
        return (username, password)
    return None


def _perform_twitter_login(context, cookies_path: Path) -> bool:
    """
    Logs into Twitter/X using credentials from environment variables.
    Saves cookies to cookies_path after successful login.
    Returns True if login succeeded.
    """
    creds = _get_login_credentials()
    if not creds:
        print("[LOGIN] No hay credenciales en X_USERNAME/X_PASSWORD", flush=True)
        return False

    username, password = creds
    print(f"[LOGIN] Iniciando sesión en X como {username}...", flush=True)

    page = context.new_page()
    try:
        page.set_default_timeout(60000)

        # Go to login page - try main site first then login flow
        page.goto("https://x.com/login", wait_until="domcontentloaded", timeout=60000)
        try:
            page.wait_for_load_state("networkidle", timeout=30000)
        except Exception:
            pass
        page.wait_for_timeout(3000)

        # Try multiple selectors for username input - name="text" is the current one
        username_selectors = [
            'input[name="text"]',
            'input[autocomplete="username"]',
            'input[name="session[username_or_email]"]',
        ]
        username_input = None
        for selector in username_selectors:
            try:
                loc = page.locator(selector)
                if loc.count() > 0:
                    loc.first.wait_for(state="visible", timeout=10000)
                    username_input = loc.first
                    print(f"[LOGIN] Encontrado input de usuario con: {selector}", flush=True)
                    break
            except Exception:
                continue

        if not username_input:
            print(f"[LOGIN] No se encontró el campo de usuario. URL actual: {page.url}", flush=True)
            # Debug: show what inputs exist
            try:
                all_inputs = page.locator('input')
                print(f"[LOGIN] Inputs en la página: {all_inputs.count()}", flush=True)
            except Exception:
                pass
            return False

        username_input.fill(username)
        page.wait_for_timeout(1000)

        # Click Next button
        next_selectors = [
            'button:has-text("Next")',
            'button:has-text("Siguiente")',
            'div[role="button"]:has-text("Next")',
            'div[role="button"]:has-text("Siguiente")',
        ]
        for selector in next_selectors:
            try:
                btn = page.locator(selector).first
                if btn.count() > 0 and btn.is_visible():
                    btn.click()
                    print(f"[LOGIN] Click en Next con: {selector}", flush=True)
                    break
            except Exception:
                continue

        page.wait_for_timeout(3000)

        # Check if Twitter asks for email/phone verification (unusual activity)
        unusual_check = page.locator('input[data-testid="ocfEnterTextTextInput"]')
        try:
            if unusual_check.count() > 0 and unusual_check.is_visible():
                print("[LOGIN] X pide verificación adicional (email/usuario), ingresando...", flush=True)
                unusual_check.fill(username)
                page.wait_for_timeout(500)
                verify_next = page.locator('button[data-testid="ocfEnterTextNextButton"]')
                if verify_next.count() > 0:
                    verify_next.click()
                    page.wait_for_timeout(3000)
        except Exception:
            pass

        # Enter password - try multiple selectors
        password_selectors = [
            'input[name="password"]',
            'input[type="password"]',
            'input[autocomplete="current-password"]',
        ]
        password_input = None
        for selector in password_selectors:
            try:
                loc = page.locator(selector).first
                if loc.count() > 0:
                    loc.wait_for(state="visible", timeout=15000)
                    password_input = loc
                    print(f"[LOGIN] Encontrado input de password con: {selector}", flush=True)
                    break
            except Exception:
                continue

        if not password_input:
            print(f"[LOGIN] No se encontró el campo de password. URL actual: {page.url}", flush=True)
            return False

        password_input.fill(password)
        page.wait_for_timeout(1000)

        # Click Log in button
        login_selectors = [
            'button[data-testid="LoginForm_Login_Button"]',
            'button:has-text("Log in")',
            'button:has-text("Iniciar sesión")',
            'div[role="button"]:has-text("Log in")',
        ]
        for selector in login_selectors:
            try:
                btn = page.locator(selector).first
                if btn.count() > 0 and btn.is_visible():
                    btn.click()
                    print(f"[LOGIN] Click en Login con: {selector}", flush=True)
                    break
            except Exception:
                continue

        page.wait_for_timeout(5000)

        # Check if login succeeded (should be on home or not on login page)
        current_url = page.url
        if "/login" in current_url or "/flow/login" in current_url:
            # Check for error messages
            error = page.locator('[data-testid="error"], [role="alert"]')
            if error.count() > 0:
                try:
                    error_text = error.first.inner_text()
                    print(f"[LOGIN] Error de login: {error_text}", flush=True)
                except Exception:
                    print("[LOGIN] Error de login (no se pudo leer mensaje)", flush=True)
            else:
                print(f"[LOGIN] Login falló - todavía en página de login: {current_url}", flush=True)
            return False

        print("[LOGIN] Login exitoso, guardando cookies...", flush=True)

        # Extract and save cookies
        cookies = context.cookies()
        cookies_dict = {}
        for cookie in cookies:
            cookies_dict[cookie["name"]] = cookie["value"]

        # Add a default User-Agent
        cookies_dict["user-agent"] = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120 Safari/537.36"

        # Save cookies
        cookies_path.write_text(json.dumps(cookies_dict, indent=2))
        print(f"[LOGIN] Cookies guardadas en {cookies_path}", flush=True)

        return True

    except Exception as e:
        print(f"[LOGIN] Error durante login: {e}", flush=True)
        print(f"[LOGIN] URL actual: {page.url}", flush=True)
        return False
    finally:
        page.close()


def _try_scrape_methods(context, target_date) -> str:
    """Busca imagen en el perfil del BCRA scrolleando el timeline."""
    profile_url = f"https://x.com/{USERNAME}"
    target_iso = target_date.isoformat()

    page = context.new_page()
    try:
        page.set_default_timeout(60000)
        print(f"[SCRAPER] Navegando a {profile_url}", flush=True)
        page.goto(profile_url, wait_until="domcontentloaded", timeout=60000)
        page.wait_for_timeout(3000)

        if "/i/flow/login" in page.url or "/login" in page.url:
            raise RuntimeError("X redirigió al login; cookies inválidas o expiradas.")

        # Scrollear el timeline buscando tweet con imagen de la fecha objetivo
        for scroll in range(15):  # Máximo 15 scrolls
            # Usar JavaScript para encontrar tweets con fechas e imágenes
            result = page.evaluate('''(targetDate) => {
                const tweets = document.querySelectorAll('article');
                for (const tweet of tweets) {
                    // Buscar elemento time con datetime
                    const timeEl = tweet.querySelector('time[datetime]');
                    if (!timeEl) continue;

                    const datetime = timeEl.getAttribute('datetime');
                    if (!datetime) continue;

                    // Extraer fecha (YYYY-MM-DD) del datetime
                    const tweetDate = datetime.split('T')[0];
                    if (tweetDate !== targetDate) continue;

                    // Buscar imagen de media
                    const img = tweet.querySelector('img[src*="twimg.com/media"], img[src*="pbs.twimg.com/media"]');
                    if (!img) continue;

                    const src = img.getAttribute('src');
                    if (src) {
                        return { found: true, src: src, date: tweetDate };
                    }
                }
                return { found: false, count: tweets.length };
            }''', target_iso)

            if result.get('found'):
                src = result['src']
                if "name=" in src:
                    src = re.sub(r"name=[a-z]+", "name=large", src)
                print(f"[SCRAPER] Imagen encontrada para {result['date']}", flush=True)
                return src

            print(f"[SCRAPER] Scroll {scroll + 1}/15, tweets visibles: {result.get('count', 0)}", flush=True)
            page.mouse.wheel(0, 1500)
            page.wait_for_timeout(2000)

        # Debug: guardar screenshot si no encuentra
        try:
            page.screenshot(path="/tmp/profile_debug.png")
            print("[SCRAPER] Screenshot guardado en /tmp/profile_debug.png", flush=True)
        except Exception:
            pass

        raise RuntimeError(f"No encontré imagen para {target_iso} después de scrollear el timeline")

    finally:
        page.close()


def _fetch_image_url_with_playwright(cookies_path: Path, target_date) -> str:
    try:
        from playwright.sync_api import sync_playwright
    except ImportError as exc:
        raise RuntimeError(
            "Falta instalar Playwright. Ejecutá: pip install playwright && playwright install"
        ) from exc

    # Try to load existing cookies, but don't fail if they don't exist
    cookies = {}
    pw_cookies = []
    user_agent = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120 Safari/537.36"

    if cookies_path.exists():
        try:
            cookies = _read_cookies_file(cookies_path)
            # Para Playwright solo necesitamos las cookies, no el header Authorization
            # Validamos que existan las cookies esenciales
            if not cookies.get("auth_token"):
                raise RuntimeError("Las cookies no incluyen 'auth_token'.")
            if not cookies.get("ct0"):
                raise RuntimeError("Las cookies no incluyen 'ct0'.")
            pw_cookies = _cookies_to_playwright_list(cookies)
            user_agent = cookies.get("user-agent", user_agent)
        except Exception as e:
            print(f"[SCRAPER] Cookies inválidas, se intentará login: {e}", flush=True)
            cookies = {}
            pw_cookies = []

    with sync_playwright() as p:
        # Anti-detection: disable automation flags
        browser = p.chromium.launch(
            headless=True,
            args=[
                '--disable-blink-features=AutomationControlled',
                '--no-sandbox',
            ]
        )
        context = browser.new_context(
            user_agent=user_agent,
            locale="es-ES",
            timezone_id="America/Argentina/Buenos_Aires",
            viewport={'width': 1280, 'height': 720},
        )
        # Anti-detection: hide webdriver property
        context.add_init_script('Object.defineProperty(navigator, "webdriver", {get: () => undefined});')
        try:
            if pw_cookies:
                context.add_cookies(pw_cookies)

            page_headers = {
                "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
                "accept-language": "es-ES,es;q=0.9,en;q=0.8",
                "upgrade-insecure-requests": "1",
            }
            context.set_extra_http_headers(page_headers)

            # If no cookies, login first (X.com shows old/random tweets without auth)
            if not pw_cookies:
                print("[SCRAPER] No hay cookies, intentando login primero...", flush=True)
                if _perform_twitter_login(context, cookies_path):
                    print("[SCRAPER] Login exitoso, procediendo con scraping...", flush=True)
                else:
                    print("[SCRAPER] Login falló, intentando scraping sin auth...", flush=True)

            # Try scraping
            try:
                return _try_scrape_methods(context, target_date)
            except RuntimeError as e:
                error_msg = str(e).lower()
                # If we already have cookies but got redirected to login, try re-login
                needs_reauth = (
                    "redirigió al login" in error_msg or
                    "cookies inválidas" in error_msg
                )
                if needs_reauth and pw_cookies:
                    print(f"[SCRAPER] Sesión expirada, intentando re-login...", flush=True)
                    if _perform_twitter_login(context, cookies_path):
                        print("[SCRAPER] Re-login exitoso, reintentando scraping...", flush=True)
                        return _try_scrape_methods(context, target_date)
                raise
        finally:
            context.close()
            browser.close()


def download_bcra_image(target_date) -> Path:
    """Descarga la imagen del tweet #DataBCRA para la fecha indicada."""
    SAVE_DIR.mkdir(parents=True, exist_ok=True)
    fname = f"bcra_{target_date.isoformat()}.jpg"
    out_path = SAVE_DIR / fname

    # Si ya existe la imagen, reutilizarla
    if out_path.exists() and out_path.stat().st_size > 0:
        print(f"♻️ Imagen ya descargada, reusando: {out_path}", flush=True)
        return out_path

    cookies_path = _resolve_cookies_file()

    print("[SCRAPER] Descargando imagen vía Playwright...", flush=True)
    img_url = _fetch_image_url_with_playwright(cookies_path, target_date)

    ir = requests.get(img_url, headers={"User-Agent": "Mozilla/5.0"}, timeout=60)
    ir.raise_for_status()
    out_path.write_bytes(ir.content)

    print(f"✅ Imagen descargada: {out_path}", flush=True)
    return out_path


# =========================================================
# === UTILIDADES DE PARSEO OCR ============================
# =========================================================

_DATE_PATTERNS = [
    (re.compile(r"\b(\d{1,2})[\/\-.](\d{1,2})[\/\-.](\d{4})\b"), "dmy"),
    (re.compile(r"\b(\d{4})[\/\-.](\d{1,2})[\/\-.](\d{1,2})\b"), "ymd"),
]

_SPANISH_MONTHS = {
    'enero': 1, 'febrero': 2, 'marzo': 3, 'abril': 4, 'mayo': 5, 'junio': 6,
    'julio': 7, 'agosto': 8, 'septiembre': 9, 'octubre': 10, 'noviembre': 11, 'diciembre': 12
}


def _normalize_number_es(raw: str) -> float:
    """Convierte números estilo ES (44.607 / 1.453,446) a float.

    Reglas:
    - Si hay coma y punto: punto=miles, coma=decimal (1.453,446 -> 1453.446)
    - Si solo hay punto seguido de exactamente 3 dígitos: punto=miles (44.607 -> 44607)
    - Si solo hay coma: coma=decimal (39,69 -> 39.69)
    - Si solo hay punto NO seguido de 3 dígitos: punto=decimal (44.5 -> 44.5)
    """
    cleaned = re.sub(r"[^\d,.\-]", "", raw.strip())

    if "," in cleaned and "." in cleaned:
        # Ambos: punto=miles, coma=decimal
        cleaned = cleaned.replace(".", "").replace(",", ".")
    elif "." in cleaned and "," not in cleaned:
        # Solo punto: verificar si es separador de miles (seguido de exactamente 3 dígitos)
        if re.match(r"^-?\d{1,3}(\.\d{3})+$", cleaned):
            # Es separador de miles: 44.607 -> 44607
            cleaned = cleaned.replace(".", "")
        # Si no, es decimal y lo dejamos como está
    elif "," in cleaned and "." not in cleaned:
        # Solo coma: es decimal
        cleaned = cleaned.replace(",", ".")

    try:
        return float(cleaned)
    except Exception:
        raise ValueError(f"No pude parsear número: '{raw}' -> '{cleaned}'")


def _extract_fecha(texto: str) -> str:
    """Devuelve fecha yyyy-mm-dd encontrada en el texto; fallback: hoy BA."""
    # Patrón 1: día de semana + número, luego "de [mes] de [año]" (más confiable)
    # Ej: "lunes 19 ... de enero de 2026"
    m_weekday = re.search(r"(?:lunes|martes|miércoles|miercoles|jueves|viernes|sábado|sabado|domingo)\s+(\d{1,2})", texto, re.IGNORECASE)
    m_month_year = re.search(r"de\s+(enero|febrero|marzo|abril|mayo|junio|julio|agosto|septiembre|octubre|noviembre|diciembre)\s+de\s+(\d{4})", texto, re.IGNORECASE)
    if m_weekday and m_month_year:
        day_str = m_weekday.group(1)
        month_name = m_month_year.group(1)
        year_str = m_month_year.group(2)
        month_num = _SPANISH_MONTHS.get(month_name.lower())
        if month_num:
            try:
                return datetime(int(year_str), month_num, int(day_str)).strftime("%Y-%m-%d")
            except ValueError:
                pass

    # Patrón 2: formato español limpio "16 DE ENERO DE 2026" (requiere palabra boundary antes del día)
    m_es = re.search(r"(?:^|[^\d(])(\d{1,2})\s+de\s+(enero|febrero|marzo|abril|mayo|junio|julio|agosto|septiembre|octubre|noviembre|diciembre)\s+de\s+(\d{4})", texto, re.IGNORECASE)
    if m_es:
        day_str, month_name, year_str = m_es.groups()
        month_num = _SPANISH_MONTHS.get(month_name.lower())
        if month_num:
            try:
                return datetime(int(year_str), month_num, int(day_str)).strftime("%Y-%m-%d")
            except ValueError:
                pass

    # Patrón 3: formatos numéricos dd/mm/yyyy o yyyy-mm-dd
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
    Compatible con Tesseract y Google Cloud Vision.
    """
    texto = _clean_text(texto_ocr)
    low = texto.lower()

    fecha = _extract_fecha(texto)

    # Extraer todos los números del texto (excluyendo referencias)
    all_numbers = []
    for m in re.finditer(r"[-+]?[\d\.,]+", texto):
        try:
            val = _normalize_number_es(m.group())
            pos = m.start()
            # Excluir números entre paréntesis (son referencias como (1), (2))
            if pos > 0 and texto[pos - 1] == "(":
                continue
            if m.end() < len(texto) and texto[m.end()] == ")":
                continue
            # Excluir números después de "Comunicación" (son códigos de regulación)
            previo = texto[max(0, pos - 15):pos].lower()
            if "comunicación" in previo or "comunicacion" in previo:
                continue
            all_numbers.append((val, m.start(), m.group()))
        except Exception:
            continue

    # RESERVAS: número entre 40000 y 100000 (millones USD)
    # Las reservas del BCRA típicamente están en ese rango
    reservas = None
    reservas_pos = 0
    for val, pos, raw in all_numbers:
        if 40000 <= val <= 100000:
            reservas = val
            reservas_pos = pos
            break

    # COMPRA/VENTA: número entre -500 y 500 que aparece DESPUÉS de reservas
    # y que NO sea porcentaje ni referencia
    compra_venta = 0.0
    if "sin intervención" in low or "sin intervencion" in low:
        compra_venta = 0.0
    else:
        for val, pos, raw in all_numbers:
            # Debe aparecer después de reservas en el texto
            if pos <= reservas_pos:
                continue
            if val == reservas:
                continue
            if not (-500 <= val <= 500):
                continue
            # Verificar que no sea porcentaje
            siguiente = texto[pos + len(raw):pos + len(raw) + 5] if pos + len(raw) < len(texto) else ""
            if "%" in siguiente:
                continue
            # Verificar que no sea día del mes (seguido de "de enero", etc.)
            if 1 <= val <= 31:
                contexto = texto[pos:pos + 30].lower()
                if " de enero" in contexto or " de febrero" in contexto or " de marzo" in contexto:
                    continue
            compra_venta = val
            break

    if reservas is None:
        raise RuntimeError("No pude extraer 'reservas_millones_usd' del OCR.")

    return {
        "fecha": fecha,
        "reservas_millones_usd": float(reservas),
        "compra_venta_divisas_millones_usd": float(compra_venta),
    }


# =========================================================
# === OCR CON GOOGLE CLOUD VISION =========================
# =========================================================

def parse_bcra_image(img_path: Path) -> dict:
    """OCR usando Google Cloud Vision API."""
    try:
        from google.cloud import vision
    except ImportError as exc:
        raise RuntimeError(
            "Falta instalar 'google-cloud-vision'. Ejecutá: pip install google-cloud-vision"
        ) from exc

    print("[OCR] Ejecutando Google Cloud Vision...", flush=True)

    client = vision.ImageAnnotatorClient()

    # Leer imagen
    with open(img_path, "rb") as f:
        content = f.read()

    image = vision.Image(content=content)

    # Ejecutar OCR (document_text_detection es mejor para texto estructurado)
    response = client.document_text_detection(image=image)

    if response.error.message:
        raise RuntimeError(f"Google Vision API error: {response.error.message}")

    texto = response.full_text_annotation.text

    if not texto.strip():
        raise RuntimeError("Google Cloud Vision no devolvió texto.")

    print(f"[OCR] Texto extraído ({len(texto)} chars)", flush=True)
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

    print(f"[DB] Conectando a Postgres host={db_host} port={db_port} db={db_name} user={db_user}", flush=True)
    DATABASE_URL = f"postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"

    engine = create_engine(DATABASE_URL, pool_pre_ping=True, connect_args={"connect_timeout": 10})

    try:
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        print("[DB] ✔ Conexión a Postgres verificada", flush=True)
    except Exception as e:
        print(f"[DB] ❌ Error conectando a Postgres: {e}", file=sys.stderr)
        traceback.print_exc()
        raise

    return engine


def save_reservas_to_db(engine: Engine, parsed: dict) -> int:
    """Inserta en la tabla reservas_scrape (date, valor)."""
    fecha = parsed.get("fecha")
    valor = parsed.get("reservas_millones_usd", None)

    if not fecha:
        raise ValueError("El JSON no trae 'fecha'.")
    if valor is None:
        raise ValueError("El JSON no trae 'reservas_millones_usd'.")

    try:
        _ = datetime.strptime(fecha, "%Y-%m-%d").date()
    except Exception:
        raise ValueError(f"Fecha inválida: {fecha}")

    try:
        valor = float(valor)
    except Exception:
        raise ValueError(f"Valor de reservas_millones_usd inválido: {valor}")

    print(f"[DB] Upsert reservas_scrape fecha={fecha} valor={valor}", flush=True)

    insert_sql = text("""
        INSERT INTO public.reservas_scrape (date, valor)
        VALUES (:fecha, :valor)
        ON CONFLICT (date) DO UPDATE SET valor = EXCLUDED.valor
    """)

    with engine.begin() as conn:
        try:
            res = conn.execute(insert_sql, {"fecha": fecha, "valor": valor})
            rc = res.rowcount if (res.rowcount is not None and res.rowcount >= 0) else 1
            print(f"[DB] reservas_scrape rowcount={rc}", flush=True)
            return rc
        except Exception as e:
            print(f"[DB] ❌ Error insertando en reservas_scrape: {e}", file=sys.stderr)
            traceback.print_exc()
            raise


def save_compra_venta_to_db(engine: Engine, parsed: dict) -> int:
    """Inserta en la tabla comprasMULCBCRA (date, comprasBCRA)."""
    fecha = parsed.get("fecha")
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
            print(f"[DB] ❌ Error upsert en comprasMULCBCRA: {e}", file=sys.stderr)
            traceback.print_exc()
            raise


# =========================================================
# === MAIN ================================================
# =========================================================

def main():
    start_time = datetime.now(BA_TZ)
    print(f"=== Inicio: {start_time.strftime('%Y-%m-%d %H:%M:%S')} ===", flush=True)

    try:
        parser = argparse.ArgumentParser()
        parser.add_argument(
            "--target-date",
            help="Fecha objetivo en formato YYYY-MM-DD (default: hoy en Buenos Aires).",
            default=None,
        )
        args = parser.parse_args()
        target_date = _parse_target_date(args.target_date)

        print("=== Descargando imagen del BCRA desde X ===", flush=True)
        img_path = download_bcra_image(target_date)

        print("=== Parseando imagen con Google Cloud Vision ===", flush=True)
        parsed = parse_bcra_image(img_path)
        print("JSON parseado:", flush=True)
        print(json.dumps(parsed, indent=2, ensure_ascii=False), flush=True)

        print("=== Guardando en Postgres ===", flush=True)
        try:
            engine = build_engine()
        except Exception:
            return

        try:
            n1 = save_compra_venta_to_db(engine, parsed)
            print(f"✅ Inserted {n1} rows into comprasMULCBCRA", flush=True)
        except Exception:
            return

        try:
            n2 = save_reservas_to_db(engine, parsed)
            print(f"✅ Inserted {n2} rows into reservas_scrape", flush=True)
        except Exception:
            return

    finally:
        end_time = datetime.now(BA_TZ)
        duration = end_time - start_time
        print(f"=== Fin: {end_time.strftime('%Y-%m-%d %H:%M:%S')} (duración: {duration}) ===", flush=True)


if __name__ == "__main__":
    main()
