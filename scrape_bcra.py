import os, json, requests, re, argparse
from urllib.parse import unquote
import sys, traceback
from pathlib import Path
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from PIL import Image

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

# --- Config global ---
BA_TZ = ZoneInfo("America/Argentina/Buenos_Aires")
USERNAME = "BancoCentral_AR"
SAVE_DIR = Path("./bcra_imagenes")
REQUIRED_HASHTAGS = ("#databcra", "#reservasbcra")

_COOKIE_HEADER_KEYS = {
    "authorization",
    "Authorization",
    "user-agent",
    "x-twitter-auth-type",
    "x-twitter-active-user",
    "x-twitter-client-language",
}


def _read_cookies_file(path: Path) -> dict:
    if not path.exists():
        raise RuntimeError(f"No se encontró el archivo de cookies en {path}")
    try:
        data = json.loads(path.read_text())
    except Exception as exc:
        raise RuntimeError(f"No pude parsear {path} como JSON de cookies.") from exc
    return data


def _cookies_to_headers(cookies: dict) -> dict:
    auth = cookies.get("authorization") or cookies.get("Authorization")
    if not auth:
        raise RuntimeError("Las cookies no incluyen el header 'authorization'.")
    ct0 = cookies.get("ct0")
    if not ct0:
        raise RuntimeError("Las cookies no incluyen 'ct0'.")

    if "%3D" in auth or "%2F" in auth or "%2B" in auth:
        auth = unquote(auth)

    headers = {
        "Authorization": auth if auth.startswith("Bearer ") else f"Bearer {auth}",
        "x-csrf-token": ct0,
        "x-twitter-auth-type": cookies.get("x-twitter-auth-type", "OAuth2Session"),
        "x-twitter-active-user": cookies.get("x-twitter-active-user", "yes"),
        "x-twitter-client-language": cookies.get("x-twitter-client-language", "en"),
        "User-Agent": cookies.get(
            "user-agent",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120 Safari/537.36",
        ),
        "Referer": "https://x.com/",
        "Origin": "https://x.com",
    }
    guest_token = cookies.get("gt") or cookies.get("guest_token")
    if guest_token:
        headers["x-guest-token"] = str(guest_token)
    return headers


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

        # Go to login page
        page.goto("https://x.com/i/flow/login", wait_until="domcontentloaded", timeout=60000)
        page.wait_for_timeout(3000)

        # Enter username
        username_input = page.locator('input[autocomplete="username"]')
        username_input.wait_for(state="visible", timeout=30000)
        username_input.fill(username)
        page.wait_for_timeout(500)

        # Click Next
        next_button = page.locator('button:has-text("Next"), button:has-text("Siguiente")')
        next_button.click()
        page.wait_for_timeout(2000)

        # Check if Twitter asks for email/phone verification (unusual activity)
        unusual_check = page.locator('input[data-testid="ocfEnterTextTextInput"]')
        if unusual_check.count() > 0 and unusual_check.is_visible():
            print("[LOGIN] X pide verificación adicional (email/usuario), ingresando...", flush=True)
            unusual_check.fill(username)
            page.wait_for_timeout(500)
            verify_next = page.locator('button[data-testid="ocfEnterTextNextButton"]')
            if verify_next.count() > 0:
                verify_next.click()
                page.wait_for_timeout(2000)

        # Enter password
        password_input = page.locator('input[name="password"], input[type="password"]')
        password_input.wait_for(state="visible", timeout=30000)
        password_input.fill(password)
        page.wait_for_timeout(500)

        # Click Log in
        login_button = page.locator('button[data-testid="LoginForm_Login_Button"]')
        login_button.click()
        page.wait_for_timeout(5000)

        # Check if login succeeded (should be on home or not on login page)
        current_url = page.url
        if "/login" in current_url or "/flow/login" in current_url:
            # Check for error messages
            error = page.locator('[data-testid="error"], [role="alert"]')
            if error.count() > 0:
                error_text = error.first.inner_text()
                print(f"[LOGIN] Error de login: {error_text}", flush=True)
            else:
                print("[LOGIN] Login falló - todavía en página de login", flush=True)
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
        return False
    finally:
        page.close()


def _needs_login(page) -> bool:
    """Check if the current page indicates we need to log in."""
    url = page.url
    if "/i/flow/login" in url or "/login" in url:
        return True
    # Check for empty/blocked page (no content loaded)
    try:
        title = page.title()
        if not title or title.strip() == "":
            return True
    except Exception:
        pass
    return False


def _normalize_tweet_date(iso_ts: str) -> str:
    try:
        return datetime.fromisoformat(iso_ts.replace("Z", "+00:00")).date().isoformat()
    except Exception:
        return ""


def _build_search_query(target_date) -> str:
    since = target_date.isoformat()
    until = (target_date + timedelta(days=1)).isoformat()
    return (
        f"from:{USERNAME} {REQUIRED_HASHTAGS[0]} {REQUIRED_HASHTAGS[1]} "
        f"filter:images since:{since} until:{until}"
    )


def _collect_status_urls(page) -> list[str]:
    urls: list[str] = []
    try:
        anchors = page.locator('a[href*="/status/"]')
        count = anchors.count()
        for idx in range(min(count, 60)):
            href = anchors.nth(idx).get_attribute("href")
            if not href:
                continue
            if href.startswith("/"):
                href = f"https://x.com{href}"
            if href not in urls:
                urls.append(href)
    except Exception:
        return urls
    return urls


def _fetch_image_from_status_urls(context, status_urls: list[str], target_date) -> str:
    target_iso = target_date.isoformat()
    for url in status_urls[:25]:
        page = context.new_page()
        try:
            page.set_default_timeout(60000)
            page.goto(url, wait_until="domcontentloaded", timeout=60000)
            if "/i/flow/login" in page.url or "/login" in page.url:
                continue
            time_el = page.locator("time")
            if time_el.count() == 0:
                continue
            iso_ts = time_el.first.get_attribute("datetime") or ""
            if _normalize_tweet_date(iso_ts) != target_iso:
                continue
            text = ""
            try:
                text = (page.locator("article").first.inner_text() or "").lower()
            except Exception:
                text = ""
            if text and not all(tag in text for tag in REQUIRED_HASHTAGS):
                continue
            meta_img = page.locator('meta[property="og:image"]')
            if meta_img.count() > 0:
                src = meta_img.first.get_attribute("content")
                if src:
                    if "name=" in src:
                        src = re.sub(r"name=[a-z]+", "name=large", src)
                    return src
            img = page.locator("img[src*='twimg.com/media'], img[src*='pbs.twimg.com/media']")
            if img.count() > 0:
                src = img.first.get_attribute("src")
                if src:
                    if "name=" in src:
                        src = re.sub(r"name=[a-z]+", "name=large", src)
                    return src
        finally:
            page.close()
    raise RuntimeError("No encontré imagen navegando a statuses desde perfil/media.")


def _fetch_image_url_with_playwright_search(context, target_date) -> str:
    query = _build_search_query(target_date)
    search_urls = [
        f"https://x.com/search?q={requests.utils.quote(query)}&f=live",
        f"https://x.com/search?q={requests.utils.quote(query)}&src=typed_query",
        f"https://x.com/search?q={requests.utils.quote(query)}&f=top",
    ]
    page = context.new_page()
    try:
        page.set_default_timeout(60000)
        for search_url in search_urls:
            page.goto(search_url, wait_until="domcontentloaded", timeout=60000)
            if "/i/flow/login" in page.url or "/login" in page.url:
                raise RuntimeError("X redirigió al login; cookies inválidas o expiradas.")
            candidates = page.locator("article, div[data-testid='cellInnerDiv']")
            for _ in range(6):
                if candidates.count() == 0:
                    page.wait_for_timeout(1500)
                    page.mouse.wheel(0, 1200)
                    continue
                status_urls = _collect_status_urls(page)
                if status_urls:
                    try:
                        return _fetch_image_from_status_urls(context, status_urls, target_date)
                    except Exception:
                        pass
                page.mouse.wheel(0, 1200)
                page.wait_for_timeout(1500)
        title = ""
        try:
            title = page.title()
        except Exception:
            title = ""
        raise RuntimeError(
            f"No encontré imagen en búsqueda. url={page.url} title={title}"
        )
    finally:
        page.close()


def _fetch_image_url_with_playwright_profile(context, target_date) -> str:
    profile_url = f"https://x.com/{USERNAME}"
    page = context.new_page()
    try:
        page.set_default_timeout(60000)
        page.goto(profile_url, wait_until="domcontentloaded", timeout=60000)
        try:
            page.wait_for_load_state("networkidle", timeout=60000)
        except Exception:
            pass
        if "/i/flow/login" in page.url or "/login" in page.url:
            raise RuntimeError("X redirigió al login; cookies inválidas o expiradas.")
        target_iso = target_date.isoformat()
        candidates = page.locator("article, div[data-testid='cellInnerDiv']")
        for _ in range(8):
            if candidates.count() == 0:
                page.wait_for_timeout(1500)
                page.mouse.wheel(0, 1400)
                continue
            for idx in range(candidates.count()):
                try:
                    node = candidates.nth(idx)
                    time_el = node.locator("time")
                    if time_el.count() == 0:
                        continue
                    iso_ts = time_el.first.get_attribute("datetime") or ""
                    if _normalize_tweet_date(iso_ts) != target_iso:
                        continue
                    text = ""
                    try:
                        text = (node.inner_text() or "").lower()
                    except Exception:
                        text = ""
                    if text and not all(tag in text for tag in REQUIRED_HASHTAGS):
                        continue
                    img = node.locator("img[src*='twimg.com/media'], img[src*='pbs.twimg.com/media']")
                    if img.count() == 0:
                        continue
                    src = img.first.get_attribute("src")
                    if not src:
                        continue
                    if "name=" in src:
                        src = re.sub(r"name=[a-z]+", "name=large", src)
                    return src
                except Exception:
                    continue
            page.mouse.wheel(0, 1400)
            page.wait_for_timeout(1500)
        title = ""
        try:
            title = page.title()
        except Exception:
            title = ""
        raise RuntimeError(
            f"No encontré imagen en perfil. url={page.url} title={title}"
        )
    finally:
        page.close()


def _fetch_image_url_with_playwright_profile_media(context, target_date) -> str:
    media_url = f"https://x.com/{USERNAME}/media"
    page = context.new_page()
    try:
        page.set_default_timeout(60000)
        page.goto(media_url, wait_until="domcontentloaded", timeout=60000)
        try:
            page.wait_for_load_state("networkidle", timeout=60000)
        except Exception:
            pass
        if "/i/flow/login" in page.url or "/login" in page.url:
            raise RuntimeError("X redirigió al login; cookies inválidas o expiradas.")
        target_iso = target_date.isoformat()
        candidates = page.locator("article, div[data-testid='cellInnerDiv']")
        for _ in range(8):
            if candidates.count() == 0:
                page.wait_for_timeout(1500)
                page.mouse.wheel(0, 1400)
                continue
            for idx in range(candidates.count()):
                try:
                    node = candidates.nth(idx)
                    time_el = node.locator("time")
                    if time_el.count() == 0:
                        continue
                    iso_ts = time_el.first.get_attribute("datetime") or ""
                    if _normalize_tweet_date(iso_ts) != target_iso:
                        continue
                    img = node.locator("img[src*='twimg.com/media'], img[src*='pbs.twimg.com/media']")
                    if img.count() == 0:
                        continue
                    src = img.first.get_attribute("src")
                    if not src:
                        continue
                    if "name=" in src:
                        src = re.sub(r"name=[a-z]+", "name=large", src)
                    return src
                except Exception:
                    continue
            page.mouse.wheel(0, 1400)
            page.wait_for_timeout(1500)
        status_urls = _collect_status_urls(page)
        try:
            return _fetch_image_from_status_urls(context, status_urls, target_date)
        except Exception:
            pass
        title = ""
        try:
            title = page.title()
        except Exception:
            title = ""
        raise RuntimeError(
            f"No encontré imagen en perfil/media. url={page.url} title={title}"
        )
    finally:
        page.close()


def _try_scrape_methods(context, target_date) -> str:
    """Try all scraping methods. Returns image URL or raises exception."""
    errors = []
    # Try profile/media first, then search, then profile
    try:
        return _fetch_image_url_with_playwright_profile_media(context, target_date)
    except Exception as e:
        errors.append(f"profile/media: {e}")
    try:
        return _fetch_image_url_with_playwright_search(context, target_date)
    except Exception as e:
        errors.append(f"search: {e}")
    try:
        return _fetch_image_url_with_playwright_profile(context, target_date)
    except Exception as e:
        errors.append(f"profile: {e}")
    raise RuntimeError(f"Todos los métodos fallaron: {'; '.join(errors)}")


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
            headers = _cookies_to_headers(cookies)
            pw_cookies = _cookies_to_playwright_list(cookies)
            user_agent = headers.get("User-Agent", user_agent)
        except Exception as e:
            print(f"[SCRAPER] Cookies inválidas, se intentará login: {e}", flush=True)
            cookies = {}
            pw_cookies = []

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(
            user_agent=user_agent,
            locale="es-ES",
            timezone_id="America/Argentina/Buenos_Aires",
        )
        try:
            if pw_cookies:
                context.add_cookies(pw_cookies)

            page_headers = {
                "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
                "accept-language": "es-ES,es;q=0.9,en;q=0.8",
                "upgrade-insecure-requests": "1",
            }
            context.set_extra_http_headers(page_headers)

            # First attempt with existing cookies (or no cookies)
            try:
                return _try_scrape_methods(context, target_date)
            except RuntimeError as e:
                error_msg = str(e).lower()
                needs_auth = (
                    "login" in error_msg or
                    "cookies" in error_msg or
                    "title=" in error_msg or  # Empty title usually means blocked/no auth
                    not pw_cookies  # No cookies loaded
                )
                if not needs_auth:
                    raise

                print(f"[SCRAPER] Sesión inválida o expirada, intentando auto-login...", flush=True)

                # Try auto-login
                if _perform_twitter_login(context, cookies_path):
                    print("[SCRAPER] Login exitoso, reintentando scraping...", flush=True)
                    # Retry after login
                    return _try_scrape_methods(context, target_date)
                else:
                    raise RuntimeError(
                        "No se pudo hacer login automático. "
                        "Verificá X_USERNAME y X_PASSWORD o renovar cookies manualmente."
                    )
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

    print("[SCRAPER] Descargando imagen vía Playwright...", flush=True)
    cookies_path = _resolve_cookies_file()
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
    Maneja tanto el formato donde el número viene ANTES del label (Tesseract)
    como el formato donde viene DESPUÉS.
    """
    texto = _clean_text(texto_ocr)
    low = texto.lower()

    fecha = _extract_fecha(texto)

    reservas = None
    # Patrón 1: número ANTES de "reservas"
    m_res_before = re.search(r"([\d\.,]+)\s+reservas", low, flags=re.IGNORECASE)
    if m_res_before:
        try:
            reservas = _normalize_number_es(m_res_before.group(1))
        except Exception:
            reservas = None

    # Patrón 2: "reservas" seguido de número (con texto basura en el medio)
    if reservas is None:
        m_res_after = re.search(r"reservas\s+(?:en\s+)?(?:millones\s+)?(?:de\s+)?(?:usd)?\W{0,20}([\d\.,]+)", low, flags=re.IGNORECASE)
        if m_res_after:
            try:
                reservas = _normalize_number_es(m_res_after.group(1))
            except Exception:
                reservas = None

    # Patrón 3: buscar número grande (>10000)
    if reservas is None:
        all_numbers = re.findall(r"[\d\.,]+", low)
        for num_str in all_numbers:
            try:
                val = _normalize_number_es(num_str)
                if val > 10000:
                    reservas = val
                    break
            except Exception:
                continue

    compra_venta = 0.0
    if "sin intervención" in low or "sin intervencion" in low:
        compra_venta = 0.0
    else:
        # Patrón 1: número ANTES de "compra"
        m_cv_before = re.search(r"([-+]?\s*[\d\.,\]]+)\s+compra", low, flags=re.IGNORECASE)
        if m_cv_before:
            try:
                compra_venta = _normalize_number_es(m_cv_before.group(1))
            except Exception:
                compra_venta = 0.0

        # Patrón 2: "compra/venta" seguido de número
        if compra_venta == 0.0:
            m_cv_after = re.search(r"(compra|venta)\s+de\s+divisas\D{0,30}([-+]?\s*[\d\.,]+)", low, flags=re.IGNORECASE)
            if m_cv_after:
                try:
                    compra_venta = _normalize_number_es(m_cv_after.group(2))
                except Exception:
                    compra_venta = 0.0

        # Patrón 3: buscar número pequeño (1-500) que no sea día
        if compra_venta == 0.0:
            all_numbers = re.findall(r"[\d\.,]+", low)
            for num_str in all_numbers:
                try:
                    val = _normalize_number_es(num_str)
                    if 1 <= val <= 500 and val != 19:
                        compra_venta = val
                        break
                except Exception:
                    continue

    if reservas is None:
        raise RuntimeError("No pude extraer 'reservas_millones_usd' del OCR.")

    return {
        "fecha": fecha,
        "reservas_millones_usd": float(reservas),
        "compra_venta_divisas_millones_usd": float(compra_venta),
    }


# =========================================================
# === OCR CON TESSERACT ===================================
# =========================================================

def parse_bcra_image(img_path: Path) -> dict:
    """OCR usando Tesseract + parseo por regex."""
    try:
        import pytesseract
    except ImportError as exc:
        raise RuntimeError(
            "Falta instalar 'pytesseract'. Ejecutá: pip install pytesseract"
        ) from exc

    print("[OCR] Ejecutando Tesseract...", flush=True)
    image = Image.open(img_path).convert("RGB")
    # Probar PSM 11 (sparse text) - mejor para infografías
    texto = pytesseract.image_to_string(image, lang='spa+eng', config='--psm 11')

    if not texto.strip():
        raise RuntimeError("Tesseract no devolvió texto.")

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

        print("=== Parseando imagen con Tesseract ===", flush=True)
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
