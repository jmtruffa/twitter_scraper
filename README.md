# BCRA Twitter Scraper

Scraper que extrae datos de reservas y operaciones de divisas del BCRA desde la cuenta oficial de Twitter [@BancoCentral_AR](https://x.com/BancoCentral_AR).

## Funcionamiento

1. Busca tweets con hashtags `#DataBCRA` o `#ReservasBCRA` para la fecha indicada
2. Descarga la imagen adjunta del tweet
3. Extrae los datos via OCR (Tesseract)
4. Guarda en PostgreSQL (tablas `reservas_scrape` y `comprasMULCBCRA2`)

## Requisitos

### Sistema
```bash
# macOS
brew install tesseract tesseract-lang

# Ubuntu/Debian
sudo apt install tesseract-ocr tesseract-ocr-spa
```

### Python
```bash
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt
playwright install chromium
```

## Configuracion de Cookies

El scraper requiere cookies de una sesion autenticada en X.com. Las cookies expiran cada 1-3 meses.

### Paso 1: Exportar cookies del browser

1. Instalar extension [Cookie-Editor](https://cookie-editor.cgagnier.ca/) en tu browser
2. Ir a [x.com](https://x.com) y loguearte
3. Click en Cookie-Editor -> Export -> JSON
4. Guardar como `~/Downloads/x_cookies.json`

### Paso 2: Procesar cookies

```bash
# Para uso local (desarrollo)
./process_cookies.sh local

# Para enviar al servidor GCP (produccion)
./process_cookies.sh cloud
```

El script:
- Lee `~/Downloads/x_cookies.json`
- Extrae solo las cookies necesarias (`auth_token`, `ct0`, etc.)
- Las guarda en formato compatible con el scraper
- Elimina el archivo original

## Uso

```bash
source venv/bin/activate

# Scraper para la fecha de hoy
python scrape_bcra.py

# Scraper para una fecha especifica
python scrape_bcra.py --target-date 2026-01-22
```

## Variables de Entorno

### Cookies (opcional si existe ./cookies.json)
```bash
export X_COOKIES_FILE=/ruta/a/cookies.json
```

### Base de datos
```bash
export POSTGRES_USER=usuario
export POSTGRES_PASSWORD=password
export POSTGRES_HOST=localhost
export POSTGRES_DB=bcra
export POSTGRES_PORT=5432  # opcional, default 5432
```

## Tablas de Base de Datos

### reservas_scrape
| Columna | Tipo | Descripcion |
|---------|------|-------------|
| date | DATE | Fecha del dato |
| valor | NUMERIC | Reservas en millones USD |

### comprasMULCBCRA2
| Columna | Tipo | Descripcion |
|---------|------|-------------|
| date | DATE | Fecha del dato |
| comprasBCRA | NUMERIC | Intervencion cambiaria (+ compra, - venta, 0 sin intervencion) |

## Estructura del Proyecto

```
twitter_scraper/
├── scrape_bcra.py      # Script principal
├── process_cookies.sh  # Procesa cookies de Cookie-Editor
├── cookies.json        # Cookies procesadas (no commitear)
├── bcra_imagenes/      # Imagenes descargadas
├── requirements.txt    # Dependencias Python
└── README.md
```

## Cronjob (servidor)

El scraper corre automaticamente via cron. Para configurar:

```bash
crontab -e

# Ejemplo: correr todos los dias a las 19:00 hora Argentina
0 19 * * * /home/jmt/dev/python/twitter_scraper/run_scraper.sh >> /var/log/bcra_scraper.log 2>&1
```

## Troubleshooting

### "Las cookies no incluyen 'auth_token'"
Las cookies expiraron o no se exportaron correctamente. Repetir el proceso de exportacion con Cookie-Editor.

### "X redirigió al login; cookies inválidas o expiradas"
Misma solucion: exportar cookies nuevamente.

### No encuentra el tweet de la fecha
- El BCRA no publico datos ese dia (fines de semana, feriados)
- El tweet fue borrado o modificado
- Probar con `--target-date` para una fecha especifica
