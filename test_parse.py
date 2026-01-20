import re

texto = '''BANCO CENTRAL
DE LA REPÚBLICA ARGENTINA

#DATABCRA
PRINCIPALES
VARIABLES

LUNES 19
DE ENERO DE 2026

da BADLARS@,19% TEA com car

44.808

Reservas en millones
de USD"

2]

Compra de divisas
en millones de USD
'''

texto = texto.replace("\u2212", "-")
texto = re.sub(r"[ \t]+", " ", texto)
low = texto.lower()

print("Buscando reservas...")
m = re.search(r"([\d\.,]+)\s+reservas", low, flags=re.IGNORECASE)
print(f"Match: {m}")
if m:
    print(f"Valor: {m.group(1)}")
