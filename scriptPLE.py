
from pathlib import Path
import pandas as pd
import re
import logging

# ------------------------------------------------------------------
# Configuración general
# ------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(levelname)s: %(message)s"
)

RUC = "20470526379"
INPUT_DIR = Path(".")
OUTPUT_DIR = INPUT_DIR / "output_txt"
OUTPUT_DIR.mkdir(exist_ok=True)

MESES = {
    "ENERO": "01", "FEBRERO": "02", "MARZO": "03", "ABRIL": "04", "MAYO": "05",
    "JUNIO": "06", "JULIO": "07", "AGOSTO": "08", "SETIEMBRE": "09", "SEPTIEMBRE": "09",
    "OCTUBRE": "10", "NOVIEMBRE": "11", "DICIEMBRE": "12"
}

# ------------------------------------------------------------------
# Utilidades
# ------------------------------------------------------------------

def extraer_mes_archivo(nombre: str) -> str | None:
    """Devuelve el número de mes (01‑12) detectado en el nombre del archivo."""
    for mes, codigo in MESES.items():
        if mes in nombre.upper():
            return codigo
    return None


def normalizar_codigo(codigo) -> str:
    """Convierte el código de cuenta a string limpio y en mayúsculas.
    - Convierte floats como 4001.0 en "4001".
    - Elimina espacios y relleno.
    - Devuelve string vacío si el valor es NaN / None.
    """
    if pd.isna(codigo):
        return ""
    if isinstance(codigo, float) and codigo.is_integer():
        return str(int(codigo))
    return str(codigo).strip().replace(" ", "").upper()


def canon(col: str) -> str:
    """Normaliza nombres de columnas (minúsculas, 1 espacio entre palabras)."""
    return re.sub(r"\s+", " ", col.lower()).strip()


def buscar_columna(df: pd.DataFrame, *keywords: str) -> str | None:
    """Devuelve el primer nombre de columna que contenga todos los *keywords* (canon)."""
    kws = [canon(k) for k in keywords]
    for original in df.columns:
        c = canon(original)
        if all(k in c for k in kws):
            return original
    return None

# ------------------------------------------------------------------
# Procesamiento principal
# ------------------------------------------------------------------

def procesar_excel(archivo: Path) -> None:
    logging.info(f"Procesando {archivo.name}")
    nombre_archivo = archivo.stem
    mes = extraer_mes_archivo(nombre_archivo)
    if not mes:
        logging.warning("No se pudo detectar el mes en el nombre de archivo – se omite.")
        return

    try:
        xls = pd.ExcelFile(archivo)
        diario_df = xls.parse(sheet_name=4)  # Libro Diario
        pc_df = xls.parse(sheet_name=5)      # Plan de Cuentas
    except Exception as e:
        logging.error(f"Error leyendo hojas: {e}")
        return

    # Normalizar encabezados solo para búsquedas (no modificamos los nombres originales)
    diario_df.columns = [str(c) for c in diario_df.columns]
    pc_df.columns = [str(c) for c in pc_df.columns]

    # -------------------------
    # PLAN DE CUENTAS
    # -------------------------
    pc_df = pc_df.fillna("")
    col_cuenta_pc = buscar_columna(pc_df, "cuenta peruana") or pc_df.columns[2]
    pc_df["codigo_cuenta"] = pc_df[col_cuenta_pc].map(normalizar_codigo)
    col_nombre_cuenta = pc_df.columns[1]

    pc_df = pc_df[pc_df["codigo_cuenta"] != ""]

    # -------------------------
    # LIBRO DIARIO – localización columnas clave
    # -------------------------
    col_cuenta_diario = buscar_columna(diario_df, "cuenta peruana")
    if not col_cuenta_diario:
        logging.error("No se halló columna de cuenta contable en Diario.")
        return

    col_fecha = buscar_columna(diario_df, "transaction date") or buscar_columna(diario_df, "date")
    col_glosa = buscar_columna(diario_df, "description")
    col_debe = buscar_columna(diario_df, "debit/credit")  # Usamos signo para determinar
    col_monto = buscar_columna(diario_df, "transaction amount") or buscar_columna(diario_df, "base amount")
    col_journal_number = buscar_columna(diario_df, "journal number")
    col_journal_type = buscar_columna(diario_df, "journal type")

    if None in [col_fecha, col_glosa, col_debe, col_monto, col_journal_number, col_journal_type]:
        logging.error("Faltan columnas requeridas en Diario.")
        return

    # ♦ Tipo / serie / número de comprobante
    col_tipo_doc = buscar_columna(diario_df, "tipo de documento")
    col_serie_doc = buscar_columna(diario_df, "serie") or buscar_columna(diario_df, "journal source")
    col_num_doc = buscar_columna(diario_df, "numero del comprobante") or buscar_columna(diario_df, "transaction reference")

    periodo_ple = f"2020{mes}00"

    # -------------------------
    # Generación líneas Libro Diario 5.1
    # -------------------------
    diario_lines = []
    cuo_cache: dict[str, int] = {}

    for _, row in diario_df.iterrows():
        cuenta = normalizar_codigo(row[col_cuenta_diario])
        if cuenta == "" or cuenta not in set(pc_df["codigo_cuenta" ].values):
            continue

        # CUO = tipo + número (sin decimales)
        j_type = str(row[col_journal_type]).strip().upper()
        j_number_raw = str(row[col_journal_number]).split(".")[0]
        cuo = f"{j_type}{j_number_raw}"

        # Correlativo interno M1/M2…
        idx = cuo_cache.get(cuo, 0) + 1
        cuo_cache[cuo] = idx
        correlativo = f"M{idx}"

        # Fechas y montos
        fecha_oper = pd.to_datetime(row[col_fecha])
        fecha_str = fecha_oper.strftime("%d/%m/%Y") if not pd.isna(fecha_oper) else ""

        monto = float(row[col_monto] or 0)
        if str(row[col_debe]).strip().upper().startswith("D") or monto > 0:
            debe = monto
            haber = 0
        else:
            debe = 0
            haber = abs(monto)

        # Tipo de comprobante heurístico
        serie_doc = str(row.get(col_serie_doc, "")).strip().upper() if col_serie_doc else ""
        if serie_doc.startswith("F"):
            tipo_cmp = "01"
        elif serie_doc.startswith("B"):
            tipo_cmp = "03"
        elif serie_doc.startswith("T"):
            tipo_cmp = "12"
        else:
            tipo_cmp = "00"

        num_doc = str(row.get(col_num_doc, "")).strip().upper() if col_num_doc else ""

        line = [
            periodo_ple,         # 1  Periodo
            cuo,                 # 2  CUO
            correlativo,         # 3  Correlativo
            cuenta,              # 4  Cuenta contable
            "",                 # 5  Unidad Operación (vacío)
            "",                 # 6  Centro costos (vacío)
            "PEN",              # 7  Moneda (harcode PEN, ajusta si usas otra)
            "", "",            # 8‑9  tipo/num doc Identidad emisor (vacío)
            tipo_cmp,            # 10 Tipo Comprobante
            serie_doc,           # 11 Serie
            num_doc,             # 12 Número Doc
            fecha_str,           # 13 Fecha contable
            "",                 # 14 Fecha vencimiento
            fecha_str,           # 15 Fecha operación
            str(row[col_glosa])[:200],  # 16 Glosa
            "",                 # 17 Glosa referencial
            f"{debe:.2f}",      # 18 Debe
            f"{haber:.2f}",     # 19 Haber
            "",                 # 20 Dato Estructurado (ventas/compras)
            "1"                 # 21 Estado (1 vigente)
        ]
        diario_lines.append("|".join(line))

    archivo_diario = OUTPUT_DIR / f"LE{RUC}2020{mes}00050100001111.txt"
    archivo_diario.write_text("\n".join(diario_lines), encoding="utf-8")
    logging.info(f"Libro Diario generado → {archivo_diario}  líneas: {len(diario_lines)}")

    # -------------------------
    # Plan de Cuentas 6.1 (filtrado a las usadas)
    # -------------------------
    cuentas_usadas = set(canon(c) for c in diario_df[col_cuenta_diario].map(normalizar_codigo))
    pc_filtrado = pc_df[pc_df["codigo_cuenta"].isin(cuentas_usadas)]

    plan_lines = []
    for _, row in pc_filtrado.iterrows():
        line = [
            periodo_ple,
            row["codigo_cuenta"],
            str(row[col_nombre_cuenta]).strip()[:200],
            "01",  # Código PCGE (asumido)
            "", "", "",  # campos vacíos
            "1",   # Estado 1
            ""      # libre
        ]
        plan_lines.append("|".join(line))

    archivo_plan = OUTPUT_DIR / f"LE{RUC}2020{mes}00060300001111.txt"
    archivo_plan.write_text("\n".join(plan_lines), encoding="utf-8")
    logging.info(f"Plan de Cuentas generado → {archivo_plan}  líneas: {len(plan_lines)}")
    logging.info("------------------------------------------------------------\n")

# ------------------------------------------------------------------
# Ejecución
# ------------------------------------------------------------------

def main():
    archivos = list(INPUT_DIR.glob("DIARIO,*2020_2.xlsx"))
    if not archivos:
        logging.warning("No se encontraron archivos con patrón 'DIARIO,*2020_2.xlsx'")
    for archivo in archivos:
        procesar_excel(archivo)

if __name__ == "__main__":
    main()
