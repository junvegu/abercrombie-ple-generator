from pathlib import Path
import pandas as pd
import re
import logging
from collections import defaultdict
import calendar  # para calcular último día del mes

# ------------------------------------------------------------------
# Configuración general
# ------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(levelname)s: %(message)s"
)

RUC = "20470526379"  # ← actualízalo si cambia tu número de RUC
INPUT_DIR = Path(".")              # carpeta donde pones los Excel
OUTPUT_DIR = INPUT_DIR / "output_txt"  # carpeta destino de los .TXT PLE
OUTPUT_DIR.mkdir(exist_ok=True)

MESES = {
    "ENERO": "01", "FEBRERO": "02", "MARZO": "03", "ABRIL": "04", "MAYO": "05",
    "JUNIO": "06", "JULIO": "07", "AGOSTO": "08", "SETIEMBRE": "09", "SEPTIEMBRE": "09",
    "OCTUBRE": "10", "NOVIEMBRE": "11", "DICIEMBRE": "12"
}

# ------------------------------------------------------------------
# Utilidades genéricas
# ------------------------------------------------------------------

def extraer_mes_archivo(nombre: str) -> str | None:
    """Devuelve el número de mes (01‑12) detectado en el nombre del archivo."""
    for mes, codigo in MESES.items():
        if mes in nombre.upper():
            return codigo
    return None


def ultimo_dia_mes(anno: int, mes: int) -> int:
    """Devuelve el último día (28‑31) para *mes* y *anno*."""
    return calendar.monthrange(anno, mes)[1]


def normalizar_codigo(codigo) -> str:
    """Convierte el código de cuenta a string limpio y en mayúsculas."""
    if pd.isna(codigo):
        return ""
    if isinstance(codigo, float) and codigo.is_integer():
        return str(int(codigo))
    return str(codigo).strip().replace(" ", "").upper()


def canon(col: str) -> str:
    """Normaliza nombres de columnas (minúsculas, 1 espacio entre palabras)."""
    return re.sub(r"\s+", " ", str(col).lower()).strip()


def buscar_columna(df: pd.DataFrame, *keywords: str) -> str | None:
    """Devuelve el primer nombre de columna que contenga *todos* los keywords (canon)."""
    kws = [canon(k) for k in keywords]
    for original in df.columns:
        c = canon(original)
        if all(k in c for k in kws):
            return original
    return None

# ------------------------------------------------------------------
#  Parseo de Transaction Reference  →  (serie, número)
# ------------------------------------------------------------------

def parse_doc(ref_raw: str) -> tuple[str, str]:
    """Extrae serie y número desde Transaction Reference (normalizado)."""
    if pd.isna(ref_raw):
        return "", ""
    ref = str(ref_raw).strip().upper()
    if not ref:
        return "", ""

    ref = ref.replace("/", "-").replace(" ", "-")

    # Patrón SERIE-NUMERO  (F001-5068, B123-45, etc.)
    m = re.match(r"^([A-Z0-9]{1,10})[-](\d{1,20})$", ref)
    if m:
        return m.group(1), m.group(2).lstrip("0") or "0"

    # Patrón compacto F0015068
    m = re.match(r"^([A-Z]{1}\d{3})(\d{1,20})$", ref)
    if m:
        return m.group(1), m.group(2).lstrip("0") or "0"

    # Solo dígitos → asumimos sin serie
    if ref.isdigit():
        return "", ref.lstrip("0") or "0"

    return "", "0"

# ------------------------------------------------------------------
# Procesamiento principal
# ------------------------------------------------------------------

def procesar_excel(archivo: Path) -> None:
    logging.info(f"Procesando {archivo.name}")
    mes = extraer_mes_archivo(archivo.stem)
    if not mes:
        logging.warning("No se pudo detectar el mes en el nombre de archivo – se omite.")
        return

    try:
        xls = pd.ExcelFile(archivo)
        diario_df = xls.parse(sheet_name=4)  # Hoja 5: Libro Diario
        pc_df     = xls.parse(sheet_name=5)  # Hoja 6: Plan de Cuentas
    except Exception as e:
        logging.error(f"Error leyendo hojas: {e}")
        return

    diario_df.columns = [str(c) for c in diario_df.columns]
    pc_df.columns     = [str(c) for c in pc_df.columns]

    # ---------------- Plan de cuentas ----------------
    col_cuenta_pc     = buscar_columna(pc_df, "cuenta peruana") or pc_df.columns[2]

    # Tomar "Nombre de Cuenta Contable" (preferencia por encabezado; fallback a columna D)
    col_nombre_cuenta = (
        buscar_columna(pc_df, "nombre", "cuenta", "contable")
        or (pc_df.columns[3] if len(pc_df.columns) > 3 else pc_df.columns[1])
    )
    logging.info(f"Plan de Cuentas: usando columna de NOMBRE = '{col_nombre_cuenta}'")

    pc_df = pc_df.fillna("")
    pc_df["codigo_cuenta"] = pc_df[col_cuenta_pc].map(normalizar_codigo)
    pc_df = pc_df[pc_df["codigo_cuenta"] != ""]

    # ------------------ eliminar duplicados de código de cuenta ------------------
    # preferir nombre más largo si hay múltiples registros para un mismo código
    pc_df["nombre_temp"] = pc_df.get(col_nombre_cuenta, "").astype(str).fillna("").str.strip()
    before = len(pc_df)
    # ordenar para que el nombre más descriptivo (más largo) quede primero
    pc_df["name_len"] = pc_df["nombre_temp"].str.len()
    pc_df = (
        pc_df.sort_values(by=["codigo_cuenta", "name_len"], ascending=[True, False])
             .drop_duplicates(subset=["codigo_cuenta"], keep="first")
    )
    after = len(pc_df)
    logging.info(f"Plan de Cuentas: removidos {before - after} duplicados de código de cuenta (quedan {after}).")
    # limpiar columnas auxiliares
    pc_df = pc_df.drop(columns=["nombre_temp", "name_len"] )

    # ---------------- Libro Diario ----------------
    col_cuenta_diario = buscar_columna(diario_df, "cuenta peruana")
    col_fecha         = buscar_columna(diario_df, "transaction date")

    # Glosa: preferimos "Description.1" (AA); si no, "Description" genérica.
    col_glosa = "Description.1" if "Description.1" in diario_df.columns else buscar_columna(diario_df, "description")

    col_debe_credit   = buscar_columna(diario_df, "debit/credit")
    col_monto         = buscar_columna(diario_df, "transaction amount") or buscar_columna(diario_df, "base amount")
    col_journal_num   = buscar_columna(diario_df, "journal number")
    col_journal_type  = buscar_columna(diario_df, "journal type")
    col_currency      = buscar_columna(diario_df, "transaction currency code")
    col_ref_doc       = buscar_columna(diario_df, "transaction reference")

    required = [col_cuenta_diario, col_fecha, col_glosa, col_monto, col_journal_num, col_journal_type]
    if any(c is None for c in required):
        logging.error("Faltan columnas requeridas en Diario – verifica nombres.")
        logging.error("Columnas detectadas → cuenta=%s, fecha=%s, glosa=%s, monto=%s, jnum=%s, jtype=%s",
                      col_cuenta_diario, col_fecha, col_glosa, col_monto, col_journal_num, col_journal_type)
        logging.info("Encabezados disponibles en Diario: %s", list(diario_df.columns))
        return

    # Año de trabajo (deducido del nombre o fijo 2020 aquí)
    ANIO = 2020
    dia_final = ultimo_dia_mes(ANIO, int(mes))
    periodo_plan = f"{ANIO}{mes}{dia_final:02d}"   # 20200131 para plan de cuentas
    periodo_diario = f"{ANIO}{mes}00"              # 20200100 para libro diario (regla SUNAT)

    diario_lines = []
    correlativos_tipo: defaultdict[str, int] = defaultdict(int)
    cuentas_validas = set(pc_df["codigo_cuenta"].values)

    for _, row in diario_df.iterrows():
        cuenta = normalizar_codigo(row[col_cuenta_diario])
        if cuenta == "" or cuenta not in cuentas_validas:
            continue

        j_type = str(row[col_journal_type]).strip().upper()

        # --- CUO agrupar mínimo (puedes refinar después) ---
        correlativos_tipo[j_type] += 1
        cuo = f"{j_type}{correlativos_tipo[j_type]:03d}"

        # ---------------- Correlativo (M + JournalNumber) -------------
        jnum_raw = row[col_journal_num]
        jnum_str = "" if pd.isna(jnum_raw) else str(jnum_raw).rstrip(".0")
        correlativo = f"M{jnum_str}"

        # Fecha original de la línea
        fecha_oper = pd.to_datetime(row[col_fecha], errors="coerce")
        dia_target = dia_final
        fecha_target_str = f"{dia_target:02d}/{mes}/{ANIO}"

        # ---------------- Estado + Fecha + Periodo por línea según reglas ----------------
        estado = "1"
        periodo_linea = periodo_diario
        if pd.isna(fecha_oper):
            fecha_str = fecha_target_str
        else:
            if fecha_oper.year == ANIO:
                if fecha_oper.month < int(mes):
                    estado = "8"
                    fecha_str = fecha_oper.strftime("%d/%m/%Y")
                    periodo_linea = f"{ANIO}{fecha_oper.month:02d}00"
                elif fecha_oper.month == int(mes):
                    estado = "1"
                    fecha_str = fecha_oper.strftime("%d/%m/%Y")
                else:
                    estado = "1"
                    fecha_str = fecha_target_str
            else:
                if fecha_oper.year < ANIO:
                    estado = "8"
                    fecha_str = fecha_oper.strftime("%d/%m/%Y")
                    periodo_linea = f"{fecha_oper.year}{fecha_oper.month:02d}00"
                else:
                    estado = "1"
                    fecha_str = fecha_target_str

        # Montos – Debe / Haber
        try:
            monto_raw = float(row.get(col_monto, 0))
        except ValueError:
            monto_raw = 0.0

        if col_debe_credit:
            flag = str(row[col_debe_credit]).strip().upper()
            if flag.startswith("D"):
                debe, haber = abs(monto_raw), 0.0
            elif flag.startswith("C"):
                debe, haber = 0.0, abs(monto_raw)
            else:
                debe, haber = (monto_raw, 0.0) if monto_raw > 0 else (0.0, abs(monto_raw))
        else:
            debe, haber = (monto_raw, 0.0) if monto_raw > 0 else (0.0, abs(monto_raw))

        # Moneda (Transaction Currency Code)
        moneda = "PEN"
        if col_currency:
            moneda_val = str(row.get(col_currency, "")).strip().upper()
            if moneda_val.startswith("USD"):
                moneda = "USD"
            elif moneda_val.startswith("PEN") or moneda_val.startswith("SOL"):
                moneda = "PEN"
            elif moneda_val:
                moneda = moneda_val[:3]

        # Tipo, serie y número de comprobante
        serie_doc, num_doc = parse_doc(row.get(col_ref_doc, "")) if col_ref_doc else ("", "")
        if serie_doc.startswith("F"):
            tipo_cmp = "01"
        elif serie_doc.startswith("B"):
            tipo_cmp = "03"
        elif serie_doc.startswith("T"):
            tipo_cmp = "12"
        else:
            tipo_cmp = "00"

        # Si falta número cuando el tipo es válido, se rellena con '0' (campo obligatorio)
        if tipo_cmp != "00" and not num_doc:
            logging.warning(f"Comprobante tipo {tipo_cmp} sin número: se pondrá '0' de fallback.")
            num_doc = "0"

        glosa_val = str(row.get(col_glosa, "")).strip()

        line = [
            periodo_linea,  # 1 – Periodo ajustado por línea
            cuo,            # 2 – CUO
            correlativo,    # 3 – Correlativo
            cuenta,         # 4 – Cuenta contable
            "", "",         # 5‑6 – subcuenta / CCosto (vacío)
            moneda,         # 7 – Moneda
            "", "",         # 8‑9 – TC y glosa TC (vacío)
            tipo_cmp,       # 10 – Tipo de Comprobante
            serie_doc,      # 11 – Serie
            num_doc,        # 12 – Número
            "", "",         # 13‑14 – Doc ref (vacío)
            fecha_str,      # 15 – Fecha
            glosa_val,      # 16 – Glosa
            "",             # 17 – Código libro (vacío)
            f"{debe:.2f}",  # 18 – Debe
            f"{haber:.2f}", # 19 – Haber
            "",             # 20 – Campo libre
            estado          # 21 – Estado
        ]
        diario_lines.append("|".join(line) + "|")
    # ----------------‑‑ Guardado archivos ----------------
    archivo_diario = OUTPUT_DIR / f"LE{RUC}{ANIO}{mes}00050100001111.txt"
    archivo_diario.write_text("\n".join(diario_lines), encoding="utf-8")
    logging.info(f"Diario PLE → {archivo_diario.name}  (líneas: {len(diario_lines)})")

    # ----- Plan de Cuentas (usa periodo con día final) -----
    plan_lines = []
    for _, row in pc_df.iterrows():
        cuenta = row["codigo_cuenta"]
        nombre = str(row.get(col_nombre_cuenta, "")).strip()
        line = [
            periodo_plan, cuenta, nombre, "01", "", "", "", "1", ""
        ]
        plan_lines.append("|".join(line) + "|")

    archivo_plan = OUTPUT_DIR / f"LE{RUC}{ANIO}{mes}00050300001111.txt"
    archivo_plan.write_text("\n".join(plan_lines), encoding="utf-8")
    logging.info(f"Plan de Ctas PLE → {archivo_plan.name}  (líneas: {len(plan_lines)})")
    logging.info("------------------------------------------------------------\n")

# ------------------------------------------------------------------
# Búsqueda de archivos y ejecución
# ------------------------------------------------------------------

def main():
    archivos = list(INPUT_DIR.glob("DIARIO,*2020_2.xlsx"))
    if not archivos:
        logging.warning("No se encontraron archivos con patrón 'DIARIO,*2020_2.xlsx'")
    for archivo in archivos:
        procesar_excel(archivo)


if __name__ == "__main__":
    main()
