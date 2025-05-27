from fastapi import FastAPI, File, UploadFile, Form, HTTPException
from fastapi.responses import FileResponse
import pandas as pd
import pdfplumber
import tempfile
import pikepdf
import shutil
import os
import re

app = FastAPI()

def desbloquear_pdf(uploaded_file_path, password):
    try:
        unlocked_path = tempfile.mktemp(suffix=".pdf")
        with pikepdf.open(uploaded_file_path, password=password) as pdf:
            pdf.save(unlocked_path)
        return unlocked_path
    except pikepdf._qpdf.PasswordError:
        raise HTTPException(status_code=401, detail="Contraseña incorrecta para el PDF.")

def extraer_anio_desde_pdf(pdf_path):
    with pdfplumber.open(pdf_path) as pdf:
        for page in pdf.pages:
            texto = page.extract_text()
            if not texto:
                continue
            match = re.search(r'\d{1,2}\s*/\s*[A-Z]{3}\s*/\s*(\d{4})', texto)
            if match:
                return match.group(1)
            match = re.search(r'DESDE.*?(\d{4})|HASTA.*?(\d{4})', texto)
            if match:
                return match.group(1) or match.group(2)
    return "2025"

def extraer_movimientos_desde_pdf(pdf_path):
    movimientos = []
    anio = extraer_anio_desde_pdf(pdf_path)
    
    debug_txt_path = os.path.join(tempfile.gettempdir(), "debug_texto_cartola.txt")
    
    with open(debug_txt_path, "w", encoding="utf-8") as debug_file:
        debug_file.write(f"=== EXTRACCIÓN DE MOVIMIENTOS ===\n")
        debug_file.write(f"Año detectado: {anio}\n\n")
        
        with pdfplumber.open(pdf_path) as pdf:
            for page_num, page in enumerate(pdf.pages, start=1):
                # Try to extract the table from the page
                tables = page.extract_tables()
                debug_file.write(f"\n--- Página {page_num} ---\n")
                debug_file.write(f"Tablas encontradas: {len(tables)}\n")
                
                # Fallback to text extraction if no tables are found
                texto = page.extract_text()
                debug_file.write(f"Texto extraído:\n{texto if texto else '[Sin texto detectable]'}\n")
                debug_file.write("\n---------------------------\n")
                
                if tables:
                    for table_num, table in enumerate(tables):
                        debug_file.write(f"\nTabla {table_num + 1}:\n")
                        for row in table:
                            debug_file.write(f"Fila: {row}\n")
                            if not row or len(row) < 5:  # Need at least Fecha, Descripción, Docto No., Cargo/Abono, Saldo
                                continue
                            
                            # Expected columns: Fecha, Descripción, Docto No., Cargo, Abono, Saldo
                            fecha_str = row[0].strip() if row[0] else ''
                            match_fecha = re.match(r'^(\d{1,2})\s*/\s*([A-Z]{3})', fecha_str)
                            if not match_fecha:
                                continue
                            
                            dia = match_fecha.group(1).zfill(2)
                            mes = match_fecha.group(2)
                            fecha_completa = f"{dia}/{mes}/{anio}"
                            
                            descripcion = row[1].strip() if row[1] else ''
                            docto_no = row[2].replace('.', '') if row[2] else ''
                            cargo_str = row[3].replace('.', '').replace(',', '.') if row[3] else '0'
                            abono_str = row[4].replace('.', '').replace(',', '.') if row[4] else '0'
                            saldo_str = row[5].replace('.', '').replace(',', '.') if len(row) > 5 and row[5] else '0'
                            
                            try:
                                cargo = float(cargo_str) if cargo_str else 0.0
                                abono = float(abono_str) if abono_str else 0.0
                                saldo = float(saldo_str) if saldo_str else 0.0
                            except ValueError:
                                debug_file.write(f"Error convirtiendo valores a float: Cargo={cargo_str}, Abono={abono_str}, Saldo={saldo_str}\n")
                                continue
                            
                            # Calculate monto: positive for abonos, negative for cargos
                            monto = abono if abono > 0 else -cargo if cargo > 0 else 0.0
                            
                            movimiento = {
                                "Fecha": fecha_completa,
                                "Descripción": descripcion,
                                "Documento": docto_no,
                                "Cargo": cargo,
                                "Abono": abono,
                                "Monto": monto,
                                "Saldo": saldo
                            }
                            
                            movimientos.append(movimiento)
                            debug_file.write(f"Movimiento extraído: {movimiento}\n")
                else:
                    debug_file.write("No se encontraron tablas, intentando extracción de texto...\n")
                    if not texto:
                        continue
                    
                    lineas = texto.split('\n')
                    debug_file.write(f"\nTotal de líneas en página {page_num}: {len(lineas)}\n")
                    
                    for i, linea in enumerate(lineas):
                        linea = linea.strip()
                        if not linea:
                            continue
                        
                        match_fecha = re.match(r'^(\d{1,2})\s*/\s*([A-Z]{3})\s+(.+)', linea)
                        if match_fecha:
                            dia = match_fecha.group(1).zfill(2)
                            mes = match_fecha.group(2)
                            fecha_completa = f"{dia}/{mes}/{anio}"
                            resto_linea = match_fecha.group(3).strip()
                            
                            debug_file.write(f"\nLínea {i+1}: {linea}\n")
                            debug_file.write(f"Fecha extraída: {fecha_completa}\n")
                            debug_file.write(f"Resto de línea: {resto_linea}\n")
                            
                            # Split by multiple spaces to align with table columns
                            partes = re.split(r'\s{2,}', resto_linea)
                            debug_file.write(f"Partes divididas: {partes}\n")
                            
                            if len(partes) < 3:  # Need at least Descripción, Docto No., and Saldo
                                debug_file.write("No hay suficientes partes para procesar\n")
                                continue
                            
                            descripcion = partes[0].strip()
                            docto_no = partes[1].replace('.', '') if len(partes) > 1 else ''
                            saldo_str = partes[-1].replace('.', '').replace(',', '.')
                            try:
                                saldo = float(saldo_str) if saldo_str else 0.0
                            except ValueError:
                                debug_file.write(f"Error convirtiendo saldo a float: {saldo_str}\n")
                                continue
                            
                            cargo = 0.0
                            abono = 0.0
                            
                            # Extract all numbers to understand the structure
                            numeros = re.findall(r'\d{1,3}(?:\.\d{3})*(?:,\d+)?|\d+', resto_linea)
                            debug_file.write(f"Números encontrados: {numeros}\n")
                            
                            if len(partes) == 5:  # Descripción, Docto No., Cargo, Abono, Saldo
                                cargo_str = partes[2].replace('.', '').replace(',', '.') if partes[2] else '0'
                                abono_str = partes[3].replace('.', '').replace(',', '.') if partes[3] else '0'
                                try:
                                    cargo = float(cargo_str)
                                    abono = float(abono_str)
                                except ValueError:
                                    debug_file.write(f"Error convirtiendo cargo/abono a float: {cargo_str}, {abono_str}\n")
                                    continue
                            elif len(partes) == 4:  # Descripción, Docto No., Amount, Saldo
                                monto_str = partes[2].replace('.', '').replace(',', '.') if partes[2] else '0'
                                try:
                                    monto_valor = float(monto_str)
                                    # In the cartola, CARGO and ABONO are mutually exclusive
                                    # Determine if the amount is in the CARGO or ABONO column
                                    pre_saldo = resto_linea.rsplit(saldo_str, 1)[0].strip()
                                    valores_pre_saldo = re.findall(r'\d{1,3}(?:\.\d{3})*(?:,\d+)?|\d+', pre_saldo)
                                    debug_file.write(f"Valores antes del saldo: {valores_pre_saldo}\n")
                                    
                                    if len(valores_pre_saldo) >= 2:  # Should have Docto No. and at least one amount
                                        if len(numeros) == 3:  # Docto No., Amount, Saldo
                                            # Check if the amount is in the CARGO or ABONO position
                                            # In the cartola, the CARGO column comes before ABONO
                                            # If there's only one amount before Saldo, we need to infer its position
                                            # Use the raw text to determine if there's a placeholder (e.g., 0) in CARGO or ABONO
                                            cargo_pos = resto_linea.find(valores_pre_saldo[1])
                                            abono_pos = cargo_pos + len(valores_pre_saldo[1])
                                            post_amount = resto_linea[abono_pos:].strip()
                                            next_numbers = re.findall(r'\d{1,3}(?:\.\d{3})*(?:,\d+)?|\d+', post_amount)
                                            debug_file.write(f"Números después del monto: {next_numbers}\n")
                                            
                                            if len(next_numbers) == 1 and next_numbers[0] == numeros[-1]:  # Only Saldo follows
                                                cargo = monto_valor  # Amount is in CARGO position
                                                abono = 0.0
                                            else:
                                                abono = monto_valor  # Amount is in ABONO position
                                                cargo = 0.0
                                        else:
                                            # Fallback: assume Cargo
                                            cargo = monto_valor
                                            abono = 0.0
                                    else:
                                        debug_file.write("No se encontraron suficientes valores numéricos antes del saldo\n")
                                        continue
                                except ValueError:
                                    debug_file.write(f"Error convirtiendo monto a float: {monto_str}\n")
                                    continue
                            else:
                                debug_file.write("Formato inesperado de línea, saltando...\n")
                                continue
                            
                            # Calculate monto: positive for abonos, negative for cargos
                            monto = abono if abono > 0 else -cargo if cargo > 0 else 0.0
                            
                            movimiento = {
                                "Fecha": fecha_completa,
                                "Descripción": descripcion,
                                "Documento": docto_no,
                                "Cargo": cargo,
                                "Abono": abono,
                                "Monto": monto,
                                "Saldo": saldo
                            }
                            
                            movimientos.append(movimiento)
                            debug_file.write(f"Movimiento extraído: {movimiento}\n")
        
        debug_file.write(f"\n=== RESUMEN ===\n")
        debug_file.write(f"Total movimientos encontrados: {len(movimientos)}\n")
    
    print(f"\nTexto extraído guardado en: {debug_txt_path}")
    print(f"Movimientos encontrados: {len(movimientos)}")
    
    return movimientos

def convertir_fecha_espanol(fecha_str):
    meses_espanol = {
        'ENE': 'Jan', 'FEB': 'Feb', 'MAR': 'Mar', 'ABR': 'Apr',
        'MAY': 'May', 'JUN': 'Jun', 'JUL': 'Jul', 'AGO': 'Aug',
        'SEP': 'Sep', 'OCT': 'Oct', 'NOV': 'Nov', 'DIC': 'Dec'
    }
    
    try:
        for esp, eng in meses_espanol.items():
            if esp in fecha_str:
                fecha_str = fecha_str.replace(esp, eng)
                break
        return pd.to_datetime(fecha_str, format="%d/%b/%Y", errors='coerce')
    except:
        return pd.NaT

@app.post("/procesar-cartola/")
async def procesar_cartola(
    archivo: UploadFile = File(...),
    clave: str = Form(...)
):
    with tempfile.NamedTemporaryFile(delete=False, suffix=".pdf") as tmp_pdf:
        shutil.copyfileobj(archivo.file, tmp_pdf)
        tmp_pdf_path = tmp_pdf.name

    unlocked_pdf_path = None
    
    try:
        unlocked_pdf_path = desbloquear_pdf(tmp_pdf_path, clave)
        
        movimientos = extraer_movimientos_desde_pdf(unlocked_pdf_path)

        if not movimientos:
            raise HTTPException(
                status_code=400, 
                detail="No se encontraron movimientos. Revisa el archivo debug_texto_cartola.txt para más detalles."
            )

        print(f"Movimientos extraídos: {len(movimientos)}")
        
        for i, mov in enumerate(movimientos[:3]):
            print(f"Movimiento {i+1}: {mov}")

        df = pd.DataFrame(movimientos)
        
        print(f"DataFrame creado con {len(df)} filas")
        print("Columnas disponibles:", df.columns.tolist())
        print("Primeras fechas antes de conversión:")
        print(df["Fecha"].head())
        
        df["Fecha"] = df["Fecha"].apply(convertir_fecha_espanol)
        
        print("Fechas después de conversión:")
        print(df["Fecha"].head())
        print(f"Fechas válidas: {df['Fecha'].notna().sum()}")
        
        df_valido = df.dropna(subset=["Fecha"])
        print(f"Filas después de filtrar fechas válidas: {len(df_valido)}")
        
        if len(df_valido) == 0:
            df_valido = df.copy()
            print("Manteniendo datos originales sin conversión de fecha")
        
        df_valido["Tipo"] = df_valido["Monto"].apply(lambda x: "Ingreso" if x > 0 else "Gasto" if x < 0 else "Sin movimiento")
        
        columnas = ["Fecha", "Descripción", "Monto", "Cargo", "Abono", "Saldo", "Documento", "Tipo"]
        columnas_existentes = [col for col in columnas if col in df_valido.columns]
        df_final = df_valido[columnas_existentes]
        
        if df_final["Fecha"].notna().any():
            df_final = df_final.sort_values("Fecha")

        output_excel = tempfile.mktemp(suffix=".xlsx")
        df_final.to_excel(output_excel, index=False)
        
        print(f"Excel generado con {len(df_final)} movimientos")
        print("Resumen:")
        print(f"- Ingresos: {len(df_final[df_final['Tipo'] == 'Ingreso'])}")
        print(f"- Gastos: {len(df_final[df_final['Tipo'] == 'Gasto'])}")
        print(f"- Sin movimiento: {len(df_final[df_final['Tipo'] == 'Sin movimiento'])}")

        return FileResponse(
            output_excel, 
            media_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
            filename="movimientos_cartola.xlsx"
        )

    finally:
        if os.path.exists(tmp_pdf_path):
            os.remove(tmp_pdf_path)
        if unlocked_pdf_path and os.path.exists(unlocked_pdf_path):
            os.remove(unlocked_pdf_path)