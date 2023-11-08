import re
import pandas as pd
from datetime import datetime

# Abre el archivo SQL en modo lectura con codificación UTF-8
with open('DDL_SI.sql', 'r', encoding='utf-8') as archivo:
    contenido = archivo.read()

# Divide el contenido en declaraciones usando punto y coma como separador
declaraciones = contenido.split(';')

# Expresiones regulares para extraer nombre de tabla y atributos
declaracion_tabla = r'CREATE TABLE\s+(.*?)\s*\('
declaracion_alter = r'ALTER TABLE\s+(.*?)\s'
declaracion_comment = r'COMMENT ON\s+(.*?)\s'

tablas_SI = []
alter_tables_SI = []
comment_tables_SI = []
errores_SI = []

for declaracion in declaraciones:
    nombre_tabla_match = re.search(declaracion_tabla, declaracion)

    if nombre_tabla_match:
        nombre_tabla = nombre_tabla_match.group(1)

        indice_parentesis = declaracion.find('(')
        bloque_atributos = (declaracion[indice_parentesis:])[2:-1]

        tabla_SI = [nombre_tabla, {"atributos": []}]

        bloque_atributos = bloque_atributos.splitlines()

        atributos = tabla_SI[1]["atributos"]

        for atributo in bloque_atributos:

            if atributo =="    ," or atributo=="":
                continue
            
            if "ERROR" in atributo:
                errores_SI.append({"Tabla": nombre_tabla, "Error": atributo})
                continue

            atributo = atributo.strip()
            atributo = re.sub(r'\s+', ' ', atributo)

            atributo = atributo.split(' ', 1)

            if atributo[1].endswith(','):
                atributo[1] = atributo[1][:-1]

            nuevo_atributo = {"nombre_atributo": atributo[0], "tipo_atributo": (atributo[1])}
            atributos.append(nuevo_atributo)

        tablas_SI.append(tabla_SI)

    alter_match = re.search(declaracion_alter, declaracion)

    if alter_match:
        nombre_tabla = declaracion.split(' ')[2]
        nombre_tabla = re.sub(r'\n', '', nombre_tabla)
        declaracion = re.sub(r'\n', ' ', declaracion)        
        declaracion = re.sub(r'(?<=[^\' ]) +| +(?=[^\' ])', ' ', declaracion) # sustituye grupos de espacios por uno solo, a no ser que estén entre comillas
        alter_tables_SI.append((nombre_tabla, declaracion.strip()))

    comment_match = re.search(declaracion_comment, declaracion)

    if comment_match:
        nombre_tabla = declaracion.split(' ')[3]
        if '.' in nombre_tabla:
            nombre_tabla = nombre_tabla.split('.')[0]
        nombre_tabla = re.sub(r'\n', '', nombre_tabla)
        declaracion = re.sub(r'\n', ' ', declaracion)        
        declaracion = re.sub(r'(?<=[^\' ]) +| +(?=[^\' ])', ' ', declaracion) # sustituye grupos de espacios por uno solo, a no ser que estén entre comillas
        comment_tables_SI.append((nombre_tabla, declaracion.strip()))   


with open('ddl_db_desa.sql', 'r') as archivo: #ddl_db_desa_PRUEBA.sql
    sql_script = archivo.read()

table_definition_pattern = r'CREATE TABLE[^;]*'
declaracion_alter = r'ALTER TABLE "OEVDSSII"\."([^"]+)"(.*?)"'
declaracion_comment = r'COMMENT ON\s+(.*?)\s'

declaraciones = re.split(';', sql_script)

tablas_DB_desa = []
alter_tables_DB_desa = []
comment_tables_DB_desa = []
errores_DB_desa = []

for declaracion in declaraciones:

    create_tabla_match = re.search(table_definition_pattern, declaracion)
    alter_tabla_match = re.search(declaracion_alter, declaracion)
    comment_tabla_match = re.search(declaracion_comment, declaracion)


    if create_tabla_match:
        table_name_match = re.search(r'CREATE TABLE "OEVDSSII"\."([^"]+)"', declaracion)        
        if table_name_match:
            table_name = table_name_match.group(1)
            attributes_match = re.search(rf'{table_name}[^;]*\) SEGMENT', declaracion)
            if attributes_match:
                attributes_text = attributes_match.group(0)
                attribute_lines = attributes_text.split('\n')
                attribute_list = []
                for line in attribute_lines:
                    attribute_match = re.search(r'"([^"]+)"\s+([^,]+),', line)
                    if attribute_match:
                        attribute_name = attribute_match.group(1)
                        attribute_type = attribute_match.group(2)
                        attribute_list.append({'nombre_atributo': attribute_name, 'tipo_atributo': attribute_type})
                tablas_DB_desa.append({'nombre_tabla': table_name, 'atributos': attribute_list})

    elif alter_tabla_match:
        alter_statement = declaracion.split('\n')
        filtered_lines = [line for line in alter_statement if not line.strip().startswith('-')]

        # Unir las líneas restantes en una sola línea
        result_string = ' '.join(filtered_lines)
        result_string = re.sub(r'\s{2,}', ' ', result_string)
        # Eliminar cualquier ocurrencia de "OEVDSSII".
        result_string = result_string.replace('"OEVDSSII".', '')

        match = re.search(r'"(.*?)"', result_string)
        if match:
            table_name = match.group(1)
        alter_tables_DB_desa.append((table_name, result_string))

    elif comment_tabla_match:
        comment_statement = declaracion.split('\n')
        filtered_lines = [line for line in comment_statement if not line.strip().startswith('-')]

        result_string = declaracion.replace('"OEVDSSII".', '')
        result_string = result_string.replace('\n', ' ')

        # Dividir la cadena en dos partes en la subcadena "IS"
        parts = result_string.split("IS", 1)
        # Eliminar todas las comillas de la primera subcadena
        first_part = parts[0].replace('"', '')
        # Volver a unir las partes
        result_string = first_part + "IS" + parts[1]
        # Sustituir cualquier cadena de más de un espacio por un solo espacio
        result_string = re.sub(r'\s{2,}', ' ', result_string)
                
        # Crear la variable table_name
        if result_string.startswith("COMMENT ON COLUMN"):
            table_name = result_string.split(' ', 3)[3].split('.')[0]
        elif result_string.startswith("COMMENT ON TABLE"):
            table_name = result_string.split(' ', 4)[3]

        comment_tables_DB_desa.append((table_name, result_string.strip()))     



# Crear un DataFrame con las tablas SI
df_tablas_SI = pd.DataFrame(tablas_SI, columns=["Table_Name", "Attributes"])

# Crear un DataFrame con las tablas DB_DESA
df_tablas_DB_DESA = pd.DataFrame(tablas_DB_desa, columns=["Table_Name", "Attributes"])

# Crear un DataFrame con los ALTER TABLEs SI
df_alter_tables_SI = pd.DataFrame(alter_tables_SI, columns=["Table_Name", "Alter_Statement"])

# Crear un DataFrame con los ALTER TABLEs DB_DESA
df_alter_tables_DB_DESA = pd.DataFrame(alter_tables_DB_desa, columns=["Table_Name", "Alter_Statement"])

# Crear un DataFrame con los comentarios SI
df_comment_tables_SI = pd.DataFrame(comment_tables_SI, columns=["Table_Name", "Comment_Statement"])

# Crear un DataFrame con los comentarios DB_DESA
df_comment_tables_DB_DESA = pd.DataFrame(comment_tables_DB_desa, columns=["Table_Name", "Comment_Statement"])

# Fecha actual
fecha_actual = datetime.now()
fecha_str = fecha_actual.strftime("%d%m%Y")

# Crear un archivo Excel
excel_writer = pd.ExcelWriter(f'INFORME_BBDD_SI-DB_DESA{fecha_str}.xlsx', engine='xlsxwriter')

# Hoja 1 - Comparación de Table_Names
df_tablas_SI.sort_values(by=["Table_Name"], inplace=True)
df_tablas_DB_DESA.sort_values(by=["Table_Name"], inplace=True)

df_tablas_SI["Table_Name"] = df_tablas_SI["Table_Name"].str.strip()
df_tablas_DB_DESA = df_tablas_DB_DESA[df_tablas_DB_DESA["Table_Name"].apply(lambda x: isinstance(x, str))]
df_tablas_DB_DESA["Table_Name"] = df_tablas_DB_DESA["Table_Name"].astype(str).str.strip()


df_tablas_SI.to_excel(excel_writer, sheet_name="Tabla_Names_SI", index=False)
df_tablas_DB_DESA.to_excel(excel_writer, sheet_name="Tabla_Names_DB_DESA", index=False)

# Hoja 2 - Comparación de Atributos
df_tablas_SI_attributes = df_tablas_SI.copy()
df_tablas_DB_DESA_attributes = df_tablas_DB_DESA.copy()

df_tablas_SI_attributes["Attributes"] = df_tablas_SI_attributes["Attributes"].apply(lambda x: len(x))
df_tablas_DB_DESA_attributes["Attributes"] = df_tablas_DB_DESA_attributes["Attributes"].apply(lambda x: len(x))

df_tablas_SI_attributes.to_excel(excel_writer, sheet_name="Attributes_SI", index=False)
df_tablas_DB_DESA_attributes.to_excel(excel_writer, sheet_name="Attributes_DB_DESA", index=False)

# Hoja 3 - Comparación de ALTER TABLEs
df_alter_tables_SI.sort_values(by=["Table_Name"], inplace=True)
df_alter_tables_DB_DESA.sort_values(by=["Table_Name"], inplace=True)

df_alter_tables_SI.to_excel(excel_writer, sheet_name="Alter_Tables_SI", index=False)
df_alter_tables_DB_DESA.to_excel(excel_writer, sheet_name="Alter_Tables_DB_DESA", index=False)

# Hoja 4 - Comparación de Comentarios
df_comment_tables_SI.sort_values(by=["Table_Name"], inplace=True)
df_comment_tables_DB_DESA.sort_values(by=["Table_Name"], inplace=True)

df_comment_tables_SI.to_excel(excel_writer, sheet_name="Comments_SI", index=False)
df_comment_tables_DB_DESA.to_excel(excel_writer, sheet_name="Comments_DB_DESA", index=False)

# Guardar el archivo Excel
excel_writer._save()

print("Proceso Finalizado")



