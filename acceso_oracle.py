import oracledb
import getpass
from datetime import datetime, timedelta

# Solicitamos los datos de conexión al servidor Oracle (IP, instancia, puerto, usuario, contraseña, puerto, modo)
servidor = input("Introduzca el servidor Oracle (IP o nombre DNS) (Ejemplo: localhost): ")
if servidor == "":
    servidor = "localhost"
instancia = input("Introduzca el nombre de la instancia de la BD Oracle (por ejemplo ORCLCDB): ")
if instancia == "":
    instancia = "ORCLCDB"
puerto = input("Puerto de conexión a Oracle (por defecto 1521): ")
if puerto == "":
    puerto = 1521
usuario = input("Introduzca el usuario: ")
contrasena = getpass.getpass("Introduzca la contraseña: ")
modoSYSDBA = input("Conectar en modo SYSDBA (s/n): ")
if modoSYSDBA == "":
    modoSYSDBA = "n"

try:
    # Si se ha elegido el modo de conexión SYSDBA
    if modoSYSDBA.upper == "S":
        modo = oracledb.SYSDBA
    else:
        modo = None
    # Establecemos los parámetros de conexión con el servidor Oracle
    parametrosConexion = oracledb.ConnectParams(
        host=servidor, port=puerto, service_name=instancia, mode=modo
    )
    
    try:
        # Realizamos la conexión
        conexionOra = oracledb.connect(
            user=usuario, password=contrasena, params=parametrosConexion
        )        
        print(f"Conectado a [{servidor}], versión [{conexionOra.version}]...""")
        
        try:        
            cursor = conexionOra.cursor()
        
            # SELECCIÓN DE REGISTROS (SELECT)
            print("Ejecutando consulta SQL de tabla ALBARANES...")
            sql = """select CODIGO, CODIGO_CLIENTE, IMPORTE, TO_CHAR(FECHA, 'DD-MM-YYYY') FECHA
                from ALBARANES
                order by codigo desc
                fetch next 10 rows only"""
            cursor.execute(sql)
            registros = cursor.fetchall()
            print(f"Cosulta SQL de select ejecutada. Número de registros seleccionados: {cursor.rowcount}")
            # Mostramos los nombres de los campos en el encabezado            
            print(f"{"CÓDIGO":8} {"CÓD. CLIENTE":17} {"IMPORTE":16} FECHA")
            for codigo, codigo_cliente, importe, fecha in registros:
                print(f"{codigo:6} {codigo_cliente:14} {importe:12} {"":3} {fecha}")
                
            # SELECCIÓN DE REGISTROS (SELECT) CON FILTRO WHIERE
            print("Ejecutando consulta SQL de tabla ALBARANES con filtro where...")
            cursor = conexionOra.cursor()
            sql = """select CODIGO, CODIGO_CLIENTE, IMPORTE, TO_CHAR(FECHA, 'DD-MM-YYYY') FECHA
                from ALBARANES
                where importe >= :importeFiltro
                order by importe desc"""
            cursor.execute(sql, importeFiltro=1600)
            registros = cursor.fetchall()
            print(f"Cosulta SQL de select con where ejecutada. Número de registros seleccionados: {cursor.rowcount}")
            # Mostramos los nombres de los campos en el encabezado            
            print(f"{"CÓDIGO":8} {"CÓD. CLIENTE":17} {"IMPORTE":16} FECHA")
            for codigo, codigo_cliente, importe, fecha in registros:
                print(f"{codigo:6} {codigo_cliente:14} {importe:12} {"":3} {fecha}")                
            
            # INSERCIÓN DE REGISTROS (INSERT)
            # Obtenemos dos fechas para insertar dos registros
            # La fecha actual y la fecha actual más 10 días                        
            fechaActual = datetime.now()
            fechaMas5Dias = fechaActual + timedelta(days=10)
            print("Ejecutando consulta SQL de inserción INSERT...")
            registrosInsertar = [ (15, 1500.45, fechaActual),
                                 (109, 1000.09, fechaMas5Dias)]
            cursor.executemany("""insert into ALBARANES (
                CODIGO_CLIENTE, IMPORTE, FECHA) 
                values (
                    :1, :2, TO_DATE(:3, 'YYYY-MM-DD HH24:MI:SS'))""", registrosInsertar)
            # Guardamos cambios con un commit
            conexionOra.commit()
            print(f"Cosulta SQL de insert ejecutada. Número de registros insertados: {cursor.rowcount}")
            
            # ACTUALIZACIÓN DE REGISTROS (UPDATE)
            # Como ejemplo, actualizamos el valor del campo importe para los registros cuyo
            # CODIGO_CLIENTE sea igual a 50, sumándole 100 a su importe actual
            print("Ejecutando consulta SQL de actualización UPDATE...")
            resultado = cursor.execute("""update ALBARANES 
                set IMPORTE = IMPORTE + 100
                where CODIGO_CLIENTE = 50""")
            # Guardamos cambios con un commit
            conexionOra.commit()
            print(f"Cosulta SQL de update ejecutada. Número de registros actualizados: {cursor.rowcount}")
            
            # ELIMINACIÓN DE REGISTROS (DELETE)
            # Como ejemplo, eliminamos los registros cuyo CODIGO_CLIENTE sea igual a 10
            print("Ejecutando consulta SQL de eliminación DELETE...")
            cursor.execute("""delete from ALBARANES 
                where CODIGO_CLIENTE = 10""")
            # Guardamos cambios con un commit
            conexionOra.commit()
            print(f"Cosulta SQL de delete ejecutada. Número de registros eliminados: {cursor.rowcount}")
            
            # Cerramos la conexión con el servidor
            print("Cerrando conexión...")
            cursor.close
            print("Conexión cerrada")            
        except Exception as e:
            # Si se produce un error al ejecutar la consulta SQL
            print(f"Se ha producido un error al ejecutar la consulta SQL: {e}")    
    except Exception as e:
        # Si se produce un error al conectar con el servidor de Oracle
        print(f"Se ha producido un error al conectar al servidor de Oracle: {e}")        
except Exception as e:
    # Error al establecer parámetros de conexión
    print(f"No se han establecido los parámetros de conexión correctos: {e}")