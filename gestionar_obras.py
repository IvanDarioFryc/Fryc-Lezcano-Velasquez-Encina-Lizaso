from abc import ABC, abstractmethod
import pandas as pd
from peewee import fn
import datetime
from modelo_orm import (
    sqlite_db, Entorno, Etapa, Empresa_licitadora, Tipo_obra,
    Area_responsable, Comuna, Barrio, Tipo_contratacion, Financiamiento, Obra
)


# Funciones auxiliares para pedir datos por teclado y validar
def pedir_y_validar_o_crear(modelo, campo, mensaje_input, campos_extra=None):
    while True:
        print(f"\nOpciones disponibles ({modelo.__name__}):")
        for item in modelo.select():
            print(f"- {getattr(item, campo.name)}")

        valor = input(mensaje_input).strip()
        obj = modelo.get_or_none(campo == valor)

        if obj:
            return obj
        else:
            crear = input(f"'{valor}' no existe. ¿Deseás crearlo? (SI/NO): ").strip().lower()
            if crear in ['si', 'sí']:
                datos = {campo.name: valor}
                if campos_extra:
                    datos.update(campos_extra())
                nuevo = modelo.create(**datos)
                print(f"\u2714 Nuevo {modelo.__name__} creado: {valor}")
                return nuevo
            else:
                print("\u274C No se creó el registro. Intente nuevamente.")


def pedir_float(mensaje):
    while True:
        try:
            return float(input(mensaje))
        except ValueError:
            print("\u274C Ingrese un número decimal válido (use '.' para decimales).")


def pedir_int(mensaje):
    while True:
        try:
            return int(input(mensaje))
        except ValueError:
            print("\u274C Ingrese un número entero válido.")


def pedir_fecha(mensaje):
    import datetime
    while True:
        try:
            return datetime.datetime.strptime(input(mensaje), "%Y-%m-%d").date()
        except ValueError:
            print("\u274C Formato inválido. Use AAAA-MM-DD.")


def pedir_si_no(mensaje):
    while True:
        valor = input(mensaje + " (SI/NO): ").strip().lower()
        if valor in ['si', 'sí']:
            return "SI"
        elif valor == 'no':
            return "NO"
        else:
            print("\u274C Respuesta inválida. Ingrese 'SI' o 'NO'.")


# Definición de clases abstractas y concretas para la gestión
class GestionarObra(ABC):

    @classmethod
    @abstractmethod
    def conectar_db(cls):
        pass

    @classmethod
    @abstractmethod
    def mapear_orm(cls):
        pass

    @classmethod
    @abstractmethod
    def extraer_datos(cls, ruta_csv):
        pass

    @classmethod
    @abstractmethod
    def Obtener_indicadores(cls):
        pass

    @classmethod
    @abstractmethod
    def cargar_datos(cls, df):
        pass

    @classmethod
    @abstractmethod
    def nueva_obra(cls):
        pass


class ControlObra(GestionarObra):

    @classmethod
    def conectar_db(cls):
        sqlite_db.connect()
        print("\U0001F4E1 Conexión establecida con la base de datos.")

    @classmethod
    def mapear_orm(cls):
        sqlite_db.create_tables([
            Entorno, Etapa, Empresa_licitadora, Tipo_obra, Area_responsable,
            Comuna, Barrio, Tipo_contratacion, Financiamiento, Obra
        ])
        print("Tablas creadas correctamente \U0001F44D")

    @classmethod
    def extraer_datos(cls, ruta_csv):
        try:
            print(f"\U0001F4BB  Intentando cargar CSV desde: {ruta_csv}")
            df = pd.read_csv(ruta_csv, sep=';', encoding='latin1')

            columnas_utiles = [
                'entorno', 'nombre', 'etapa', 'tipo', 'area_responsable', 'descripcion',
                'monto_contrato', 'comuna', 'barrio', 'direccion', 'lat', 'lng',
                'fecha_inicio', 'fecha_fin_inicial', 'plazo_meses', 'porcentaje_avance',
                'licitacion_oferta_empresa', 'licitacion_anio', 'contratacion_tipo',
                'nro_contratacion', 'cuit_contratista', 'beneficiarios', 'mano_obra',
                'compromiso', 'destacada', 'ba_elige', 'expediente-numero', 'financiamiento'
            ]

            print(f"\U0001F4CA Columnas en el CSV: {df.columns.tolist()}")

            df = df[columnas_utiles]
            df = df.where(pd.notna(df), None)
            df['ba_elige'] = df['ba_elige'].apply(lambda x: None if pd.isna(x) else x)

            print("\u2714 DataFrame cargado con éxito.")
            return df

        except Exception as e:
            print(f"\u274C Error al cargar el DataFrame: {e}")
            return None

    @classmethod
    def Obtener_indicadores(cls):
        indicadores = {}

        indicadores['areas_responsables'] = list(Area_responsable.select())
        indicadores['tipos_obra'] = list(Tipo_obra.select())
        indicadores['obras_por_etapa'] = list(
            Obra
            .select(Etapa.Desc_etapa, Obra.Etapa, fn.COUNT(Obra.Id_Obra).alias('cantidad_obras'))
            .join(Etapa)
            .group_by(Obra.Etapa, Etapa.Desc_etapa)
        )
        indicadores['obras_y_monto_por_tipo'] = list(
            Obra
            .select(
                Tipo_obra.Desc_tipo,
                Obra.Tipo_obra,
                fn.COUNT(Obra.Id_Obra).alias('cantidad_obras'),
                fn.SUM(Obra.Monto_contrato).alias('monto_por_tipo')
            )
            .join(Tipo_obra)
            .group_by(Obra.Tipo_obra, Tipo_obra.Desc_tipo)
        )
        indicadores['barrios_comunas_123'] = list(
            Barrio.select().where(Barrio.Comuna.in_([1, 2, 3]))
        )
        indicadores['obras_finalizadas_en_24_meses'] = (
            Obra.select().where((Obra.Etapa == 3) & (Obra.Plazo <= 24)).count()
        )
        indicadores['monto_total_inversion'] = (
            Obra.select(fn.SUM(Obra.Monto_contrato)).scalar() or 0
        )

        print("\U0001F4CA Indicadores generados correctamente.")
        return indicadores

    @classmethod
    def cargar_datos(cls, df):
        if df is None:
            print("\u26A0 No se puede cargar datos: el DataFrame está vacío.")
            return
        def convertir_a_int(valor):
            try:
                return int(valor)
            except (ValueError, TypeError):
                return 0
        def convertir_a_float(valor):
            try:
                if isinstance(valor, str):
                    valor = valor.replace(',', '.')
                return float(valor)
            except (ValueError, TypeError):
                return 0.0
        def parse_monto(monto_str):
            import pandas as pd
            if pd.isna(monto_str):
                return 0.0
            monto_str = str(monto_str).replace('$', '').replace(' ', '').replace('.', '').replace(',', '.')
            try:
                return float(monto_str)
            except ValueError:
                return 0.0
        def fecha_o_none(valor):
            fecha = pd.to_datetime(valor, errors='coerce')
            return fecha.date() if pd.notna(fecha) else None
        
        total_insertadas = 0

        for _, fila in df.iterrows():
            try:
                entorno, _ = Entorno.get_or_create(Desc_entorno=fila['entorno'])
                etapa, _ = Etapa.get_or_create(Desc_etapa=fila['etapa'])
                valor_tipo_obra = fila['tipo'] or "No especificado"
                tipo_obra, _ = Tipo_obra.get_or_create(Desc_tipo=valor_tipo_obra)
                #tipo_obra, _ = Tipo_obra.get_or_create(Desc_tipo=fila['tipo'])
                area_resp, _ = Area_responsable.get_or_create(Area=fila['area_responsable'])
                valor_tipo_contratacion = fila['contratacion_tipo'] or "No especificado"
                #tipo_contratacion, _ = Tipo_contratacion.get_or_create(Desc_contrataciones=fila['contratacion_tipo'])
                tipo_contratacion, _ = Tipo_contratacion.get_or_create(Desc_contrataciones=valor_tipo_contratacion)
                comuna, _ = Comuna.get_or_create(Comuna=fila['comuna'])
                barrio, _ = Barrio.get_or_create(Barrio=fila['barrio'], Comuna=comuna)
                valor_financiamiento = fila['financiamiento'] if fila['financiamiento'] else "No especificado"
                financiamiento, _ = Financiamiento.get_or_create(Desc_financiamiento=valor_financiamiento)
                nombre_empresa = fila['licitacion_oferta_empresa'] if fila['licitacion_oferta_empresa'] else "Empresa Desconocida"

                nombre_empresa = fila['licitacion_oferta_empresa'] if fila['licitacion_oferta_empresa'] else "Empresa Desconocida"
                cuit = fila['cuit_contratista'] if fila['cuit_contratista'] else "00-00000000-0"

                empresa, _ = Empresa_licitadora.get_or_create(
                    Empresa=nombre_empresa,
                    defaults={'cuit_contratista': cuit}
                )
                

                Obra.create(
                    Entorno=entorno,
                    Nombre_obra=fila['nombre'],
                    Etapa=etapa,
                    Tipo_obra=tipo_obra,
                    Area_responsable=area_resp,
                    Descripcion=fila['descripcion'],
                    Monto_contrato=parse_monto(fila['monto_contrato']),
                    Barrio=barrio,
                    Direccion=fila['direccion'],
                    Latitud=str(fila['lat']),
                    Longitud=str(fila['lng']),
                    #Fecha_inicio=pd.to_datetime(fila['fecha_inicio'], errors='coerce', dayfirst=True),
                    #Fecha_fin_inicial=pd.to_datetime(fila['fecha_fin_inicial'], errors='coerce', dayfirst=True),
                    Fecha_inicio=fecha_o_none(fila['fecha_inicio']),
                    Fecha_fin_inicial=fecha_o_none(fila['fecha_fin_inicial']),
                    #Plazo=convertir_a_float(fila['plazo_meses']) if fila['plazo_meses'] else 0,
                    Plazo=parse_monto(fila['plazo_meses']),
                    Porcentaje_avance=float(fila['porcentaje_avance']) if fila['porcentaje_avance'] else 0.0,
                    Empresa_licitadora=empresa,
                    año_licitacion=str(fila['licitacion_anio']),
                    Tipo_contratacion=tipo_contratacion,
                    Nro_contratacion=fila['nro_contratacion'],
                    Mano_de_obra=convertir_a_int(fila['mano_obra']) if fila['mano_obra'] else 0,
                    Compromiso=fila['compromiso'],
                    Destacada=fila['destacada'],
                    ba_elige=fila['ba_elige'],
                    Expediente=fila['expediente-numero'],
                    Financiamiento=financiamiento
                )

                total_insertadas += 1

            except Exception as e:
                print(f"\u274C Error al insertar una obra: {e}")

        print(f"\u2714 Se cargaron correctamente {total_insertadas} obras.")

    @classmethod
    def nueva_obra(cls):
        print("\n\U0001F6E0 Creación de nueva obra (datos básicos)")

        entorno = pedir_y_validar_o_crear(Entorno, Entorno.Desc_entorno, "Ingrese entorno de la obra: ")
        nombre_obra = input("Ingrese nombre de la obra: ")
        tipo_obra = pedir_y_validar_o_crear(Tipo_obra, Tipo_obra.Desc_tipo, "Ingrese el tipo de obra: ")
        area = pedir_y_validar_o_crear(Area_responsable, Area_responsable.Area, "Ingrese el área responsable: ")
        descripcion = input("Ingrese descripción de la obra: ")

        def crear_barrio_con_comuna():
            comuna = pedir_y_validar_o_crear(Comuna, Comuna.Comuna, "Ingrese la comuna del nuevo barrio: ")
            return {"Comuna": comuna}

        barrio = pedir_y_validar_o_crear(Barrio, Barrio.Barrio, "Ingrese barrio de la obra: ", campos_extra=crear_barrio_con_comuna)
        direccion = input("Ingrese dirección donde se desarrolla la obra: ")
        latitud = input("Ingrese latitud (ej: -34.6037): ")
        longitud = input("Ingrese longitud (ej: -58.3816): ")
        año_licitacion = input("Ingrese año en que se licitó la obra: ")
        compromiso = pedir_si_no("¿Tiene compromiso?")
        ba_elige = pedir_si_no("¿Forma parte de BA Elige?")

        # Crear la obra con datos básicos
        obra = Obra.create(
            Entorno=entorno,
            Nombre_obra=nombre_obra,
            Tipo_obra=tipo_obra,
            Area_responsable=area,
            Descripcion=descripcion,
            Barrio=barrio,
            Direccion=direccion,
            Latitud=latitud,
            Longitud=longitud,
            año_licitacion=año_licitacion,
            Compromiso=compromiso,
            ba_elige=ba_elige
        )

        print("\n\u2714 Obra creada parcialmente. Ahora se completarán los datos con cada método:")

        input("🔹 Enter para crear el nuevo proyecto...")
        obra.nuevo_proyecto()

        input("🔹 Enter para iniciar contratación...")
        obra.iniciar_contratacion()

        input("🔹 Enter para adjudicar obra...")
        obra.adjudicar_obra()

        input("🔹 Enter para iniciar obra...")
        obra.iniciar_obra()

        input("🔹 Enter para actualizar porcentaje...")
        obra.actualizar_porcentaje_avance()

        input("🔹 Enter para incrementar plazo...")
        obra.incrementar_plazo()

        input("🔹 Enter para incrementar mano de obra...")
        obra.incrementar_mano_obra()

        input("🔹 Enter para finalizar la obra...")
        obra.finalizar_obra()

        input("🔹 Enter para rescindir la obra...")
        obra.rescindir_obra()

    @classmethod
    def main(cls):
        cls.conectar_db()
        cls.mapear_orm()
        ruta_csv = r"C:\Users\Usuario\Desktop\TP Integrador POO\observatorio_de_obras_urbanas.csv"
        df = cls.extraer_datos(ruta_csv)

        if df is None:
            print("U0001F621 No se pudo cargar el DataFrame.")
            return

        print(df.head())

        cls.cargar_datos(df)

        print()
        print("Creamos la primer instancia de obra:")
        cls.nueva_obra()
        print()
        print("Creamos la segunda instancia de obra:")
        cls.nueva_obra()
        print()
        print()
        indicadores = ControlObra.Obtener_indicadores()

        print("\n--- INDICADORES ---")

        # Mostrar áreas responsables
        print("\n\U0001F4C8 Áreas responsables:")
        for area in indicadores['areas_responsables']:
            print(f"- {area.Id_Area_responsable} | {area.Area}")

        # Mostrar tipos de obra
        print("\n\U0001F4C8 Tipos de obra:")
        for tipo in indicadores['tipos_obra']:
            print(f"- {tipo.Id_Tipo_obra} | {tipo.Desc_tipo}")

        # Obras por etapa
        print("\n\U0001F4C8 Obras por etapa:")
        for fila in indicadores['obras_por_etapa']:
            print(f"- Etapa: {fila.Etapa.Desc_etapa} | Cantidad: {fila.cantidad_obras}")

        # Obras y monto por tipo
        print("\n\U0001F4C8 Obras y monto por tipo:")
        for fila in indicadores['obras_y_monto_por_tipo']:
            print(f"- Tipo: {fila.Tipo_obra.Desc_tipo} | Obras: {fila.cantidad_obras} | Monto total: ${fila.monto_por_tipo:,.2f}")

        # Barrios de comunas 1, 2 y 3
        print("\n\U0001F4C8 Barrios en comunas 1, 2 y 3:")
        for barrio in indicadores['barrios_comunas_123']:
            print(f"- {barrio.Barrio} (Comuna {barrio.Comuna.Id_Comuna})")

        # Obras finalizadas en 24 meses
        print(f"\n\U0001F4C8 Obras finalizadas en 24 meses: {indicadores['obras_finalizadas_en_24_meses']}")

        # Monto total de inversión
        print(f"\n\U0001F4C8 Monto total de inversión: ${indicadores['monto_total_inversion']:,.2f}")


if __name__ == "__main__":
    ControlObra.main()
