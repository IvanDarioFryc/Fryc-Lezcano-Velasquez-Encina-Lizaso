from peewee import *
import datetime
# Conexión a la base de datos
sqlite_db = SqliteDatabase(
    r'C:\Users\Usuario\Desktop\TP Integrador POO\obras_urbanas.db',
    pragmas={'journal_mode': 'wal'}
)

# Clase base
class BaseModel(Model):
    class Meta:
        database = sqlite_db

# Modelos
class Etapa(BaseModel):
    Id_Etapa = IntegerField(primary_key=True)
    Desc_etapa = TextField()

    class Meta:
        table_name = 'Etapas'

class Entorno(BaseModel):
    Id_Entorno = IntegerField(primary_key=True)
    Desc_entorno = TextField()

    class Meta:
        table_name = 'Entornos'

class Tipo_obra(BaseModel):
    Id_Tipo_obra = IntegerField(primary_key=True)
    Desc_tipo = TextField()

    class Meta:
        table_name = 'Tipos_obras'

class Area_responsable(BaseModel):
    Id_Area_responsable = IntegerField(primary_key=True)
    Area = TextField()

    class Meta:
        table_name = 'Areas_Responsables'

class Comuna(BaseModel):
    Id_Comuna = IntegerField(primary_key=True)
    Comuna = TextField()

    class Meta:
        table_name = 'Comunas'

class Barrio(BaseModel):
    Id_barrio = IntegerField(primary_key=True)
    Barrio = TextField()
    Comuna = ForeignKeyField(Comuna, backref='barrios')

    class Meta:
        table_name = 'Barrios'

class Tipo_contratacion(BaseModel):
    Id_Tipo_contratacione = IntegerField(primary_key=True)
    Desc_contrataciones = TextField()

    class Meta:
        table_name = 'Tipos_contrataciones'

class Empresa_licitadora(BaseModel):
    Id_Empresa_licitadora = IntegerField(primary_key=True)
    Empresa = TextField()
    cuit_contratista = CharField(null=True)

    class Meta:
        table_name = 'Empresas_licitadoras'

class Financiamiento(BaseModel):
    Id_Financiamiento = IntegerField(primary_key=True)
    Desc_financiamiento= TextField(null=True)

    class Meta:
        table_name = 'Financiamientos'

class Obra(BaseModel):
    Id_Obra = AutoField()
    Entorno = ForeignKeyField(Entorno, backref='obras', null=True)
    Nombre_obra = TextField()
    Etapa = ForeignKeyField(Etapa, backref='obras', null=True)
    Tipo_obra = ForeignKeyField(Tipo_obra, backref='obras', null=True)
    Area_responsable = ForeignKeyField(Area_responsable, backref='obras', null=True)
    Descripcion = TextField(null=True)
    Monto_contrato = FloatField(null=True)
    Barrio = ForeignKeyField(Barrio, backref='obras', null=True)
    Direccion = TextField(null=True)
    Latitud = TextField(null=True)
    Longitud = TextField(null=True)
    Fecha_inicio = DateField(null=True)
    Fecha_fin_inicial = DateField(null=True)
    Financiamiento=ForeignKeyField(Financiamiento, backref='obras', null=True)
    Plazo = FloatField(null=True)
    Porcentaje_avance = FloatField(null=True)
    Empresa_licitadora = ForeignKeyField(Empresa_licitadora, backref='obras', null=True)
    año_licitacion = CharField(null=True)
    Tipo_contratacion = ForeignKeyField(Tipo_contratacion, backref='obras', null=True)
    Nro_contratacion = CharField(null=True)
    Mano_de_obra = IntegerField(null=True)
    Compromiso = TextField(null=True)
    Destacada = CharField(null=True)
    ba_elige = CharField(null=True)
    Expediente = TextField(null=True)
    
    def nuevo_proyecto(self):
        print("\n\U0001F4CC Etapa: Nuevo Proyecto")

        # Usamos tu lógica para validar o crear etapa "Proyecto"
        etapa = Etapa.get_or_none(Etapa.Desc_etapa == "Proyecto")
        if not etapa:
            etapa = Etapa.create(Desc_etapa="Proyecto")
            print("\u2705 Etapa 'Proyecto' creada y asignada.")
        else:
            print("\U0001F527 Etapa 'Proyecto' ya existente. Se asignará.")

        self.Etapa = etapa
        self.save()

    print("\U0001F680 Se creó el nuevo proyecto (Etapa: Proyecto)")

    def iniciar_contratacion(self):
        from gestionar_obras import pedir_y_validar_o_crear
        print("\n\U0001F4CC Iniciando contratación")

        tipo_contratacion = pedir_y_validar_o_crear(
            Tipo_contratacion,
            Tipo_contratacion.Desc_contrataciones,
            "Ingrese tipo de contratación: "
        )
        monto_contratacion = input("Ingrese monto de contratación: ").strip()
        nro_contratacion = input("Ingrese número de contratación: ").strip()

        
        self.Tipo_contratacion = tipo_contratacion
        self.Monto_contrato = monto_contratacion
        self.Nro_contratacion = nro_contratacion
        self.save()

        print("\u2705 Contratación registrada con éxito.")

    def adjudicar_obra(self):
        print("\n\U0001F4CC Adjudicando obra")

        # Mostrar empresas existentes, pedir que elijan una que ya exista (sin crear)
        while True:
            print("\nEmpresas licitadoras disponibles:")
            for empresa in Empresa_licitadora.select():
                print(f"- {empresa.Empresa}")

            empresa_nombre = input("Ingrese empresa licitadora (de la lista): ").strip()
            empresa = Empresa_licitadora.get_or_none(Empresa_licitadora.Empresa == empresa_nombre)

            if empresa:
                break
            else:
                print("\u274C Empresa no encontrada. Por favor, ingrese una empresa válida.")

        expediente = input("Ingrese número de expediente: ").strip()

        self.Empresa_licitadora = empresa
        self.Expediente = expediente
        self.save()

        print("\u2705 Obra adjudicada correctamente.")
    
    def iniciar_obra(self):
        from gestionar_obras import (pedir_y_validar_o_crear, pedir_int, pedir_fecha, pedir_si_no)
        print("\n\U0001F4CC Iniciando obra")

        destacada = pedir_si_no("¿Es destacada?")
        fecha_inicio = pedir_fecha("Ingrese fecha de inicio (AAAA-MM-DD): ")
        fecha_fin_inicial = pedir_fecha("Ingrese fecha de finalización estimada (AAAA-MM-DD): ")
        financiamiento = pedir_y_validar_o_crear(
            Financiamiento,
            Financiamiento.Desc_financiamiento,
            "Ingrese financiamiento: "
        )
        mano_obra = pedir_int("Ingrese cantidad de personas en obra: ")

        self.Destacada = destacada
        self.Fecha_inicio = fecha_inicio
        self.Fecha_fin_inicial = fecha_fin_inicial
        self.Financiamiento = financiamiento
        self.Mano_de_obra = mano_obra
        self.save()

        print("\u2705 Obra iniciada correctamente.")

    def actualizar_porcentaje_avance(self):
        from gestionar_obras import pedir_float
        print("\n\U0001F4CC Actualizando porcentaje de avance")

        porcentaje = pedir_float("Ingrese porcentaje de avance (ej: 42.5): ")

        self.Porcentaje_avance = porcentaje
        self.save()

        print(f"\u2705 Porcentaje de avance actualizado a {porcentaje}%.")

    def incrementar_plazo(self):
        from gestionar_obras import pedir_int
        from gestionar_obras import pedir_float
        print("\n\U0001F4CC Incrementando plazo de la obra")

        nuevo_plazo = pedir_float("Ingrese plazo (en días): ")

        self.Plazo = nuevo_plazo
        self.save()

        print(f"\u2705 Plazo actualizado a {nuevo_plazo} días.") 

    def incrementar_mano_obra(self):
        from gestionar_obras import pedir_int
        print("\n\U0001F4CC Actualizando mano de obra")

        nueva_mano_obra = pedir_int("Ingrese nueva cantidad de mano de obra: ")

        self.Mano_de_obra = nueva_mano_obra
        self.save()

        print(f"\u2705 Mano de obra actualizada a {nueva_mano_obra}personas.") 

    def finalizar_obra(self):
        print("\n\U0001F4CC Finalizando obra")

        # Buscar la etapa "Finalizada" (si no existe, crearla)
        etapa_finalizada, creada = Etapa.get_or_create(Desc_etapa="Finalizada")

        # Actualizar la etapa de la obra
        self.Etapa = etapa_finalizada
        self.save()

    def rescindir_obra(self):
        print("\n\U0001F4CC Rescindiendo obra")

        # Buscar la etapa "Rescindida", y si no existe, crearla
        etapa_rescindida, creada = Etapa.get_or_create(Desc_etapa="Rescisión")

        # Actualizar la etapa de la obra
        self.Etapa = etapa_rescindida
        self.save()

        print("\u2705 Obra marcada como rescindida.")       


    class Meta:
        table_name = 'Obras'
