from abc import ABC
from abc import ABCMeta
from peewee import *

sqlite_db= SqliteDatabase('obras_urbanas.db', pragmas={'journal_mode':'wal'})



class BaseModel(Model):
    class Meta:
        database=sqlite_db


# Modelo ORM de la tabla Obras

class Entorno(BaseModel):
    Id_Entorno=IntegerField(primary_key=True)
    Desc_entorno=TextField()



class Tipo_obra(BaseModel):
    Id_Tipo_obra=IntegerField(primary_key=True)
    Desc_tipo=TextField()


class Area_responsable(BaseModel):
    Id_Area_responsable=IntegerField(primary_key=True)
    Area=TextField()

class Comuna(BaseModel):
    Id_Comuna=IntegerField(primary_key=True)
    Comuna=TextField()
    

class Barrio(BaseModel):
    Id_barrio=IntegerField(primary_key=True)
    Barrio=TextField()
    Comuna=ForeignKeyField(Comuna, backref='Barrio') 


class Tipo_contratacion(BaseModel):
    Id_Tipo_contratacione=IntegerField(primary_key=True)
    Desc_contrataciones=TextField()

class Empresa_licitadora(BaseModel):
    Id_Empresa_licitadora=IntegerField(primary_key=True)
    Empresa=TextField()
    cuit_contratista=TextField()
    

    
class Obra(BaseModel):
    #__tablename__ = 'Personas'

    #Declaracion de atributos

    Id_Obra=IntegerField(primary_key=True)
    Nombre_obra=TextField() 
    Etapa=TextField() 
    Tipo_obra=ForeignKeyField(Tipo_obra, backref='Obra') 
    Area_responsable=ForeignKeyField(Area_responsable, backref='Obra') 
    Descripcion=TextField()
    Monto_contrato=FloatField()
    Barrio=ForeignKeyField(Barrio, backref='Obra')
    Direccion=TextField()
    Fecha_inicio=DateField()
    Fecha_fin=DateField()
    Plazo=IntegerField()
    Porcentaje_avance=FloatField()
    año_licitacion=CharField()
    Tipo_contratacion=ForeignKeyField(Tipo_contratacion, backref='Obra') 
    Empresa_licitadora = ForeignKeyField(Empresa_licitadora, backref='obras')
    Nro_contratacion=CharField()
    Cuit_contratista=TextField()
    Mano_de_obra=IntegerField()
    Compromiso=TextField()
    Destacada=CharField()
    ba_elige=CharField()
    Expedeiente=TextField()

class Detalle_Obra_Empresa(BaseModel):
    Id_detalle_Obra_Empresa=IntegerField(primary_key=True)
    Obra=ForeignKeyField(Obra, backref='Detalle_Obra_Empresa') 
    Empresa=ForeignKeyField(Empresa_licitadora, backref='Detalle_Obra_Empresa') 


sqlite_db.connect()

Entorno1= Entorno.create(Id_Entorno=1,Desc_entorno='Entorno 1')

sqlite_db.close()
   

 