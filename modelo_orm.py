from sqlalchemy import create_engine, Column, Integer, String
from sqlalchemy.orm import declarative_base, sessionmaker

# Crear base y motor de conexión
Base = declarative_base()
engine = create_engine('sqlite:///obras_urbanas.db', echo=True)  # echo=True muestra los logs de SQL

# Sesión para interactuar con la base
Session = sessionmaker(bind=engine)
session = Session()

# Modelo ORM de la tabla Personas
class Persona(Base):
    __tablename__ = 'Personas'

    id = Column(Integer, primary_key=True, autoincrement=True)
    nombre = Column(String, nullable=False)
    edad = Column(Integer, nullable=False)

    def es_mayor(self):
        return "Es mayor" if self.edad > 18 else "Es menor"

    def __repr__(self):
        return f"<Persona(nombre='{self.nombre}', edad={self.edad})>"

# Crear la tabla (si no existe)
Base.metadata.create_all(engine)

# Crear instancias
p1 = Persona(nombre="Ivan", edad=27)
p2 = Persona(nombre="Lucas", edad=29)

# Agregar personas a la sesión
session.add_all([p1, p2])
session.commit()

# Consultar personas mayores de edad
personas_mayores = session.query(Persona).filter(Persona.edad > 18).all()

# Mostrar resultados
print("Personas mayores de edad:")
for persona in personas_mayores:
    print(persona)

# Verificación del método es_mayor()
print(f"{p1.nombre}: {p1.es_mayor()}")
print(f"{p2.nombre}: {p2.es_mayor()}")

# Cerrar la sesión
session.close()