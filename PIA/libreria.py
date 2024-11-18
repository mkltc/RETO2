import pymongo
import json
from datetime import datetime, timezone
import pandas as pd

# Función para conectar la Base de Datos.
def conectarBd():
    """
    Establece una conexión con una base de datos MongoDB utilizando pymongo.

    Proceso:
    - Recupera la URI de conexión desde una variable de entorno o la URI hardcoded.
    - Intenta conectar al servidor MongoDB utilizando la URI proporcionada.
    - Si la conexión es exitosa, se retorna el objeto `MongoClient` que permitirá realizar operaciones con MongoDB.
    - Si ocurre un error de conexión, la excepción `ConnectionFailure` es capturada y se informa al usuario con un mensaje de error.

    Retorno:
    - **Objeto MongoClient**: Si la conexión es exitosa, se retorna el objeto que permite interactuar con MongoDB.
    - **None**: Si ocurre un error de conexión, se retorna `None` y se muestra un mensaje de error.

    Definiciones:
    - **MongoClient**: Clase de PyMongo que establece una conexión con un servidor o clúster MongoDB utilizando una URI, permitiendo realizar operaciones en bases de datos y colecciones.

    """
    try:
        myclient = pymongo.MongoClient("mongodb+srv://ajaureguia24:ajaureguia24@cluster0.0rnpw.mongodb.net/")
        print("Conexión exitosa a MongoDB")
        return myclient
    except pymongo.errors.ConnectionFailure:
        print("Error de conexión a MongoDB")
        return None

# Función para crear la Base de Datos.   
def crearBd(bd):
    """
    Crea una base de datos en MongoDB.
    
    :param bd: Nombre de la base de datos a crear.

    Proceso:
    - Llama a la función `conectarBd()` para establecer una conexión con MongoDB.
    - Si la conexión es exitosa, intenta acceder a la base de datos especificada por el parámetro `bd`.
      MongoDB crea automáticamente la base de datos cuando se realiza la primera operación sobre ella.
    - Si la base de datos se crea con éxito, se retorna el objeto de la base de datos.
    - Si ocurre un error en la conexión o al intentar acceder a la base de datos, se captura el error 
      y se muestra un mensaje de error. En este caso, la función retorna `None`.

    Retorno:
    - **Objeto de la base de datos**: Si la base de datos se crea correctamente, se retorna el objeto de la base de datos.
    - **None**: Si hay un error en la conexión o en la creación de la base de datos, se retorna `None`.

    """
    myclient = conectarBd()
    if myclient:
        try:
            mydb = myclient[bd]
            print(f"Base de datos '{bd}' creada.")
            return mydb
        except pymongo.errors.ConnectionFailure:
            print("Error al crear la base de datos en MongoDB")
            return None
    else:
        print("No se pudo conectar a MongoDB para crear la base de datos")
        return None
    

# Función para eliminar la Base de Datos.
def eliminarBd(bd):
    """
    Elimina una base de datos en MongoDB.
    
    :param bd: Nombre de la base de datos a eliminar.
    :return: None

    Proceso:
    - Llama a la función `conectarBd()` para establecer una conexión con MongoDB.
    - Si la conexión es exitosa, utiliza el método `drop_database(bd)` para eliminar la base de datos especificada.
    - Si la eliminación se realiza con éxito, se imprime un mensaje confirmando la eliminación.
    - Si ocurre un error de conexión o un fallo en la eliminación de la base de datos, se captura el error 
      y se muestra un mensaje correspondiente.

    Retorno:
    - No retorna ningún valor, solo imprime un mensaje de confirmación o error.
    """
    myclient = conectarBd()
    if myclient:
        try:
            myclient.drop_database(bd)
            print(f"Base de datos '{bd}' eliminada exitosamente.")
        except pymongo.errors.ConnectionFailure:
            print("Error de conexión a MongoDB al intentar eliminar la base de datos.")
    else:
        print("No se pudo conectar a MongoDB para eliminar la base de datos.")



# Crear una clase para manejar las funciones: insertar, buscar, actualizar, eliminar y cargarJSON.
class OperacionesBdCrud:
    def __init__(self, db, collecion):
        """
        Inicializa la conexión a la base de datos y colección específicas.
        
        :param db: Nombre de la base de datos.
        :param collecion: Nombre de la colección.
        
        Proceso:
        - Llama a la función `conectarBd()` para establecer la conexión con MongoDB.
        - Si la conexión es exitosa, inicializa los atributos `db` y `collection` con la base de datos y colección especificadas.
        - Si la conexión falla, asigna `None` a los atributos `db` y `collection`.

        Retorno:
        - No retorna ningún valor. Solo inicializa la conexión a la base de datos y la colección.
        """
        client = conectarBd()
        if client:
            self.db = client[db]
            self.collection = self.db[collecion]
        else:
            self.db = None
            self.collection = None

    def insertar(self, documento):
        """
        Inserta un documento en la colección.
        
        :param documento: Diccionario que representa el documento a insertar.
        
        Proceso:
        - Intenta insertar el documento en la colección.
        - Si la inserción es exitosa, retorna el ID del documento insertado.
        - Si ocurre un error durante la inserción, captura la excepción y retorna `None`.

        Retorno:
        - **ID del documento insertado**: Si la inserción es exitosa.
        - **None**: Si ocurre un error durante la inserción.

        """
        try:
            resultado = self.collection.insert_one(documento)
            print("Documento insertado con ID:", resultado.inserted_id)
            return resultado.inserted_id
        except Exception as e:
            print("Error al insertar el documento:", e)
            return None

    def buscar(self, query):
        """
        Busca documentos en la colección que coincidan con la consulta dada.
        
        :param query: Diccionario que representa la consulta de búsqueda.
        
        Proceso:
        - Intenta buscar documentos que coincidan con la consulta proporcionada.
        - Si la búsqueda es exitosa, retorna una lista de documentos encontrados.
        - Si ocurre un error durante la búsqueda, captura la excepción y retorna `None`.

        Retorno:
        - **Lista de documentos**: Si la búsqueda es exitosa, retorna los documentos encontrados.
        - **None**: Si ocurre un error durante la búsqueda.

        """
        try:
            resultados = list(self.collection.find(query))
            print("Documentos encontrados:", resultados)
            return resultados
        except Exception as e:
            print("Error al buscar documentos:", e)
            return None

    def actualizar(self, query, nuevos_valores):
        """
        Actualiza documentos en la colección que coincidan con la consulta.
        
        :param query: Diccionario que representa la consulta para seleccionar documentos.
        :param nuevos_valores: Diccionario con los nuevos valores a actualizar.
        
        Proceso:
        - Intenta actualizar los documentos que coincidan con la consulta.
        - Si la actualización es exitosa, retorna el número de documentos modificados.
        - Si ocurre un error durante la actualización, captura la excepción y retorna `None`.

        Retorno:
        - **Número de documentos actualizados**: Si la actualización es exitosa, retorna la cantidad de documentos actualizados.
        - **None**: Si ocurre un error durante la actualización.

        """
        try:
            resultado = self.collection.update_many(query, {"$set": nuevos_valores})
            print("Documentos actualizados:", resultado.modified_count)
            return resultado.modified_count
        except Exception as e:
            print("Error al actualizar documentos:", e)
            return None

    def eliminar(self, query):
        """
        Elimina documentos en la colección que coincidan con la consulta.
        
        :param query: Diccionario que representa la consulta para seleccionar documentos.
        
        Proceso:
        - Intenta eliminar los documentos que coincidan con la consulta.
        - Si la eliminación es exitosa, retorna el número de documentos eliminados.
        - Si ocurre un error durante la eliminación, captura la excepción y retorna `None`.

        Retorno:
        - **Número de documentos eliminados**: Si la eliminación es exitosa, retorna la cantidad de documentos eliminados.
        - **None**: Si ocurre un error durante la eliminación.

        """
        try:
            resultado = self.collection.delete_many(query)
            print("Documentos eliminados:", resultado.deleted_count)
            return resultado.deleted_count
        except Exception as e:
            print("Error al eliminar documentos:", e)
            return None
        
    
    def cargarDatosJSON(self, ruta_archivo):
        """
        Carga un archivo JSON, realiza el parsing y lo inserta en MongoDB.
    
        :param ruta_archivo: Ruta del archivo JSON con los datos de reseñas.
        
        Proceso:
        - Intenta abrir y leer el archivo JSON especificado por `ruta_archivo`.
        - Si el archivo se carga correctamente, parsea su contenido.
        - Para cada reseña en los datos, se procesan y limpian ciertos campos (como fechas y texto) y se formatean para ser insertados en MongoDB.
        - Después, se insertan las reseñas procesadas en la colección de MongoDB.
        - Si ocurre un error durante la carga del archivo o el proceso de inserción, se captura la excepción y se retorna `None`.

        Retorno:
        - **Número de documentos insertados**: Si los datos se cargan e insertan correctamente, retorna el número de documentos insertados.
        - **None**: Si ocurre un error durante la carga o inserción de los datos.
        
        Excepciones manejadas:
        - **FileNotFoundError**: Si el archivo JSON no se encuentra en la ruta especificada.
        - **json.JSONDecodeError**: Si el archivo JSON no puede ser parseado correctamente.
        - **Exception**: Cualquier otro error no especificado que ocurra durante el proceso.

        Definiciones:
        - **`datetime.fromtimestamp`**: Función utilizada para convertir una marca de tiempo de Unix a una fecha legible.
        - **`insert_many`**: Método de PyMongo utilizado para insertar múltiples documentos en una colección de MongoDB.
        """
        try:
            with open(ruta_archivo, 'r') as file:
                datos = json.load(file)
                
            # Parsing y limpieza de datos
            reseñas_procesadas = []
            for reseña in datos:
                # Conversión del tiempo de Unix a fecha con zona horaria explícita
                fecha_review = datetime.fromtimestamp(reseña.get("unixReviewTime", 0), tz=timezone.utc)
                
                reseña_procesada = {
                    "reviewerID": reseña.get("reviewerID", "").strip(),
                    "asin": reseña.get("asin", None),
                    "reviewerName": reseña.get("reviewerName", "").strip(),
                    "helpful": {
                        "helpfulVotes": reseña.get("helpful", [0, 0])[0],
                        "totalVotes": reseña.get("helpful", [0, 0])[1]
                    },
                    "reviewText": reseña.get("reviewText", "").strip(),
                    "overall": int(reseña.get("overall", 0)),
                    "summary": reseña.get("summary", "").strip(),
                    "reviewTime": fecha_review,
                    "instrumentType": reseña.get("instrumentType", "").strip()
                }
                
                reseñas_procesadas.append(reseña_procesada)

            # Insertar en MongoDB
            if self.collection is not None:
                resultado = self.collection.insert_many(reseñas_procesadas)
                print(f"{len(resultado.inserted_ids)} reseñas insertadas en MongoDB.")
                return len(resultado.inserted_ids)
            else:
                print("No se pudo acceder a la colección en MongoDB.")
                return None
        except FileNotFoundError:
            print("Archivo JSON no encontrado.")
            return None
        except json.JSONDecodeError:
            print("Error al parsear el archivo JSON.")
            return None
        except Exception as e:
            print("Error al cargar datos en MongoDB:", e)
            return None



# Función para extraer los Datos de MongoDB a un Data Frame.
def extraerDatosMongoDB_a_dataframe(uri, bd, coleccion, batch_size=1000):
    """
    Extrae datos de MongoDB a un DataFrame de pandas de manera eficiente usando lotes.
    
    :param uri: URI de conexión a MongoDB.
    :param bd: Nombre de la base de datos.
    :param coleccion: Nombre de la colección.
    :param batch_size: Cantidad de documentos por lote.
    
    Proceso:
    - Se establece una conexión con MongoDB utilizando la URI proporcionada.
    - Se accede a la base de datos y la colección especificada.
    - Se crea un cursor para obtener los documentos por lotes de acuerdo al tamaño especificado (`batch_size`).
    - Se recorren los documentos del cursor y se procesan para convertir los valores del campo `_id` en un string y añadir cada documento a una lista.
    - Después de iterar sobre todos los documentos, se convierte la lista de datos en un DataFrame de pandas.
    - Se realiza una validación para asegurarse de que las columnas `reviewerID`, `overall` y `reviewTime` tengan los tipos correctos, y se manejan valores nulos si es necesario.
    
    Retorno:
    - Un **DataFrame de pandas** con los documentos extraídos de MongoDB, con validación y tipos de datos corregidos.

    Excepciones manejadas:
    - Si hay errores de conexión o en el proceso de conversión, se manejarán dentro del proceso de validación y se imprimirá una advertencia.
    
    Definiciones:
    - **batch_size**: Cantidad de documentos a procesar por lote para optimizar el rendimiento al extraer grandes volúmenes de datos.
    - **pymongo.MongoClient**: Clase utilizada para conectar con MongoDB.
    - **find()**: Método para obtener los documentos de la colección.
    - **pd.DataFrame()**: Método de pandas para convertir una lista de diccionarios en un DataFrame.
    
    """
    # Conexión a MongoDB
    client = pymongo.MongoClient(uri)
    db = client[bd]
    coleccion = db[coleccion]
    
    # Crea un cursor para obtener los documentos por lotes
    cursor = coleccion.find().batch_size(batch_size)
    
    # Lista para almacenar los datos
    datos = []
    
    # Iterar sobre los documentos en el cursor
    for documento in cursor:
        # Asegurarse de convertir el ObjectId a string
        documento["_id"] = str(documento["_id"])
        
        # Agregar el documento procesado a la lista de datos
        datos.append(documento)
        
    
    # Crear un DataFrame de pandas con los datos
    df = pd.DataFrame(datos)
    
    # Validación de los datos (ejemplo)
    # Verificar tipos de datos esperados
    df['reviewerID'] = df['reviewerID'].astype(str)  # Asegurar que reviewerID sea string
    df['overall'] = pd.to_numeric(df['overall'], errors='coerce')  # Convertir overall a numérico
    df['reviewTime'] = pd.to_datetime(df['reviewTime'], errors='coerce')  # Convertir a fecha
    
    # Validación adicional: Verificar valores nulos en campos importantes
    if df['reviewerID'].isnull().any():
        print("Advertencia: Hay valores nulos en 'reviewerID'.")
    
    # Retornar el DataFrame
    return df


