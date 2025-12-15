# Script para borrar todos los documentos de una colección de forma eficiente en Firebase Firestore.

import firebase_admin
from firebase_admin import credentials, firestore

# --- CONFIGURACIÓN ---
# La colección que se desea eliminar (debe coincidir con la que está en uso).
COLLECTION_TO_DELETE = "demographics_cruice"
CREDENTIALS_FILE = "datalake-7a937-firebase-adminsdk-fbsvc-a5778267ab.json"
PROJECT_ID = "datalake-7a937"

# Límite de documentos a procesar por llamada. Usamos 500 que es el límite máximo
# de documentos que se pueden obtener por vez con .stream().
BATCH_SIZE = 500

# Inicializa Firebase
try:
    cred = credentials.Certificate(CREDENTIALS_FILE)
    firebase_admin.initialize_app(cred, {
        "projectId": PROJECT_ID,
        "storageBucket": f"{PROJECT_ID}.firebasestorage.app"
    })
    db = firestore.client()
    print("✅ Conexión a Firebase Firestore establecida.")
except Exception as e:
    print(f"❌ Error al inicializar Firebase: {e}")
    db = None

def delete_collection(db, collection_path, batch_size):
    """
    Elimina todos los documentos de una colección dada en lotes.

    Firestore no tiene una función de "eliminar colección" nativa en el SDK,
    por lo que debemos obtener los documentos en lotes y eliminarlos uno por uno.
    """
    if not db:
        print("❌ Operación de borrado abortada debido a error de conexión.")
        return

    ref = db.collection(collection_path)
    print(f"\n🗑️ Iniciando borrado masivo en la colección '{collection_path}'...")
    
    deleted_count = 0
    
    # Bucle para asegurar que se procesan todos los documentos
    while True:
        # Obtener una "página" de documentos (máximo 500)
        docs = ref.limit(batch_size).stream()
        
        # Obtener los IDs de los documentos
        deleted_list = list(docs)
        if not deleted_list:
            break

        # Iniciar una operación de lote
        batch = db.batch()
        
        # Marcar todos los documentos del lote para eliminación
        for doc in deleted_list:
            batch.delete(doc.reference)

        # Ejecutar el lote
        batch.commit()
        deleted_count += len(deleted_list)
        print(f"    ➡️ Eliminados {deleted_count} documentos hasta ahora...")

    print(f"✅ Borrado completado para '{collection_path}'. Total de documentos eliminados: {deleted_count}")


if __name__ == "__main__":
    if db:
        # Llama a la función de borrado masivo
        delete_collection(db, COLLECTION_TO_DELETE, BATCH_SIZE)
    else:
        print("No se pudo proceder con el borrado.")