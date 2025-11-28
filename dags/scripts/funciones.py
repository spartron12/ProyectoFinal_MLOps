
import pandas as pd
import joblib
import numpy as np
from sklearn.model_selection import train_test_split
from sklearn.metrics import accuracy_score, mean_squared_error, r2_score
from airflow.providers.mysql.hooks.mysql import MySqlHook
import os
import requests
from datetime import datetime
import mlflow
from mlflow.tracking import MlflowClient
from sklearn.ensemble import GradientBoostingRegressor
from airflow.exceptions import AirflowSkipException 

MODEL_PATH = "/opt/airflow/models/GradientBoostingRegressor.pkl"
TABLE_NAME = "house_raw"
CONN_ID = "mysql_conn"


def check_api_has_data():
    """
    Verifica si la API tiene datos disponibles.
    Retorna True si hay datos, False si está agotada.
    
    Usado por ShortCircuitOperator para detener el DAG cuando no hay datos.
    """
    import requests
    
    url = "http://10.43.100.103:8000/data?group_number=7&day=Tuesday"
    
    try:
        print(" Verificando disponibilidad de datos en la API...")
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        data = response.json()
        
        # Verificar si la API dice que no hay más datos
        if isinstance(data, dict) and "detail" in data:
            print(f" API agotada: {data['detail']}")
            print(" Deteniendo DAG - No hay más datos para procesar")
            return False  
        
        # Si llegó data válida
        print(" API tiene datos disponibles - Continuando pipeline")
        return True  # ← Continúa el DAG
        
    except Exception as e:
        print(f" Error verificando API: {e}")
        print(" Asumiendo que hay datos (error de conexión)")
        return True  


def restart_api_data():
    """Reinicia los datos de la API antes de empezar"""
    import requests
    
    url = "http://10.43.100.103:8000/restart_data_generation?group_number=7&day=Tuesday"
    
    try:
        print(" Reiniciando datos de la API...")
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        print(f" API reiniciada: {response.json()}")
    except Exception as e:
        print(f" Error al reiniciar API: {e}")
        raise


def load_data():
    """Carga datos desde la API externa manejando el fin de datos gracefully"""
    url = "http://10.43.100.103:8000/data?group_number=7&day=Tuesday"
    
    try:
        # Hacemos la petición SIN raise_for_status todavía
        response = requests.get(url, timeout=30)
        
        # 1. MANEJO ESPECÍFICO DEL ERROR 400 (FIN DE DATOS)
        if response.status_code == 400:
            try:
                data = response.json()
                # Verificamos el mensaje exacto que manda tu API
                if isinstance(data, dict) and "detail" in data:
                    if "Ya se recolectó toda la información" in data["detail"]:
                        print(f" API finalizada: {data['detail']}")
                        print(" No hay más datos para procesar. Deteniendo flujo.")
                        # Retornamos DataFrame vacío
                        return pd.DataFrame()
            except ValueError:
                # Si falla el JSON, dejamos que pase al raise_for_status normal
                pass

        # 2. Si no es el error 400 esperado, verificamos errores generales
        response.raise_for_status()
        
        # 3. Proceso normal si es 200 OK
        data = response.json()

        if not data:
            print(" API retornó lista vacía")
            return pd.DataFrame()

        # Si viene como dict suelto, lo metemos en lista
        if isinstance(data, dict):
             data = [data]

        df = pd.DataFrame(data)

        if "data" not in df.columns:
            print(" Respuesta sin columna 'data'")
            return pd.DataFrame()

        df_expanded = df.explode("data").reset_index(drop=True)
        df_details = pd.json_normalize(df_expanded["data"])
        df_final = pd.concat([df_expanded.drop(columns=["data"]), df_details], axis=1)

        print(f" Cargados {len(df_final)} registros desde la API")
        return df_final
        
    except Exception as e:
        print(f" Error inesperado cargando datos: {e}")
        raise
    


def insert_data_temp_raw():
    """Inserta datos en house_temp_raw"""
    df = load_data()
    
    # ← SI NO HAY DATOS, SOLO RETORNAR (no hacer skip)
    if df.empty:
        print(" No hay datos nuevos para insertar en temp_raw (API agotada)")
        print(" DAG continuará con los datos existentes en las tablas")
        return  # ← Simplemente retornar, no lanzar excepción
    
    hook = MySqlHook(mysql_conn_id=CONN_ID)

    insert_sql = """
    INSERT INTO house_temp_raw 
    (group_number, day, batch_number, brokered_by, status,
     price, bed, bath, acre_lot, street,
     city, state, zip_code, house_size, prev_sold_date)
    VALUES (%s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s)
    """

    values = [
        (
            int(row["group_number"]) if not pd.isna(row["group_number"]) else None,
            str(row["day"]) if not pd.isna(row["day"]) else None,
            int(row["batch_number"]) if not pd.isna(row["batch_number"]) else None,
            float(row["brokered_by"]) if not pd.isna(row["brokered_by"]) else None,
            str(row["status"]) if not pd.isna(row["status"]) else None,
            float(row["price"]) if not pd.isna(row["price"]) else None,
            float(row["bed"]) if not pd.isna(row["bed"]) else None,
            float(row["bath"]) if not pd.isna(row["bath"]) else None,
            float(row["acre_lot"]) if not pd.isna(row["acre_lot"]) else None,
            float(row["street"]) if not pd.isna(row["street"]) else None,
            str(row["city"]) if not pd.isna(row["city"]) else None,
            str(row["state"]) if not pd.isna(row["state"]) else None,
            float(row["zip_code"]) if not pd.isna(row["zip_code"]) else None,
            float(row["house_size"]) if not pd.isna(row["house_size"]) else None,
            str(row["prev_sold_date"]) if not pd.isna(row["prev_sold_date"]) else None
        )
        for _, row in df.iterrows()
    ]

    conn = hook.get_conn()
    cursor = conn.cursor()
    cursor.executemany(insert_sql, values)
    conn.commit()
    cursor.close()
    conn.close()

    print(f" {len(values)} registros insertados en house_temp_raw")



def read_temp_raw_and_insert_raw():
    """Lee de temp_raw, cuenta e inserta en raw"""
    hook = MySqlHook(mysql_conn_id=CONN_ID)
    
    print(" Leyendo datos desde house_temp_raw...")
    df = hook.get_pandas_df("SELECT * FROM house_temp_raw")
    print(f" Total de registros en temp_raw: {len(df)}")
    
    # ← SI TEMP_RAW ESTÁ VACÍA, SOLO RETORNAR
    if df.empty:
        print(" temp_raw está vacía, no hay nada que insertar en raw")
        print(" DAG continuará con los datos existentes en raw")
        return  
    
    insert_sql = """
    INSERT INTO house_raw 
    (group_number, day, batch_number, brokered_by, status,
     price, bed, bath, acre_lot, street,
     city, state, zip_code, house_size, prev_sold_date)
    VALUES (%s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s)
    """
    
    values = [
        (
            int(row["group_number"]) if not pd.isna(row["group_number"]) else None,
            str(row["day"]) if not pd.isna(row["day"]) else None,
            int(row["batch_number"]) if not pd.isna(row["batch_number"]) else None,
            float(row["brokered_by"]) if not pd.isna(row["brokered_by"]) else None,
            str(row["status"]) if not pd.isna(row["status"]) else None,
            float(row["price"]) if not pd.isna(row["price"]) else None,
            float(row["bed"]) if not pd.isna(row["bed"]) else None,
            float(row["bath"]) if not pd.isna(row["bath"]) else None,
            float(row["acre_lot"]) if not pd.isna(row["acre_lot"]) else None,
            float(row["street"]) if not pd.isna(row["street"]) else None,
            str(row["city"]) if not pd.isna(row["city"]) else None,
            str(row["state"]) if not pd.isna(row["state"]) else None,
            float(row["zip_code"]) if not pd.isna(row["zip_code"]) else None,
            float(row["house_size"]) if not pd.isna(row["house_size"]) else None,
            str(row["prev_sold_date"]) if not pd.isna(row["prev_sold_date"]) else None
        )
        for _, row in df.iterrows()
    ]
    
    conn = hook.get_conn()
    cursor = conn.cursor()
    cursor.executemany(insert_sql, values)
    conn.commit()
    cursor.close()
    conn.close()
    
    print(f" {len(values)} registros insertados en house_raw")



def clean(df):
    # 1. Eliminar columnas que no se usarán
    df = df.drop(columns=[
        "group_number", "day", "batch_number", "brokered_by",
        "city", "zip_code", "status", "prev_sold_date", "street"
    ], errors="ignore")


    numeric_cols = ["bed", "bath", "acre_lot", "house_size"]
    for col in numeric_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    df[numeric_cols] = df[numeric_cols].fillna(0)

    df["acre_lot"] = df["acre_lot"].clip(0, 50)
    df["house_size"] = df["house_size"].clip(0, 10000)


    state_map = {
        "AL": "Alabama", "AK": "Alaska", "AZ": "Arizona", "AR": "Arkansas",
        "CA": "California", "CO": "Colorado", "CT": "Connecticut", "DE": "Delaware",
        "DC": "District_of_Columbia", "FL": "Florida", "GA": "Georgia", "HI": "Hawaii",
        "ID": "Idaho", "IL": "Illinois", "IN": "Indiana", "IA": "Iowa", "KS": "Kansas",
        "KY": "Kentucky", "LA": "Louisiana", "ME": "Maine", "MD": "Maryland",
        "MI": "Michigan", "MN": "Minnesota", "MS": "Mississippi", "MO": "Missouri",
        "MT": "Montana", "NE": "Nebraska", "NV": "Nevada", "NH": "New_Hampshire",
        "NJ": "New_Jersey", "NM": "New_Mexico", "NY": "New_York",
        "NC": "North_Carolina", "ND": "North_Dakota", "OH": "Ohio", "OK": "Oklahoma",
        "OR": "Oregon", "PA": "Pennsylvania", "RI": "Rhode_Island",
        "SC": "South_Carolina", "SD": "South_Dakota", "TN": "Tennessee",
        "TX": "Texas", "UT": "Utah", "VT": "Vermont", "VA": "Virginia",
        "WA": "Washington", "WV": "West_Virginia", "WI": "Wisconsin", "WY": "Wyoming"
    }

    df["state"] = df["state"].map(state_map)


    # 3. One-Hot Encoding SOLO para "state"
    df = pd.get_dummies(df, columns=["state"], dtype=int)

    # Estas columnas solo para análisis, NO para el modelo
    df["price_per_sqft"] = df["price"] / df["house_size"]
    df["lot_per_sqft"] = df["acre_lot"] / df["house_size"]


    return df



def transform_raw_to_clean():
    """Lee TODO raw, hace TRUNCATE clean, transforma e inserta"""
    hook = MySqlHook(mysql_conn_id=CONN_ID)
    
    conn = hook.get_conn()
    cursor = conn.cursor()
    
    try:
        #  TRUNCATE AL INICIO (crítico)
        print("=" * 60)
        print(" TRUNCANDO house_clean...")
        print("=" * 60)
        cursor.execute("TRUNCATE TABLE house_clean")
        conn.commit()
        print(" house_clean truncada exitosamente")
        
        # Leer datos de raw
        print("\n Leyendo TODOS los datos acumulados desde house_raw...")
        df = hook.get_pandas_df("SELECT * FROM house_raw")
        print(f" Total de registros acumulados en raw: {len(df)}")
        
        #  Validar si hay datos en raw
        if df.empty:
            print(" house_raw está vacía, no hay nada que transformar")
            print(" Tarea finalizada - esperando datos en próxima ejecución")
            return  # ← Simplemente retornar
        
        print("\n Aplicando transformaciones...")
        df_clean = clean(df)
        
        # Columnas esperadas en house_clean
        final_columns = [
            'price', 'bed', 'bath', 'acre_lot', 'house_size',
            'state_Alabama','state_Alaska','state_Arizona','state_Arkansas','state_California',
            'state_Colorado','state_Connecticut','state_Delaware','state_District_of_Columbia',
            'state_Florida','state_Georgia','state_Hawaii','state_Idaho','state_Illinois',
            'state_Indiana','state_Iowa','state_Kansas','state_Kentucky','state_Louisiana',
            'state_Maine','state_Maryland','state_Michigan','state_Minnesota','state_Mississippi',
            'state_Missouri','state_Montana','state_Nebraska','state_Nevada','state_New_Hampshire',
            'state_New_Jersey','state_New_Mexico','state_New_York','state_North_Carolina',
            'state_North_Dakota','state_Ohio','state_Oklahoma','state_Oregon','state_Pennsylvania',
            'state_Rhode_Island','state_South_Carolina','state_South_Dakota','state_Tennessee',
            'state_Texas','state_Utah','state_Vermont','state_Virginia','state_Washington',
            'state_West_Virginia','state_Wisconsin','state_Wyoming',
            'price_per_sqft', 'lot_per_sqft'
        ]
        
        # Asegurar todas las columnas
        for col in final_columns:
            if col not in df_clean.columns:
                df_clean[col] = 0
        
        df_clean = df_clean[final_columns]
        df_clean = df_clean.where(pd.notnull(df_clean), None)
        
        # Insertar datos
        cols_str = ", ".join(final_columns)
        placeholders = ", ".join(["%s"] * len(final_columns))
        
        insert_sql = f"""
            INSERT INTO house_clean ({cols_str})
            VALUES ({placeholders})
        """
        
        print(f"\n Insertando {len(df_clean)} registros en house_clean...")
        cursor.executemany(insert_sql, df_clean.values.tolist())
        conn.commit()
        
        print("=" * 60)
        print(f" {len(df_clean)} registros insertados exitosamente en house_clean")
        print("=" * 60)
        
    except Exception as e:
        print(f" Error en transform_raw_to_clean: {e}")
        conn.rollback()
        raise
    finally:
        cursor.close()
        conn.close()

    
 
def check_table_exists(**kwargs):
    from airflow.providers.mysql.hooks.mysql import MySqlHook
    hook = MySqlHook(mysql_conn_id=CONN_ID)
    query = "SHOW TABLES LIKE 'house_raw';"
    df = hook.get_pandas_df(query)
    if df.empty:
        return "delete_table_raw"
    else:
        return "truncate_temp_raw"



###==============================================================================
#  CONFIGURACIÓN GLOBAL
# ==============================================================================


# MLflow (en Docker Compose, puerto 8084 mapeado a localhost):
MLFLOW_TRACKING_URI = "http://mlflow_serv:5000"  # Nombre del servicio en docker-compose

EXPERIMENT_NAME = "house_price_prediction"
MODEL_NAME = "HousePriceModel"
CONN_ID = "mysql_conn"  # Configurado automáticamente por AIRFLOW_CONN_MYSQL_CONN
MODEL_PATH = "/opt/airflow/models/HousePriceModel.pkl"
API_URL = "http://api-servicio:5000"  # Ajusta si tienes una API


# ==============================================================================
#  FUNCIÓN DE CONFIGURACIÓN MLFLOW (LA QUE FALTABA)
# ==============================================================================
def configure_mlflow():
    """
    Configura las variables de entorno y tracking URI de MLflow.
     ESTA FUNCIÓN ES CRÍTICA - Sin ella, train_model() falla.
    """
    # MinIO (contenedor en Docker Compose)
    os.environ['MLFLOW_S3_ENDPOINT_URL'] = "http://minio:9000"
    os.environ['AWS_ACCESS_KEY_ID'] = 'admin'
    os.environ['AWS_SECRET_ACCESS_KEY'] = 'supersecret'
    
    # MLflow tracking server (contenedor en Docker Compose)
    mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)
    mlflow.set_experiment(EXPERIMENT_NAME)
    
    print(f" MLflow configurado:")
    print(f"   Tracking URI: {MLFLOW_TRACKING_URI}")
    print(f"   MinIO: {os.environ['MLFLOW_S3_ENDPOINT_URL']}")
    print(f"   Experimento: {EXPERIMENT_NAME}")

# ==============================================================================
#  FUNCIÓN DE ENTRENAMIENTO
# ==============================================================================

def train_model():
    """
    Entrena modelo GradientBoostingRegressor y lo REGISTRA en Model Registry.
    Lee datos desde house_clean.
    """
    print("=" * 80)
    print(" INICIANDO ENTRENAMIENTO DE MODELO")
    print("=" * 80)
    
    # Configurar MLflow
    configure_mlflow()
    
    # Conectar a MySQL
    hook = MySqlHook(mysql_conn_id=CONN_ID)
    
    # Cargar datos limpios
    print("\n CARGANDO DATOS DESDE house_clean...")
    query = "SELECT * FROM house_clean"
    df = hook.get_pandas_df(sql=query)
    
    if df.empty:
        print(" No hay datos para entrenar en house_clean")
        return
    
    print(f" Datos cargados: {df.shape[0]} registros, {df.shape[1]} columnas")
    
    # Verificar que 'price' existe
    if 'price' not in df.columns:
        print(f" ERROR: Columna 'price' no encontrada. Columnas disponibles: {df.columns.tolist()}")
        return
    
    df = df.drop(columns=[
            "price_per_sqft",
            "lot_per_sqft"
        ], errors="ignore")
            
    
    # Preparar datos
    y = df["price"]
    X = df.drop(["price"], axis=1)
    
    # Split train/test (80/20)
    from sklearn.model_selection import train_test_split
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=42
    )
    
    print(f"\n INFORMACIÓN DEL DATASET:")
    print(f"   Features: {X.shape[1]} columnas")
    print(f"   Target: price")
    print(f"   Train: {X_train.shape[0]} registros")
    print(f"   Test:  {X_test.shape[0]} registros")
    
    # Entrenar modelo
    print("\n ENTRENANDO GradientBoostingRegressor...")
    model = GradientBoostingRegressor(
        n_estimators=200,
        learning_rate=0.05,
        max_depth=5,
        random_state=42
    )
    model.fit(X_train, y_train)
    print(" Modelo entrenado exitosamente")
    
    # Evaluar modelo
    print("\n EVALUANDO MODELO...")
    y_pred = model.predict(X_test)
    
    mse = mean_squared_error(y_test, y_pred)
    rmse = float(np.sqrt(mse))
    r2 = float(r2_score(y_test, y_pred))
    mae = float(np.mean(np.abs(y_test - y_pred)))
    
    print(f"\n MÉTRICAS:")
    print(f"   MSE:  {mse:.4f}")
    print(f"   RMSE: {rmse:.4f}")
    print(f"   MAE:  {mae:.4f}")
    print(f"   R²:   {r2:.4f}")
    
    # ===========================================================================
    #  REGISTRAR EN MLFLOW
    # ===========================================================================
    
    # Obtener número de run
    experiment = mlflow.get_experiment_by_name(EXPERIMENT_NAME)
    experiment_id = experiment.experiment_id
    runs = mlflow.search_runs(experiment_ids=[experiment_id])
    run_number = len(runs) + 1
    run_name = f"GradientBoostingRegressor_{run_number}"
    
    print(f"\n Registrando run: {run_name}")
    
    with mlflow.start_run(run_name=run_name) as run:
        # Log parámetros
        mlflow.log_param("model_type", "GradientBoostingRegressor")
        mlflow.log_param("n_estimators", 200)
        mlflow.log_param("learning_rate", 0.05)
        mlflow.log_param("max_depth", 5)
        mlflow.log_param("random_state", 42)
        mlflow.log_param("test_size", 0.2)
        mlflow.log_param("n_features", X.shape[1])
        mlflow.log_param("n_samples", df.shape[0])
        
        # Log métricas
        mlflow.log_metric("mse", mse)
        mlflow.log_metric("rmse", rmse)
        mlflow.log_metric("mae", mae)
        mlflow.log_metric("r2_score", r2)
        
        # Log modelo
        mlflow.sklearn.log_model(
            model,
            artifact_path="model"
        )
        
        run_id = run.info.run_id
        print(f" Run registrado con ID: {run_id}")
        
        # ===========================================================================
        #  REGISTRAR EN MODEL REGISTRY
        # ===========================================================================
        model_uri = f"runs:/{run_id}/model"
        result = mlflow.register_model(
            model_uri=model_uri,
            name=MODEL_NAME
        )
        
        print(f"\n MODELO REGISTRADO:")
        print(f"   Nombre: {MODEL_NAME}")
        print(f"   Versión: {result.version}")
        print(f"    Ver en: {MLFLOW_TRACKING_URI}/#/models/{MODEL_NAME}")
    
    # Guardar localmente (opcional, para compatibilidad)
    try:
        os.makedirs('/opt/airflow/models', exist_ok=True)
        joblib.dump(model, MODEL_PATH)
        print(f"\n Modelo guardado localmente: {MODEL_PATH}")
    except Exception as e:
        print(f"\n No se pudo guardar localmente: {e}")
    
    print("\n" + "=" * 80)
    print(" ENTRENAMIENTO COMPLETADO")
    print("=" * 80)


# ==============================================================================
#  FUNCIÓN DE PROMOCIÓN
# ==============================================================================

def promote_best_model():
    """
    Encuentra el mejor modelo (menor RMSE) y lo promueve a Production.
    """
    print("=" * 80)
    print(" PROMOCIÓN A PRODUCTION")
    print("=" * 80)
    
    configure_mlflow()
    client = MlflowClient()
    
    try:
        # Buscar todas las versiones
        print(f"\n Buscando versiones de '{MODEL_NAME}'...")
        versions = client.search_model_versions(f"name='{MODEL_NAME}'")
        
        if not versions:
            print(f" No hay versiones de '{MODEL_NAME}' registradas")
            return
        
        print(f"✓ Encontradas {len(versions)} versiones")
        
        # Encontrar mejor versión (menor RMSE)
        print("\n Evaluando modelos...")
        best_version = None
        best_rmse = float('inf')
        best_r2 = 0
        
        for v in versions:
            run_data = client.get_run(v.run_id).data
            rmse = run_data.metrics.get("rmse", float('inf'))
            r2 = run_data.metrics.get("r2_score", 0)
            
            print(f"   Versión {v.version}: RMSE={rmse:.4f}, R²={r2:.4f}, Stage={v.current_stage}")
            
            if rmse < best_rmse:
                best_rmse = rmse
                best_r2 = r2
                best_version = v.version
        
        print(f"\n MEJOR MODELO:")
        print(f"   Versión: {best_version}")
        print(f"   RMSE: {best_rmse:.4f}")
        print(f"   R²: {best_r2:.4f}")
        
        # Promover a Production
        print(f"\n Promoviendo a Production...")
        client.transition_model_version_stage(
            name=MODEL_NAME,
            version=best_version,
            stage="Production",
            archive_existing_versions=True
        )
        
        # Actualizar descripción
        from datetime import datetime
        client.update_model_version(
            name=MODEL_NAME,
            version=best_version,
            description=f" Production ({datetime.now().strftime('%Y-%m-%d %H:%M')}) | RMSE: {best_rmse:.4f} | R²: {best_r2:.4f}"
        )
        
        print(f"\n MODELO PROMOVIDO:")
        print(f"   {MODEL_NAME} v{best_version} → Production")
        print(f"    Ver en: {MLFLOW_TRACKING_URI}/#/models/{MODEL_NAME}/versions/{best_version}")
        
    except Exception as e:
        print(f"\n ERROR: {e}")
        import traceback
        traceback.print_exc()
    
    print("\n" + "=" * 80)
    print(" PROMOCIÓN COMPLETADA")
    print("=" * 80)