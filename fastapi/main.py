# app_with_metrics.py
from fastapi import FastAPI, HTTPException, Request
from pydantic import BaseModel
import pandas as pd
import logging
import os
import mlflow
import time
import asyncio

# métricas Prometheus
from prometheus_client import Counter, Histogram, generate_latest, CONTENT_TYPE_LATEST
from fastapi.responses import Response

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(
    title="Predicción de reingreso hospitalario en pacientes diabéticos",
    description="API para predecir si un paciente diabético será readmitido",
    version="3.1.1"
)

# Definición de métricas
REQUEST_COUNT = Counter(
    'fastapi_request_count', 'Número total de requests recibidas', ['endpoint', 'method', 'http_status']
)
REQUEST_LATENCY = Histogram(
    'fastapi_request_latency_seconds', 'Histograma del tiempo de latencia de requests', ['endpoint']
)

# Variables globales modelo
model = None
model_loaded_at = None

# … aquí va tu clase PatientFeatures y demás código … (igual al tuyo) …
class PatientFeatures(BaseModel):
    discharge_disposition_id: int
    admission_source_id: int
    time_in_hospital: int
    num_lab_procedures: int
    num_procedures: int
    num_medications: int
    number_outpatient: int
    number_emergency: int
    number_inpatient: int
    number_diagnoses: int
    max_glu_serum: int
    A1Cresult: int

    race_AfricanAmerican: int
    race_Asian: int
    race_Caucasian: int
    race_Hispanic: int
    race_Other: int

    gender_Female: int
    gender_Male: int

    age_0_10: int
    age_10_20: int
    age_20_30: int
    age_30_40: int
    age_40_50: int
    age_50_60: int
    age_60_70: int
    age_70_80: int
    age_80_90: int
    age_90_100: int

    cambio_Ch: int
    cambio_No: int
    diabetesMed_No: int
    diabetesMed_Yes: int

    diag_1_Circulatory: int
    diag_1_Diabetes: int
    diag_1_Digestive: int
    diag_1_Genitourinary: int
    diag_1_Injury: int
    diag_1_Musculoskeletal: int
    diag_1_Neoplasms: int
    diag_1_Other: int
    diag_1_Respiratory: int

    diag_2_Circulatory: int
    diag_2_Diabetes: int
    diag_2_Digestive: int
    diag_2_Genitourinary: int
    diag_2_Injury: int
    diag_2_Musculoskeletal: int
    diag_2_Neoplasms: int
    diag_2_Other: int
    diag_2_Respiratory: int

    diag_3_Circulatory: int
    diag_3_Diabetes: int
    diag_3_Digestive: int
    diag_3_Genitourinary: int
    diag_3_Injury: int
    diag_3_Musculoskeletal: int
    diag_3_Neoplasms: int
    diag_3_Other: int
    diag_3_Respiratory: int

# funciones de carga de modelo, recarga automática igual al tuyo …
def get_model_uri():
    MODEL_NAME = "GradientBoostingModel"
    MODEL_STAGE = "Production"
    return f"models:/{MODEL_NAME}/{MODEL_STAGE}"

def load_model():
    global model, model_loaded_at
    try:
        logger.info("Cargando modelo desde MLflow...")
        os.environ["MLFLOW_S3_ENDPOINT_URL"] = os.getenv("MLFLOW_S3_ENDPOINT_URL", "http://minio:9000")
        os.environ["AWS_ACCESS_KEY_ID"] = os.getenv("AWS_ACCESS_KEY_ID", "admin")
        os.environ["AWS_SECRET_ACCESS_KEY"] = os.getenv("AWS_SECRET_ACCESS_KEY", "supersecret")
        mlflow.set_tracking_uri(os.getenv("MLFLOW_TRACKING_URI", "http://10.43.100.98:8084"))

        model_uri = get_model_uri()
        model = mlflow.pyfunc.load_model(model_uri)
        model_loaded_at = time.time()
        logger.info(f"Modelo cargado correctamente: {type(model).__name__}")

    except Exception as e:
        logger.error(f"No se pudo cargar el modelo: {e}")
        model = None
        model_loaded_at = None
        raise e

async def auto_reload_model(interval_sec=600):
    global model, model_loaded_at
    while True:
        try:
            load_model()
            logger.info(f"Modelo recargado automáticamente a las {time.ctime(model_loaded_at)}")
        except Exception as e:
            logger.error(f"Error recargando modelo automáticamente: {e}")
        await asyncio.sleep(interval_sec)

@app.on_event("startup")
async def startup_event():
    load_model()
    asyncio.create_task(auto_reload_model(interval_sec=600))

# Endpoint /metrics para Prometheus
@app.get("/metrics")
def metrics():
    # Genera y retorna todas las métricas registradas
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)

# Endpoint predict con métricas
@app.post("/predict")
def predict(features: PatientFeatures, request: Request):
    endpoint = "/predict"
    method = request.method
    start_time = time.time()

    if model is None:
        REQUEST_COUNT.labels(endpoint=endpoint, method=method, http_status="503").inc()
        raise HTTPException(status_code=503, detail="Modelo no disponible")

    try:
        feature_df = pd.DataFrame([features.dict()])
        feature_df = feature_df.astype(int)

        prediction = model.predict(feature_df)[0]

        REQUEST_COUNT.labels(endpoint=endpoint, method=method, http_status="200").inc()
        return {
            "prediction": prediction,
            "message": "Predicción completada con éxito",
            "model_loaded_at": model_loaded_at
        }

    except Exception as e:
        REQUEST_COUNT.labels(endpoint=endpoint, method=method, http_status="400").inc()
        raise HTTPException(status_code=400, detail=str(e))

    finally:
        latency = time.time() - start_time
        REQUEST_LATENCY.labels(endpoint=endpoint).observe(latency)

@app.post("/reload-model")
def reload_model():
    endpoint = "/reload-model"
    method = "POST"
    start_time = time.time()

    try:
        load_model()
        REQUEST_COUNT.labels(endpoint=endpoint, method=method, http_status="200").inc()
        return {"status": "success", "model_loaded_at": model_loaded_at}
    except Exception as e:
        REQUEST_COUNT.labels(endpoint=endpoint, method=method, http_status="500").inc()
        raise HTTPException(status_code=500, detail=f"Error recargando modelo: {str(e)}")
    finally:
        REQUEST_LATENCY.labels(endpoint=endpoint).observe(time.time() - start_time)

@app.get("/health")
def health():
    endpoint = "/health"
    method = "GET"
    start_time = time.time()

    REQUEST_COUNT.labels(endpoint=endpoint, method=method, http_status="200").inc()
    REQUEST_LATENCY.labels(endpoint=endpoint).observe(time.time() - start_time)
    return {
        "status": "healthy",
        "model_loaded": model is not None,
        "model_loaded_at": model_loaded_at
    }

@app.get("/model-info")
def model_info():
    endpoint = "/model-info"
    method = "GET"
    start_time = time.time()

    if model is None:
        REQUEST_COUNT.labels(endpoint=endpoint, method=method, http_status="503").inc()
        raise HTTPException(status_code=503, detail="Modelo no disponible")

    REQUEST_COUNT.labels(endpoint=endpoint, method=method, http_status="200").inc()
    REQUEST_LATENCY.labels(endpoint=endpoint).observe(time.time() - start_time)
    return {
        "model_type": str(type(model).__name__),
        "model_loaded": True,
        "model_in_memory": True,
        "description": "Modelo de predicción de reingreso hospitalario",
        "model_loaded_at": model_loaded_at
    }