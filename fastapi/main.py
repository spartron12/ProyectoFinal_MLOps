# app.py  -----------------------------------------------------------
from fastapi import FastAPI, HTTPException, Request
from pydantic import BaseModel
import pandas as pd
import logging
import os
import mlflow
import time
import asyncio

# Prometheus
from prometheus_client import Counter, Histogram, generate_latest, CONTENT_TYPE_LATEST
from fastapi.responses import Response

# -----------------------------------------------------------
# Logging
# -----------------------------------------------------------
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# -----------------------------------------------------------
# FastAPI App
# -----------------------------------------------------------
app = FastAPI(
    title="Predicción de precios de casas",
    description="API para predecir el precio de una casa usando un modelo en MLflow",
    version="1.0.0"
)

# -----------------------------------------------------------
# Métricas Prometheus
# -----------------------------------------------------------
REQUEST_COUNT = Counter(
    'fastapi_request_count',
    'Número total de requests recibidas',
    ['endpoint', 'method', 'http_status']
)

REQUEST_LATENCY = Histogram(
    'fastapi_request_latency_seconds',
    'Histograma del tiempo de latencia',
    ['endpoint']
)

# -----------------------------------------------------------
# Modelo MLflow (global)
# -----------------------------------------------------------
model = None
model_loaded_at = None

# -----------------------------------------------------------
# Pydantic – Features de entrada (sin price)
# -----------------------------------------------------------
class HouseFeatures(BaseModel):
    bed: float | None = None
    bath: float | None = None
    acre_lot: float | None = None
    house_size: float | None = None

    state_Alabama: int
    state_Alaska: int
    state_Arizona: int
    state_Arkansas: int
    state_California: int
    state_Colorado: int
    state_Connecticut: int
    state_Delaware: int
    state_District_of_Columbia: int
    state_Florida: int
    state_Georgia: int
    state_Hawaii: int
    state_Idaho: int
    state_Illinois: int
    state_Indiana: int
    state_Iowa: int
    state_Kansas: int
    state_Kentucky: int
    state_Louisiana: int
    state_Maine: int
    state_Maryland: int
    state_Michigan: int
    state_Minnesota: int
    state_Mississippi: int
    state_Missouri: int
    state_Montana: int
    state_Nebraska: int
    state_Nevada: int
    state_New_Hampshire: int
    state_New_Jersey: int
    state_New_Mexico: int
    state_New_York: int
    state_North_Carolina: int
    state_North_Dakota: int
    state_Ohio: int
    state_Oklahoma: int
    state_Oregon: int
    state_Pennsylvania: int
    state_Rhode_Island: int
    state_South_Carolina: int
    state_South_Dakota: int
    state_Tennessee: int
    state_Texas: int
    state_Utah: int
    state_Vermont: int
    state_Virginia: int
    state_Washington: int
    state_West_Virginia: int
    state_Wisconsin: int
    state_Wyoming: int

    price_per_sqft: float | None = None
    lot_per_sqft: float | None = None


# -----------------------------------------------------------
# MLflow – carga de modelo
# -----------------------------------------------------------
def get_model_uri():
    MODEL_NAME = "HousePriceModel"
    MODEL_STAGE = "Production"
    return f"models:/{MODEL_NAME}/{MODEL_STAGE}"


def load_model():
    global model, model_loaded_at
    try:
        logger.info("Cargando modelo desde MLflow...")

        os.environ["MLFLOW_S3_ENDPOINT_URL"] = os.getenv("MLFLOW_S3_ENDPOINT_URL", "http://http://10.43.100.83:9000")
        os.environ["AWS_ACCESS_KEY_ID"] = os.getenv("AWS_ACCESS_KEY_ID", "admin")
        os.environ["AWS_SECRET_ACCESS_KEY"] = os.getenv("AWS_SECRET_ACCESS_KEY", "supersecret")

        mlflow.set_tracking_uri(os.getenv("MLFLOW_TRACKING_URI", "http://10.43.100.83:8084"))

        model = mlflow.pyfunc.load_model(get_model_uri())
        model_loaded_at = time.time()

        logger.info(f"Modelo cargado correctamente: {type(model).__name__}")

    except Exception as e:
        logger.error(f"No se pudo cargar el modelo: {e}")
        model = None
        model_loaded_at = None
        raise e


# -----------------------------------------------------------
# Recarga automática de modelo
# -----------------------------------------------------------
async def auto_reload_model(interval_sec=600):
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


# -----------------------------------------------------------
# Endpoint de métricas
# -----------------------------------------------------------
@app.get("/metrics")
def metrics():
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)


# -----------------------------------------------------------
# Endpoint /predict
# -----------------------------------------------------------
@app.post("/predict")
def predict(features: HouseFeatures, request: Request):
    endpoint = "/predict"
    method = request.method
    start_time = time.time()

    if model is None:
        REQUEST_COUNT.labels(endpoint=endpoint, method=method, http_status="503").inc()
        raise HTTPException(status_code=503, detail="Modelo no disponible")

    try:
        feature_df = pd.DataFrame([features.dict()])

        # Si tu modelo no usa estas columnas, puedes retirarlas:
        drop_cols = ["price_per_sqft", "lot_per_sqft"]
        feature_df = feature_df.drop(columns=[c for c in drop_cols if c in feature_df.columns])

        prediction = model.predict(feature_df)[0]

        REQUEST_COUNT.labels(endpoint=endpoint, method=method, http_status="200").inc()
        return {
            "predicted_price": float(prediction),
            "message": "Predicción completada con éxito",
            "model_loaded_at": model_loaded_at
        }

    except Exception as e:
        REQUEST_COUNT.labels(endpoint=endpoint, method=method, http_status="400").inc()
        raise HTTPException(status_code=400, detail=str(e))

    finally:
        REQUEST_LATENCY.labels(endpoint=endpoint).observe(time.time() - start_time)


# -----------------------------------------------------------
# Recargar modelo manual
# -----------------------------------------------------------
@app.post("/reload-model")
def reload_model():
    try:
        load_model()
        return {"status": "success", "model_loaded_at": model_loaded_at}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error recargando modelo: {str(e)}")


# -----------------------------------------------------------
# Health Check
# -----------------------------------------------------------
@app.get("/health")
def health():
    return {
        "status": "healthy",
        "model_loaded": model is not None,
        "model_loaded_at": model_loaded_at
    }


# -----------------------------------------------------------
# Información del modelo
# -----------------------------------------------------------
@app.get("/model-info")
def model_info():
    if model is None:
        raise HTTPException(status_code=503, detail="Modelo no disponible")

    return {
        "model_type": str(type(model).__name__),
        "model_loaded": True,
        "description": "Modelo de predicción de precio de casas",
        "model_loaded_at": model_loaded_at
    }
