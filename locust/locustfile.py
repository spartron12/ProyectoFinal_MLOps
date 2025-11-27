from locust import HttpUser, task, between
import json

class HousePriceUser(HttpUser):
    wait_time = between(1, 3)  # Espera entre 1 y 3 segundos entre requests
    
    @task(1)
    def predict(self):
        """Prueba el endpoint de predicción de precios de casas"""
        payload = {
            "bed": 2,
            "bath": 2,
            "acre_lot": 1.16,
            "house_size": 1200,
            "state_Alabama": 0,
            "state_Alaska": 0,
            "state_Arizona": 0,
            "state_Arkansas": 1,
            "state_California": 0,
            "state_Colorado": 0,
            "state_Connecticut": 0,
            "state_Delaware": 0,
            "state_District_of_Columbia": 0,
            "state_Florida": 0,
            "state_Georgia": 0,
            "state_Hawaii": 0,
            "state_Idaho": 0,
            "state_Illinois": 0,
            "state_Indiana": 0,
            "state_Iowa": 0,
            "state_Kansas": 0,
            "state_Kentucky": 0,
            "state_Louisiana": 0,
            "state_Maine": 0,
            "state_Maryland": 0,
            "state_Michigan": 0,
            "state_Minnesota": 0,
            "state_Mississippi": 0,
            "state_Missouri": 0,
            "state_Montana": 0,
            "state_Nebraska": 0,
            "state_Nevada": 0,
            "state_New_Hampshire": 0,
            "state_New_Jersey": 0,
            "state_New_Mexico": 0,
            "state_New_York": 0,
            "state_North_Carolina": 0,
            "state_North_Dakota": 0,
            "state_Ohio": 0,
            "state_Oklahoma": 0,
            "state_Oregon": 0,
            "state_Pennsylvania": 0,
            "state_Rhode_Island": 0,
            "state_South_Carolina": 0,
            "state_South_Dakota": 0,
            "state_Tennessee": 0,
            "state_Texas": 0,
            "state_Utah": 0,
            "state_Vermont": 0,
            "state_Virginia": 0,
            "state_Washington": 0,
            "state_West_Virginia": 0,
            "state_Wisconsin": 0,
            "state_Wyoming": 0
        }
        self.client.post("/predict", json=payload)
    
    @task(1)
    def health_check(self):
        """Prueba el endpoint de health"""
        self.client.get("/health")
    
    @task(2)
    def model_info(self):
        """Prueba el endpoint de info del modelo"""
        self.client.get("/model-info")
