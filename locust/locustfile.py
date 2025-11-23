from locust import HttpUser, task, between
import json

class DiabetesUser(HttpUser):
    wait_time = between(1, 3)  # Espera entre 1 y 3 segundos entre requests
    
    @task(1)
    def predict(self):
        """Prueba el endpoint de predicción"""
        payload = {
                    "discharge_disposition_id": 3,
                    "admission_source_id": 7,
                    "time_in_hospital": 5,
                    "num_lab_procedures": 34,
                    "num_procedures": 2,
                    "num_medications": 20,
                    "number_outpatient": 0,
                    "number_emergency": 0,
                    "number_inpatient": 0,
                    "number_diagnoses": 6,
                    "max_glu_serum": 0,
                    "A1Cresult": 0,
                    "race_AfricanAmerican": 1,
                    "race_Asian": 0,
                    "race_Caucasian": 0,
                    "race_Hispanic": 0,
                    "race_Other": 0,
                    "gender_Female": 1,
                    "gender_Male": 0,
                    "age_0_10": 0,
                    "age_10_20": 0,
                    "age_20_30": 0,
                    "age_30_40": 0,
                    "age_40_50": 0,
                    "age_50_60": 0,
                    "age_60_70": 0,
                    "age_70_80": 1,
                    "age_80_90": 0,
                    "age_90_100": 0,
                    "cambio_Ch": 0,
                    "cambio_No": 1,
                    "diabetesMed_No": 0,
                    "diabetesMed_Yes": 1,
                    "diag_1_Circulatory": 0,
                    "diag_1_Diabetes": 0,
                    "diag_1_Digestive": 0,
                    "diag_1_Genitourinary": 0,
                    "diag_1_Injury": 1,
                    "diag_1_Musculoskeletal": 0,
                    "diag_1_Neoplasms": 0,
                    "diag_1_Other": 0,
                    "diag_1_Respiratory": 0,
                    "diag_2_Circulatory": 0,
                    "diag_2_Diabetes": 0,
                    "diag_2_Digestive": 0,
                    "diag_2_Genitourinary": 0,
                    "diag_2_Injury": 0,
                    "diag_2_Musculoskeletal": 0,
                    "diag_2_Neoplasms": 0,
                    "diag_2_Other": 1,
                    "diag_2_Respiratory": 0,
                    "diag_3_Circulatory": 1,
                    "diag_3_Diabetes": 0,
                    "diag_3_Digestive": 0,
                    "diag_3_Genitourinary": 0,
                    "diag_3_Injury": 0,
                    "diag_3_Musculoskeletal": 0,
                    "diag_3_Neoplasms": 0,
                    "diag_3_Other": 0,
                    "diag_3_Respiratory": 0
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