import streamlit as st
import requests
import os

# ---------------------------------------------------
# Configuración de la API
# ---------------------------------------------------
API_URL = os.getenv("API_URL", "http://fastapi-service:8000") + "/predict"


# ---------------------------------------------------
# Función para convertir estado → one-hot (modelo FastAPI)
# ---------------------------------------------------
def encode_state(selected_state):
    states = [
        "Alabama","Alaska","Arizona","Arkansas","California","Colorado","Connecticut","Delaware",
        "District_of_Columbia","Florida","Georgia","Hawaii","Idaho","Illinois","Indiana","Iowa",
        "Kansas","Kentucky","Louisiana","Maine","Maryland","Michigan","Minnesota","Mississippi",
        "Missouri","Montana","Nebraska","Nevada","New_Hampshire","New_Jersey","New_Mexico",
        "New_York","North_Carolina","North_Dakota","Ohio","Oklahoma","Oregon","Pennsylvania",
        "Rhode_Island","South_Carolina","South_Dakota","Tennessee","Texas","Utah","Vermont",
        "Virginia","Washington","West_Virginia","Wisconsin","Wyoming"
    ]

    encoded = {f"state_{s.replace(' ', '_')}": 0 for s in states}
    encoded[f"state_{selected_state}"] = 1
    return encoded


# ---------------------------------------------------
# Interfaz Streamlit
# ---------------------------------------------------
st.title("Predicción de Precio de Casas")

st.header("Ingrese la información de la propiedad:")

# Variables numéricas
bed = st.number_input("Habitaciones", min_value=0.0, value=3.0)
bath = st.number_input("Baños", min_value=0.0, value=2.0)
acre_lot = st.number_input("Tamaño del lote (acres)", min_value=0.0, value=0.2)
house_size = st.number_input("Tamaño de la casa (ft²)", min_value=0.0, value=1500.0)

# Selección de estado
st.subheader("Estado de la propiedad")
states = [
    "Alabama","Alaska","Arizona","Arkansas","California","Colorado","Connecticut","Delaware",
    "District_of_Columbia","Florida","Georgia","Hawaii","Idaho","Illinois","Indiana","Iowa",
    "Kansas","Kentucky","Louisiana","Maine","Maryland","Michigan","Minnesota","Mississippi",
    "Missouri","Montana","Nebraska","Nevada","New_Hampshire","New_Jersey","New_Mexico",
    "New_York","North_Carolina","North_Dakota","Ohio","Oklahoma","Oregon","Pennsylvania",
    "Rhode_Island","South_Carolina","South_Dakota","Tennessee","Texas","Utah","Vermont",
    "Virginia","Washington","West_Virginia","Wisconsin","Wyoming"
]
state = st.selectbox("Estado", states)

# Campos adicionales opcionales
price_per_sqft = st.number_input("Precio por pie cuadrado (opcional)", value=0.0)
lot_per_sqft = st.number_input("Precio por pie de lote (opcional)", value=0.0)

# ---------------------------------------------------
# Botón de predicción
# ---------------------------------------------------
if st.button("Predecir precio"):
    
    # Crear diccionario base
    input_dict = {
        "bed": bed,
        "bath": bath,
        "acre_lot": acre_lot,
        "house_size": house_size,
        "price_per_sqft": price_per_sqft,
        "lot_per_sqft": lot_per_sqft,
    }

    # Agregar one-hot de estado
    input_dict.update(encode_state(state))

    try:
        response = requests.post(API_URL, json=input_dict)

        if response.status_code == 200:
            result = response.json()
            st.success(f"Precio estimado: ${result['predicted_price']:,.2f}")
            st.info(f"Mensaje: {result['message']}")

        else:
            st.error(f"Error en la API: {response.text}")

    except Exception as e:
        st.error(f"Error de conexión: {e}")
