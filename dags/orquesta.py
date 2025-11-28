"""
Orquestador Completo: Pipeline ETL + ML hacia MySQL en Kubernetes
Flujo: API -> temp_raw -> raw -> clean -> GradientBoosting -> MLflow
"""

import sys
import os
from airflow import DAG
from airflow.providers.mysql.operators.mysql import MySqlOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.sensors.filesystem import FileSensor
from datetime import datetime, timedelta
from airflow.exceptions import AirflowSkipException 

# Agregar carpeta scripts al PATH
sys.path.append(os.path.join(os.path.dirname(__file__), 'scripts'))

# Importar funciones ETL y ML

from funciones import (
   restart_api_data,
    insert_data_temp_raw,
    read_temp_raw_and_insert_raw,
    transform_raw_to_clean,
    check_api_has_data,
    train_model,
    promote_best_model
)

# Importar queries SQL
from scripts.queries import (
    DROP_TABLE_RAW, DROP_TABLE_TEMP_RAW, DROP_TABLE_CLEAN,
    CREATE_TABLE_RAW, CREATE_TABLE_TEMP_RAW, CREATE_TABLE_CLEAN,
    TRUNCATE_TABLE_TEMP_RAW
)

# Ruta del modelo
MODEL_PATH = "/opt/airflow/models/GradientBoostingRegressor.pkl"

# ===========================================
# LOGICA NUEVA: USO DE VARIABLE DE AIRFLOW
# ===========================================

def branch_first_run(**kwargs):
    """
    Compara la fecha de ejecución actual con la fecha de inicio del DAG.
    Si son iguales, es la primera ejecución histórica.
    """
    # Obtener fechas del contexto
    dag_start_date = kwargs['dag'].start_date
    current_logical_date = kwargs['logical_date']
    
    print(f" Fecha Inicio DAG: {dag_start_date}")
    print(f"⏱ Fecha Ejecución Actual: {current_logical_date}")

    # Comparación directa
    if current_logical_date == dag_start_date:
        print(" Esta es la PRIMERA ejecución absoluta. Iniciando API.")
        return "restart_api"
    else:
        print("⏭ Esta es una ejecución posterior. Saltando inicialización.")
        return "join_after_init"

# ===========================================
# VERIFICAR QUE LA API PRODUCE DATOS
# ===========================================

def branch_has_data():
    """
    CAMBIO CRÍTICO: Ahora apunta DIRECTAMENTE a truncate_temp_raw
    y NO a end_pipeline, para no romper el flujo de ML
    """
    if check_api_has_data():
        print(" API tiene datos - Continuando con ETL + ML")
        return "truncate_temp_raw"
    else:
        print(" API agotada - Saltando ETL pero continuando con ML sobre datos existentes")
        return "skip_etl_go_to_ml"  

# Configuración del DAG
start_date = datetime(2023, 1, 1)
interval = timedelta(minutes=5)
end_date = start_date + interval * 4  

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=3),
}

with DAG(
    dag_id="orquestador_final_mlops_1",
    default_args=default_args,
    description="Pipeline ETL + ML: API -> MySQL -> GradientBoostingRegressor -> MLflows",
    start_date=start_date,
    end_date=end_date,
    schedule_interval="*/5 * * * *",
    catchup=True,
    max_active_runs=1,
    tags=['etl', 'ml', 'house', 'production', 'kubernetes'],
) as dag:

    # ===== DECISIÓN INICIAL =====
    check_first_run = BranchPythonOperator(
        task_id="check_first_run",
        python_callable=branch_first_run,
    )

    # ===== RAMA DE INICIALIZACIÓN (SOLO PRIMERA VEZ) =====
    
    restart_api = PythonOperator(
        task_id="restart_api",
        python_callable=restart_api_data,
    )

    delete_table_raw = MySqlOperator(
        task_id="delete_table_raw",
        mysql_conn_id="mysql_conn",
        sql=DROP_TABLE_RAW,
    )

    delete_table_temp_raw = MySqlOperator(
        task_id="delete_table_temp_raw",
        mysql_conn_id="mysql_conn",
        sql=DROP_TABLE_TEMP_RAW,
    )

    delete_table_clean = MySqlOperator(
        task_id="delete_table_clean",
        mysql_conn_id="mysql_conn",
        sql=DROP_TABLE_CLEAN,
    )

    create_table_raw = MySqlOperator(
        task_id="create_table_raw",
        mysql_conn_id="mysql_conn",
        sql=CREATE_TABLE_RAW,
    )

    create_table_temp_raw = MySqlOperator(
        task_id="create_table_temp_raw",
        mysql_conn_id="mysql_conn",
        sql=CREATE_TABLE_TEMP_RAW,
    )

    create_table_clean = MySqlOperator(
        task_id="create_table_clean",
        mysql_conn_id="mysql_conn",
        sql=CREATE_TABLE_CLEAN,
    )

    # ===== JOIN POINT =====
    join_after_init = EmptyOperator(
        task_id="join_after_init",
        trigger_rule="none_failed_min_one_success"
    )

    # ===== VERIFICACIÓN DE DATOS =====
    
    check_data_available = BranchPythonOperator(
        task_id="check_data_available",
        python_callable=branch_has_data,
    )

    # ===== NUEVA TAREA: SALTAR ETL E IR DIRECTO A ML =====
    skip_etl_go_to_ml = EmptyOperator(
        task_id="skip_etl_go_to_ml"
    )

    # ===== PROCESAMIENTO DE DATOS =====
    
    truncate_temp_raw = MySqlOperator(
        task_id="truncate_temp_raw",
        mysql_conn_id="mysql_conn",
        sql=TRUNCATE_TABLE_TEMP_RAW,
    )

    load_to_temp_raw = PythonOperator(
        task_id="load_to_temp_raw",
        python_callable=insert_data_temp_raw,
    )

    temp_raw_to_raw = PythonOperator(
        task_id="temp_raw_to_raw",
        python_callable=read_temp_raw_and_insert_raw,
    )

    raw_to_clean = PythonOperator(
        task_id="raw_to_clean",
        python_callable=transform_raw_to_clean,
    )

    # ===== JOIN ANTES DE ML =====
    join_before_ml = EmptyOperator(
        task_id="join_before_ml",
        trigger_rule="none_failed_min_one_success"  
    )

    # =========================================================================
    #  MACHINE LEARNING (Siempre se ejecuta)
    # =========================================================================
    
    train_ml_model = PythonOperator(
        task_id='train_model',
        python_callable=train_model,
    )
    
    promote_to_production = PythonOperator(
        task_id='promote_to_production',
        python_callable=promote_best_model,
    )

    # =========================================================================
    #  FIN DEL PIPELINE
    # =========================================================================
    
    end_pipeline = EmptyOperator(
        task_id="end_pipeline"
    )

    # ===========================================
    # DEPENDENCIAS
    # ===========================================

    # RAMA 1: Primera ejecución (inicialización completa)
    check_first_run >> restart_api
    restart_api >> delete_table_raw >> delete_table_temp_raw >> delete_table_clean
    delete_table_clean >> create_table_raw >> create_table_temp_raw >> create_table_clean
    
    # Conexión final de la inicialización al join
    create_table_clean >> join_after_init

    # RAMA 2: Ejecuciones posteriores (skip inicialización)
    check_first_run >> join_after_init

    # Continúa el flujo después del join
    join_after_init >> check_data_available

    # RAMA 3A: SI hay datos en API -> ETL completo
    check_data_available >> truncate_temp_raw >> load_to_temp_raw
    load_to_temp_raw >> temp_raw_to_raw >> raw_to_clean
    raw_to_clean >> join_before_ml

    # RAMA 3B: NO hay datos en API -> Skip ETL, ir directo a ML
    check_data_available >> skip_etl_go_to_ml >> join_before_ml

    # FLUJO DE ML (SIEMPRE SE EJECUTA)
    join_before_ml >> train_ml_model >> promote_to_production >> end_pipeline