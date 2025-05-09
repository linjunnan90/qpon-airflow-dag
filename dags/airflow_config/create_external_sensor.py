from airflow.sensors.external_task import ExternalTaskSensor

def create_external_sensor(dag, dag_name, task_name):
    return ExternalTaskSensor(
        task_id=f"wait_{task_name}",
        external_dag_id=dag_name,
        external_task_id=task_name,
        mode='reschedule',
        timeout=7200,
        poke_interval=600,
        allowed_states=['success'],
        failed_states=['failed'],
        priority_weight=100,  # 显式设置权重
        dag=dag
    )