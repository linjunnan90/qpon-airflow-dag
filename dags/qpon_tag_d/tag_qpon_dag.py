from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.dummy import DummyOperator
from airflow_config.airflow_tt_send import send_failure_alert_factory
from airflow_config.create_composer_bq_task import create_composer_bq_task
from airflow_config.create_external_sensor import create_external_sensor

# ######################tt 告警设置 ####################################

send_url = "https://mtp.myoas.com/gateway/robot/webhook/send?yzjtype=0&yzjtoken=a1c0a692cdce4f4fba69b65ff65f3ca5"

failure_callback = send_failure_alert_factory(send_url)

# ######################tt 告警设置 ####################################

warehouse_layer = "qpon_tag_d"

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 5,
    'retry_delay': timedelta(minutes=2),
}

with DAG(
    "tag_qpon_dag_d",
    default_args=default_args,
    description='Qpon Tag DAG DAY',
    schedule_interval='0 18 * * *',
    catchup=False
) as dag:

    # 传感器任务
    start_task = DummyOperator(
        task_id="start",
        dag=dag
    )

    # 创建传感器实例（缩进对齐）
    dwd_dau_inc_d_sensor = create_external_sensor(dag, "dwd_dau_inc_d", "dwd_dau_inc_d")
    dwd_product_order_voucher_all_sensor = create_external_sensor(dag, "dwd_product_order_voucher_all", "dwd_product_order_voucher_all")
    dws_qpon_device_active_info_all_d_sensor = create_external_sensor(dag, "dws_qpon_device_active_info_all_d", "dws_qpon_device_active_info_all_d")
	
	
	

    # 创建下游任务（缩进对齐）
    app_7_active_days_task = create_composer_bq_task(dag, warehouse_layer, "app_7_active_days", on_failure_callback = failure_callback)
    app_7_orders_num_task = create_composer_bq_task(dag, warehouse_layer, "app_7_orders_num", on_failure_callback = failure_callback)
    app_15_active_days_task = create_composer_bq_task(dag, warehouse_layer, "app_15_active_days", on_failure_callback = failure_callback)
    app_15_orders_num_task = create_composer_bq_task(dag, warehouse_layer, "app_15_orders_num", on_failure_callback = failure_callback)
    app_30_active_days_task = create_composer_bq_task(dag, warehouse_layer, "app_30_active_days", on_failure_callback = failure_callback)
    app_30_orders_num_task = create_composer_bq_task(dag, warehouse_layer, "app_30_orders_num", on_failure_callback = failure_callback)
    app_60_active_days_task = create_composer_bq_task(dag, warehouse_layer, "app_60_active_days", on_failure_callback = failure_callback)
    app_60_orders_num_task = create_composer_bq_task(dag, warehouse_layer, "app_60_orders_num", on_failure_callback = failure_callback)
    app_90_active_days_task = create_composer_bq_task(dag, warehouse_layer, "app_90_active_days", on_failure_callback = failure_callback)
    app_90_orders_num_task = create_composer_bq_task(dag, warehouse_layer, "app_90_orders_num", on_failure_callback = failure_callback)
    h5_7_active_days_task = create_composer_bq_task(dag, warehouse_layer, "h5_7_active_days", on_failure_callback = failure_callback)
    h5_7_orders_num_task = create_composer_bq_task(dag, warehouse_layer, "h5_7_orders_num", on_failure_callback = failure_callback)
    h5_15_active_days_task = create_composer_bq_task(dag, warehouse_layer, "h5_15_active_days", on_failure_callback = failure_callback)
    h5_15_orders_num_task = create_composer_bq_task(dag, warehouse_layer, "h5_15_orders_num", on_failure_callback = failure_callback)
    h5_30_active_days_task = create_composer_bq_task(dag, warehouse_layer, "h5_30_active_days", on_failure_callback = failure_callback)
    h5_30_orders_num_task = create_composer_bq_task(dag, warehouse_layer, "h5_30_orders_num", on_failure_callback = failure_callback)
    h5_60_active_days_task = create_composer_bq_task(dag, warehouse_layer, "h5_60_active_days", on_failure_callback = failure_callback)
    h5_60_orders_num_task = create_composer_bq_task(dag, warehouse_layer, "h5_60_orders_num", on_failure_callback = failure_callback)
    h5_90_active_days_task = create_composer_bq_task(dag, warehouse_layer, "h5_90_active_days", on_failure_callback = failure_callback)
    h5_90_orders_num_task = create_composer_bq_task(dag, warehouse_layer, "h5_90_orders_num", on_failure_callback = failure_callback)
    app_Lastorder_days_task = create_composer_bq_task(dag, warehouse_layer, "app_Lastorder_days", on_failure_callback = failure_callback)
    app_Lastactive_days_task = create_composer_bq_task(dag, warehouse_layer, "app_Lastactive_days", on_failure_callback = failure_callback)
    tag_qpon_base_qponid_all_d_task = create_composer_bq_task(dag, warehouse_layer, "tag_qpon_base_qponid_all_d", on_failure_callback = failure_callback)
    tag_qpon_qponid_userid_latest_task = create_composer_bq_task(dag, warehouse_layer, "tag_qpon_qponid_userid_latest", on_failure_callback = failure_callback)

    # 设置依赖关系（缩进对齐，移除注释中的非法字符）
    start_task >> [
        dwd_dau_inc_d_sensor,
        dwd_product_order_voucher_all_sensor,
        dws_qpon_device_active_info_all_d_sensor,
        tag_qpon_base_qponid_all_d_task
    ]

    dwd_dau_inc_d_sensor >> [
        app_7_active_days_task, 
        app_15_active_days_task, 
        app_30_active_days_task, 
        app_60_active_days_task, 
        app_90_active_days_task, 
        h5_7_active_days_task, 
        h5_15_active_days_task, 
        h5_30_active_days_task, 
        h5_60_active_days_task, 
        h5_90_active_days_task, 
        tag_qpon_qponid_userid_latest_task
    ]

    dwd_product_order_voucher_all_sensor >> [
        app_7_orders_num_task, 
        app_15_orders_num_task, 
        app_30_orders_num_task, 
        app_60_orders_num_task, 
        app_90_orders_num_task, 
        h5_7_orders_num_task, 
        h5_15_orders_num_task, 
        h5_30_orders_num_task, 
        h5_60_orders_num_task, 
        h5_90_orders_num_task, 
        app_Lastorder_days_task
    ]

    dws_qpon_device_active_info_all_d_sensor >> app_Lastactive_days_task