from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
import importlib

def create_composer_bq_task(dag, warehouse_layer, task_module_name, on_failure_callback=None):
    """
    根据模块名动态加载 SQL 构造函数并创建 BigQueryInsertJobOperator 任务

    参数:
        dag: airflow DAG 对象
        task_module_name: 与 SQL 构造函数一致的模块名（例如 tag_qpon_qponid_userid_latest）
        on_failure_callback: 可选失败回调
    返回:
        BigQueryInsertJobOperator 实例
    """
    # 动态导入 SQL 构造函数模块
    module_path = f"{warehouse_layer}.tasks.{task_module_name}"
    module = importlib.import_module(module_path)
    
    # 获取对应的 SQL 构造函数（命名规则：get_<task_module_name>_sql）
    get_sql_func = getattr(module, f"get_{task_module_name}_sql")
    sql = get_sql_func()

    return BigQueryInsertJobOperator(
        task_id=task_module_name,
        configuration={
            "query": {
                "query": sql,
                "useLegacySql": False,
                "location": "us-central1"
            }
        },
        on_failure_callback=on_failure_callback,
        dag=dag
    )
