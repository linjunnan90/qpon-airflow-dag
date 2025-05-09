
def get_h5_90_orders_num_sql():
    # 任务配置
    project_id = "oppo-gcp-prod-digfood-129869"
    tag_name = "'H5_90_orders_num'"
    insert_dataset_id = "qpon_services_test"
    insert_table_id = "tag_qpon_all_d_test"
    insert_metadata_table_id = "tag_qpon_metadata_test"
    
    # 使用时区感知的时间计算（去掉外层单引号）
    partition_date = "'{{ execution_date.add(days=1).strftime('%Y-%m-%d') }}'"
    partition_last_90_date = "'{{ execution_date.add(days=-88).strftime('%Y-%m-%d') }}'"

    # 修正后的 SQL 模板
    return f"""
    delete from `{project_id}.{insert_dataset_id}.{insert_table_id}`
    where dayno = DATE({partition_date})
      and tag_name = {tag_name};

    INSERT INTO `{project_id}.{insert_dataset_id}.{insert_table_id}`
    select 
      user_device_id as device_id,
      cast(orders as string) as tag_value,
      DATE({partition_date}) as dayno,
      {tag_name} as tag_name
    from (
      select
        user_device_id,
        count(distinct id) as orders
      from `oppo-gcp-prod-digfood-129869.qpon_dwd_d.dwd_product_order_voucher_all`
      where partition_date = DATE({partition_date})
        and substr(order_pay_time,1,10) between {partition_last_90_date} and {partition_date}
        and order_status = 'COMPLETED'
        and host_environment = 'h5'
        and is_formal = 1
      group by 1
    );

    merge `{project_id}.{insert_dataset_id}.{insert_metadata_table_id}` as target
    using (
      select 
        'TAG' as data_type,
        {tag_name} as data_id,
        max(dayno) as latest_dayno
      from `{project_id}.{insert_dataset_id}.{insert_table_id}`
      where tag_name = {tag_name}
        and dayno is not null
      group by data_type, data_id
    ) as source
    on target.data_type = source.data_type
      and target.data_id = source.data_id
    when matched then
      update set latest_dayno = source.latest_dayno
    when not matched then
      insert (data_type, data_id, latest_dayno)
      values (source.data_type, source.data_id, source.latest_dayno);
    """