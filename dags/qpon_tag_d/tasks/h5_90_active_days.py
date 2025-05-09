
def get_h5_90_active_days_sql():
    # 任务配置
    project_id = "oppo-gcp-prod-digfood-129869"
    tag_name = "'H5_90_active_days'"
    insert_dataset_id = "qpon_services_test"        # 统一使用 4 空格缩进
    insert_table_id = "tag_qpon_all_d_test"
    insert_metadata_table_id = "tag_qpon_metadata_test"
    
    # 使用时区感知的时间计算
    partition_date = "'{{ execution_date.add(days=1).strftime('%Y-%m-%d') }}'"
    partition_last_90_date = "'{{ execution_date.add(days=-88).strftime('%Y-%m-%d') }}'"

    # SQL模板
    return f"""
    delete from `{project_id}.{insert_dataset_id}.{insert_table_id}`
    where dayno = DATE({partition_date}) and tag_name = {tag_name};
    
    INSERT INTO `{project_id}.{insert_dataset_id}.{insert_table_id}`
    select 
        uni_device_id as device_id,
        cast(days as string) as tag_value,
        DATE({partition_date}) as dayno,
        {tag_name} as tag_name
    from (
        select 
            uni_device_id,
            count(distinct partition_date) as days
        from `oppo-gcp-prod-digfood-129869.qpon_dwd_d.dwd_dau_inc_d` 
        where partition_date between DATE({partition_last_90_date}) and DATE({partition_date})
            and uni_device_id is not null 
            and uni_device_id not in ('','null','NULL')
            and host_environment = 'h5'
        group by 1
    );
    
    merge `{project_id}.{insert_dataset_id}.{insert_metadata_table_id}` as target
    using (
        select 
            'TAG' as data_type,
            tag_name as data_id,
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