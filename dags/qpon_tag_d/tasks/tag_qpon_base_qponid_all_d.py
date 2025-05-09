
def get_tag_qpon_base_qponid_all_d_sql():
    # 任务配置
    project_id = "oppo-gcp-prod-digfood-129869"
    insert_dataset_id = "qpon_services_test"
    insert_table_id = "tag_qpon_base_qponid_all_d_test"
    insert_metadata_table_id = "tag_qpon_metadata_test"
    
    # 使用时区感知的时间计算（去掉外层单引号）
    partition_date = "'{{ execution_date.add(days=1).strftime('%Y-%m-%d') }}'"

    # 修正后的 SQL 模板
    return f"""
	delete from `{project_id}.{insert_dataset_id}.{insert_table_id}`
	where dayno = DATE({partition_date})
	;
	
	INSERT INTO `{project_id}.{insert_dataset_id}.{insert_table_id}`
	
	select 
		SPLIT(device_id, ':')[SAFE_OFFSET(0)] as device_id
		, DATE({partition_date}) as dayno
	from
	(
		select 
			device_id
		from `oppo-gcp-prod-digfood-129869.digital_food_order.digital_food_device`
		where device_id is not null 
			and device_id not in ('','null','NULL')
		group by 1
	)
	;
	
	merge `{project_id}.{insert_dataset_id}.{insert_metadata_table_id}` as target
	using (
		select 
			'BASE_DEVICE_ID' as  data_type
			, 'BASE_DEVICE_ID' as data_id 
			, max(dayno) as latest_dayno
		from `{project_id}.{insert_dataset_id}.{insert_table_id}`
		where dayno is not null
		group by data_type, data_id
	) as source
	on target.data_type = source.data_type
		and target.data_id = source.data_id
	when matched then
		update set latest_dayno = source.latest_dayno
	when not matched then
		insert (data_type, data_id, latest_dayno)
		values (source.data_type, source.data_id, source.latest_dayno)
	;
    """