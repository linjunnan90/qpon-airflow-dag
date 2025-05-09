
def get_app_Lastactive_days_sql():
    # 任务配置
    project_id = "oppo-gcp-prod-digfood-129869"
    tag_name = "'App_Lastactive_days'"
    insert_dataset_id = "qpon_services_test"
    insert_table_id = "tag_qpon_all_d_test"
    insert_metadata_table_id = "tag_qpon_metadata_test"
    
    # 使用时区感知的时间计算（去掉外层单引号）
    partition_date = "'{{ execution_date.add(days=1).strftime('%Y-%m-%d') }}'"
    partition_last_180_date = "'{{ execution_date.add(days=-178).strftime('%Y-%m-%d') }}'"

    # 修正后的 SQL 模板
    return f"""
	delete from `{project_id}.{insert_dataset_id}.{insert_table_id}`
	where dayno = DATE({partition_date}) and tag_name = {tag_name}
	;
	
	INSERT INTO `{project_id}.{insert_dataset_id}.{insert_table_id}`
	
	select 
		uni_device_id as device_id
		, cast(if(active_last_time_day_diff*-1 >= 180, 180, active_last_time_day_diff*-1) as string) as tag_value
		, DATE({partition_date}) as dayno
		,{tag_name} as tag_name
	from
	(
		select
			uni_device_id
			, DATE_DIFF(
				PARSE_DATE('%Y-%m-%d', max(last_act_day)),
				CURRENT_DATE(), 
				DAY
			) AS active_last_time_day_diff
		from `oppo-gcp-prod-digfood-129869.qpon_dws_d.dws_qpon_device_active_info_all_d` 
		where partition_date = DATE({partition_date})
			and host_environment = 'app'
		group by 1
		)
	;
	
	merge `{project_id}.{insert_dataset_id}.{insert_metadata_table_id}` as target
	using (
		select 
			'TAG' as  data_type
			, tag_name as data_id 
			, max(dayno) as latest_dayno
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
		values (source.data_type, source.data_id, source.latest_dayno)
	;
    """