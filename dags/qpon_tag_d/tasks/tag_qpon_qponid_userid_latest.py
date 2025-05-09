
def get_tag_qpon_qponid_userid_latest_sql():
    # 任务配置
    project_id = "oppo-gcp-prod-digfood-129869"
    insert_dataset_id = "qpon_services_test"
    insert_table_id = "tag_qpon_qponid_userid_latest_test"
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
		m1.user_id as user_id
		, SPLIT(m1.device_id, ':')[SAFE_OFFSET(0)] as device_id
		, m1.fcm_token as fcm_token
		, m2.adid as adid
		, DATE({partition_date}) as dayno
	from(
		select 
			user_id
			, device_id
			, fcm_token
		from
		(
			select 
				user_id
				, device_id
				, fcm_token
				, row_number() over(partition by device_id order by update_time desc) as rn 
			from `oppo-gcp-prod-digfood-129869.qpon_ods_d.ods_digital_food_device`
			where partition_date = DATE({partition_date})
				and device_id is not null 
				and device_id not in('','null','NULL')
			)
		where rn = 1
	) m1
	left join (
		select 
			uni_device_id
			, adid as adid
		from `oppo-gcp-prod-digfood-129869.qpon_dwd_d.dwd_dau_inc_d` 
		where partition_date is not null
			and uni_device_id is not null 
			and uni_device_id not in ('','null','NULL')
		group by uni_device_id, adid
	) m2 on m1.device_id = m2.uni_device_id
	
	;
	
	
	merge `{project_id}.{insert_dataset_id}.{insert_metadata_table_id}` as target
	using (
		select 
			'ID_MAPPING' as  data_type
			,'ID_MAPPING' as data_id
			,max(dayno) as latest_dayno
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