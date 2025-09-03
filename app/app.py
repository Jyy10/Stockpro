# app.py 文件中需要被替换的整个代码块

if st.sidebar.button('🚀 获取公告列表'):
    # 重置状态
    st.session_state.announcement_list = pd.DataFrame()
    st.session_state.detailed_results = {}
    
    if not keywords or len(date_range) != 2:
        st.error("请输入关键词并选择有效的日期范围。")
    else:
        start_date, end_date = date_range
        
        results_df = pd.DataFrame()
        # 创建一个用于状态更新的占位符
        status_placeholder = st.empty() 

        try:
            with st.spinner('正在从主数据源(巨潮资讯网)抓取公告列表...'):
                results_df = dh.scrape_cninfo(keywords, start_date, end_date)
            if results_df.empty:
                raise ValueError("主数据源未返回任何数据")
            
            status_placeholder.empty() # 如果主数据源成功，清空状态占位符

        except Exception as e:
            # 【关键修正】在这里！
            # 我们将 status_placeholder 作为第四个参数传给了 scrape_akshare 函数
            with status_placeholder.container():
                st.warning("⚠️ 检测到主数据源(巨潮)访问失败或无结果，已自动切换到备用数据源(东方财富)。")
                try:
                    # 将占位符传入备用函数
                    results_df = dh.scrape_akshare(keywords, start_date, end_date, status_placeholder)
                except Exception as e_ak:
                    st.error(f"备用数据源(AkShare)也抓取失败: {e_ak}")
        
        if not results_df.empty:
            st.session_state.announcement_list = results_df
            status_placeholder.success(f"成功获取 {len(results_df)} 条公告概览！")
        else:
            # 只有在两个源都尝试过后，才显示最终的警告
            if 'announcement_list' not in st.session_state or st.session_state.announcement_list.empty:
                 status_placeholder.warning("在指定条件下，主、备数据源均未找到任何相关公告。")
