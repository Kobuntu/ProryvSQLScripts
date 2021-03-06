
if ( exists (select top 1 1 from sysobjects where name like 'usp2_Expl_Import_Arch30Values'))
drop procedure usp2_Expl_Import_Arch30Values
go

create procedure [dbo].[usp2_Expl_Import_Arch30Values] 
   @InputData [Arch30VirtualValuesType] readonly,
   @ValueType tinyint
as
begin

--@ValueType = null or 0 - VAL, 1 - CAL, 2 FVAL
--ValidStatus должен быть заполнен 0 везде, чтбы не испортить старые данные а некоммерческий только на тех получасовках которые загружаем (если надо)
--кстати статус для ручного ввода и рассч данных возможно будет храниться в отдельном столбце

set @ValueType= isnull(@ValueType,0)

set nocount on
set quoted_identifier, ansi_nulls, ansi_warnings, arithabort, concat_null_yields_null, ansi_padding on
set numeric_roundabort off
set transaction isolation level read uncommitted


--print convert(varchar,getdate(),120)+': [usp2_Expl_Import_Arch30Values] начало'

 
declare @TIType int
DECLARE TIType_cursor CURSOR FOR  
select distinct Info_TI.TIType 
from @InputData temp join Info_TI on INfo_TI.TI_ID = temp.TI_ID
OPEN TIType_cursor
FETCH NEXT FROM TIType_cursor into @TIType

WHILE @@FETCH_STATUS = 0  
BEGIN      
	 
	if @titype<>2
	begin



	print convert(varchar,getdate(),120)+': usp2_Expl_Import_Arch30Values cursor TIType='+convert(varchar,@TIType)

	--структура таблиц одинакова за небольшими исключениями

	declare @s_Month_Values_PlanFact varchar(max)='', 
			@s_InsertColumns varchar(max)='DataSource_ID,ContrReplaceStatus,ManualEnterStatus,MainProfileStatus,ManualValidStatus,ManualEnterDateTime,',	
			@s_InsertValues varchar(max)='SOURCE.DataSourceType,0,0,0,0, SOURCE.DispatchDateTime,',
		   
			@s_dataSource2 varchar(max) =' and TARGET.DataSource_ID=SOURCE.DataSourceType ',
			@s_tablename varchar(max)  ='ArchCalcBit_30_Virtual_1'  
			
	if (@TIType>=11 or (@TIType<11 and @TIType<>2))
	begin			

		if (@TIType>=11)
			set @s_tablename ='ArchCalcBit_30_Virtual_'+convert(varchar, (@TIType-10))
		else 
			set @s_tablename ='ArchCalc_30_Virtual'
								
	end
	--для малых ТИ тип=2 отдельная таблица
	if  (@TIType=2)
	begin
		set  @s_tablename ='ArchCalc_30_Month_Values'
		set  @s_Month_Values_PlanFact = ' and TARGET.PlanFact=1 '
		set  @s_InsertColumns='PlanFact,'
		set  @s_InsertValues= '1,' 
		 
		set  @s_dataSource2=''

	end
	 
	--типы одинаковые, поэтому *
	declare @tempTableByTIType [Arch30VirtualValuesType]
	insert into @tempTableByTIType 
	select temp.* from @InputData  temp join Info_TI on INfo_TI.TI_ID = temp.TI_ID
	where 
	TIType= @TIType
	and ((temp.DataSourceType= 0 and @TiType=2) or @TiType <>2)

	--возврат таблицы предполагался для того чтобы пользоваетелю предварительно показать результат..
	--но так как данные в оснвоном импортируются службой, а пользователям ничего не надо.. то это потом..
	--пока сразу Merge в соответствующую таблицу Calc

	declare @sqlUpdate nvarchar(max)=
	'	UPDATE SET	
		TARGET.@VALNAME@01 = COALESCE(SOURCE.Val_01,TARGET.@VALNAME@01),
		TARGET.@VALNAME@02 = COALESCE(SOURCE.Val_02,TARGET.@VALNAME@02),
		TARGET.@VALNAME@03 = COALESCE(SOURCE.Val_03,TARGET.@VALNAME@03),
		TARGET.@VALNAME@04 = COALESCE(SOURCE.Val_04,TARGET.@VALNAME@04),
		TARGET.@VALNAME@05 = COALESCE(SOURCE.Val_05,TARGET.@VALNAME@05),
		TARGET.@VALNAME@06 = COALESCE(SOURCE.Val_06,TARGET.@VALNAME@06),
		TARGET.@VALNAME@07 = COALESCE(SOURCE.Val_07,TARGET.@VALNAME@07),
		TARGET.@VALNAME@08 = COALESCE(SOURCE.Val_08,TARGET.@VALNAME@08),
		TARGET.@VALNAME@09 = COALESCE(SOURCE.Val_09,TARGET.@VALNAME@09),
		TARGET.@VALNAME@10 = COALESCE(SOURCE.Val_10,TARGET.@VALNAME@10),
		TARGET.@VALNAME@11 = COALESCE(SOURCE.Val_11,TARGET.@VALNAME@11),
		TARGET.@VALNAME@12 = COALESCE(SOURCE.Val_12,TARGET.@VALNAME@12),
		TARGET.@VALNAME@13 = COALESCE(SOURCE.Val_13,TARGET.@VALNAME@13),
		TARGET.@VALNAME@14 = COALESCE(SOURCE.Val_14,TARGET.@VALNAME@14),
		TARGET.@VALNAME@15 = COALESCE(SOURCE.Val_15,TARGET.@VALNAME@15),
		TARGET.@VALNAME@16 = COALESCE(SOURCE.Val_16,TARGET.@VALNAME@16),
		TARGET.@VALNAME@17 = COALESCE(SOURCE.Val_17,TARGET.@VALNAME@17),
		TARGET.@VALNAME@18 = COALESCE(SOURCE.Val_18,TARGET.@VALNAME@18),
		TARGET.@VALNAME@19 = COALESCE(SOURCE.Val_19,TARGET.@VALNAME@19),
		TARGET.@VALNAME@20 = COALESCE(SOURCE.Val_20,TARGET.@VALNAME@20),
		TARGET.@VALNAME@21 = COALESCE(SOURCE.Val_21,TARGET.@VALNAME@21),
		TARGET.@VALNAME@22 = COALESCE(SOURCE.Val_22,TARGET.@VALNAME@22),
		TARGET.@VALNAME@23 = COALESCE(SOURCE.Val_23,TARGET.@VALNAME@23),
		TARGET.@VALNAME@24 = COALESCE(SOURCE.Val_24,TARGET.@VALNAME@24),
		TARGET.@VALNAME@25 = COALESCE(SOURCE.Val_25,TARGET.@VALNAME@25),
		TARGET.@VALNAME@26 = COALESCE(SOURCE.Val_26,TARGET.@VALNAME@26),
		TARGET.@VALNAME@27 = COALESCE(SOURCE.Val_27,TARGET.@VALNAME@27),
		TARGET.@VALNAME@28 = COALESCE(SOURCE.Val_28,TARGET.@VALNAME@28),
		TARGET.@VALNAME@29 = COALESCE(SOURCE.Val_29,TARGET.@VALNAME@29),
		TARGET.@VALNAME@30 = COALESCE(SOURCE.Val_30,TARGET.@VALNAME@30),
		TARGET.@VALNAME@31 = COALESCE(SOURCE.Val_31,TARGET.@VALNAME@31),
		TARGET.@VALNAME@32 = COALESCE(SOURCE.Val_32,TARGET.@VALNAME@32),
		TARGET.@VALNAME@33 = COALESCE(SOURCE.Val_33,TARGET.@VALNAME@33),
		TARGET.@VALNAME@34 = COALESCE(SOURCE.Val_34,TARGET.@VALNAME@34),
		TARGET.@VALNAME@35 = COALESCE(SOURCE.Val_35,TARGET.@VALNAME@35),
		TARGET.@VALNAME@36 = COALESCE(SOURCE.Val_36,TARGET.@VALNAME@36),
		TARGET.@VALNAME@37 = COALESCE(SOURCE.Val_37,TARGET.@VALNAME@37),
		TARGET.@VALNAME@38 = COALESCE(SOURCE.Val_38,TARGET.@VALNAME@38),
		TARGET.@VALNAME@39 = COALESCE(SOURCE.Val_39,TARGET.@VALNAME@39),
		TARGET.@VALNAME@40 = COALESCE(SOURCE.Val_40,TARGET.@VALNAME@40),
		TARGET.@VALNAME@41 = COALESCE(SOURCE.Val_41,TARGET.@VALNAME@41),
		TARGET.@VALNAME@42 = COALESCE(SOURCE.Val_42,TARGET.@VALNAME@42),
		TARGET.@VALNAME@43 = COALESCE(SOURCE.Val_43,TARGET.@VALNAME@43),
		TARGET.@VALNAME@44 = COALESCE(SOURCE.Val_44,TARGET.@VALNAME@44),
		TARGET.@VALNAME@45 = COALESCE(SOURCE.Val_45,TARGET.@VALNAME@45),
		TARGET.@VALNAME@46 = COALESCE(SOURCE.Val_46,TARGET.@VALNAME@46),
		TARGET.@VALNAME@47 = COALESCE(SOURCE.Val_47,TARGET.@VALNAME@47),
		TARGET.@VALNAME@48 = COALESCE(SOURCE.Val_48,TARGET.@VALNAME@48), 
		--TARGET.MainProfileStatus= 0,  
		--TARGET.ManualValidStatus= 0, 
		TARGET.ManualEnterDateTime= SOURCE.DispatchDateTime, 
		TARGET.DispatchDateTime= SOURCE.DispatchDateTime, 
		--статусы под вопросом, также под вопросом куда сохранять в CAL ИЛИ VAL
		TARGET.ValidStatus = TARGET.ValidStatus&SOURCE.ExistValueMask | SOURCE.ValidStatus '


	declare @sql nvarchar(max)=''
	
	set @sql=N'MERGE '+@s_tablename+' as TARGET
	using 
	(select temp.* 	from @importedData_param  temp) as SOURCE
	on
	TARGET.TI_ID=SOURCE.TI_ID 
	and TARGET.ChannelType=SOURCE.ChannelType 
	and TARGET.EventDate=SOURCE.EventDate '
	+@s_dataSource2+' '
	+@s_Month_Values_PlanFact+
	'	WHEN MATCHED THEN 
	'+@sqlUpdate+'
	WHEN NOT MATCHED BY TARGET THEN 
	INSERT
	(
	TI_ID,
	EventDate,ChannelType,
	@VALNAME@01,@VALNAME@02,@VALNAME@03,@VALNAME@04,@VALNAME@05,@VALNAME@06,@VALNAME@07,@VALNAME@08,@VALNAME@09,@VALNAME@10,@VALNAME@11,@VALNAME@12,@VALNAME@13,
	@VALNAME@14,@VALNAME@15,@VALNAME@16,@VALNAME@17,@VALNAME@18,@VALNAME@19,@VALNAME@20,@VALNAME@21,@VALNAME@22,@VALNAME@23,@VALNAME@24,@VALNAME@25,@VALNAME@26,
	@VALNAME@27,@VALNAME@28,@VALNAME@29,@VALNAME@30,@VALNAME@31,@VALNAME@32,@VALNAME@33,@VALNAME@34,@VALNAME@35,@VALNAME@36,@VALNAME@37,@VALNAME@38,@VALNAME@39,@VALNAME@40,
	@VALNAME@41,@VALNAME@42,@VALNAME@43,@VALNAME@44,@VALNAME@45,@VALNAME@46,@VALNAME@47,@VALNAME@48,
	ValidStatus,DispatchDateTime,Status,'+@s_InsertColumns +'
	CUS_ID ) 
	values
	(
	SOURCE.TI_ID,
	SOURCE.EventDate, SOURCE.ChannelType, 
	SOURCE.VAL_01, SOURCE.VAL_02, SOURCE.VAL_03, SOURCE.VAL_04, SOURCE.VAL_05, SOURCE.VAL_06, SOURCE.VAL_07, SOURCE.VAL_08, SOURCE.VAL_09, SOURCE.VAL_10, SOURCE.VAL_11, SOURCE.VAL_12, SOURCE.VAL_13, SOURCE.VAL_14, SOURCE.VAL_15, SOURCE.VAL_16, SOURCE.VAL_17, SOURCE.VAL_18, SOURCE.VAL_19, SOURCE.VAL_20, SOURCE.VAL_21, SOURCE.VAL_22, SOURCE.VAL_23, SOURCE.VAL_24, SOURCE.VAL_25, SOURCE.VAL_26, SOURCE.VAL_27, SOURCE.VAL_28, SOURCE.VAL_29, SOURCE.VAL_30, SOURCE.VAL_31, SOURCE.VAL_32, SOURCE.VAL_33, SOURCE.VAL_34, SOURCE.VAL_35, SOURCE.VAL_36, SOURCE.VAL_37, SOURCE.VAL_38, SOURCE.VAL_39, SOURCE.VAL_40, SOURCE.VAL_41, SOURCE.VAL_42, SOURCE.VAL_43, SOURCE.VAL_44, SOURCE.VAL_45, SOURCE.VAL_46, SOURCE.VAL_47, SOURCE.VAL_48, 
	SOURCE.ValidStatus, SOURCE.DispatchDateTime, SOURCE.Status, '+@s_InsertValues+'
	 0
	);
	'

	if (@ValueType=0)
	 set @sql= replace(@sql,'@VALNAME@','VAL_')
	else if (@ValueType=1)
	 set @sql= replace(@sql,'@VALNAME@','CAL_')
	else if (@ValueType=2)
	 set @sql= replace(@sql,'@VALNAME@','FVAL_')
	   

	-- select len(@sql, @sql )
	--insert into @ExistsItems
	exec sp_executesql @sql,N'	
		@importedData_param [dbo].[Arch30VirtualValuesType] readonly',
	 @importedData_param=@tempTableByTIType
	 	 
	--print convert(varchar,getdate(),120)+': usp2_Expl_Import_Arch30Values cursor end'
	 
	 end

   FETCH NEXT FROM TIType_cursor into @TIType
END    
CLOSE TIType_cursor  
DEALLOCATE TIType_cursor  
   
  
end
go


grant EXECUTE on usp2_Expl_Import_Arch30Values to UserImportService
go
grant EXECUTE on usp2_Expl_Import_Arch30Values to UserCalcService
go
grant EXECUTE on usp2_Expl_Import_Arch30Values to UserDeclarator
go
 