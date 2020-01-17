if exists (select 1
          from sysobjects
          where  id = object_id('usp2_Arch_30_Select')
          and type in ('P','PC'))
   drop procedure usp2_Arch_30_Select
go
 
-- ======================================================================================
-- Автор:
--
--		Малышев Игорь
--
-- Дата создания:
--
--		Январь, 2013
--
-- Описание:
--
--		Архивные данные в зависимости от таблицы, сортированно по приоритету источника
--
-- ======================================================================================

create proc [dbo].[usp2_Arch_30_Select]

	@TI_ID int,
	@DateStart datetime,
	@DateEnd datetime,
	@ChannelType tinyint,
	@TypeTable tinyint,
	@DataSourceType tinyint = null, -- Тип источника, если не указывается то в порядке приоритета
	@ClosedPeriod_ID uniqueidentifier = null, --Закрытый период, если не указан, читаем из таблицы расчетного профиля
	@isCoeffEnabled bit = null,
	@isValidateOtherDataSource bit = null, -- Нужны достоверности по остальным источникам
	@IsReadCalculatedValues bit, --Читаем расчетные данные
	@TP_ID int = null, -- По этой ТП
	@isReturnPreviousDispatchDateTime bit = 0, --Возвращать DispatchDateTime предыдущего поступления данных
	@IsChannelsInverted bit = 0, --Признак инвертированности каналов
	@isCa bit = 0 --Признак контрагента
	

as
begin
	
	DECLARE 
		@ParmDefinition NVARCHAR(1000),
		@SQLString NVARCHAR(4000),
		@SqlTable NVARCHAR(200),
		@SqlSelect NVARCHAR(4000),
		@SqlSelect1 NVARCHAR(4000),
		@SqlSelect2 NVARCHAR(4000),
		@SqlRequstAdditionalField NVARCHAR(1400),
		@SqlOrderByFields NVARCHAR(200),
		@SqlAdditionalFieldVirtual NVARCHAR(1200),
		@SqlWhereAdditional NVARCHAR(1000),
		@sqlTableNumber NVARCHAR(3),
		@dataSource_ID int,
		@idName NVARCHAR(14),
		@date date;


	set @date = cast(@DateStart as date);
	set @idName = 't1.TI_ID'

	set @SqlOrderByFields = ' order by TI_ID, EventDate, ChannelType, DataSource_ID';

	set @SqlSelect1 = 'select ISNULL(v.EventDate, t1.EventDate) as EventDate, ISNULL(v.ValidStatus, t1.ValidStatus) as ValidStatus,
				v.val_01,v.val_02,v.val_03,v.val_04,
				v.val_05,v.val_06,v.val_07,v.val_08,
				v.val_09,v.val_10,v.val_11,v.val_12,
				v.val_13,v.val_14,v.val_15,v.val_16,
				v.val_17,v.val_18,v.val_19,v.val_20,
				v.val_21,v.val_22,v.val_23,v.val_24,
				v.val_25,v.val_26,v.val_27,v.val_28,
				v.val_29,v.val_30,v.val_31,v.val_32,
				v.val_33,v.val_34,v.val_35,v.val_36,
				v.val_37,v.val_38,v.val_39,v.val_40,
				v.val_41,v.val_42,v.val_43,v.val_44,
				v.val_45,v.val_46,v.val_47,v.val_48,';

	set @SqlSelect2 = 'select t1.EventDate, t1.ValidStatus, 
				t1.val_01,t1.val_02,t1.val_03,t1.val_04,
				t1.val_05,t1.val_06,t1.val_07,t1.val_08,
				t1.val_09,t1.val_10,t1.val_11,t1.val_12,
				t1.val_13,t1.val_14,t1.val_15,t1.val_16,
				t1.val_17,t1.val_18,t1.val_19,t1.val_20,
				t1.val_21,t1.val_22,t1.val_23,t1.val_24,
				t1.val_25,t1.val_26,t1.val_27,t1.val_28,
				t1.val_29,t1.val_30,t1.val_31,t1.val_32,
				t1.val_33,t1.val_34,t1.val_35,t1.val_36,
				t1.val_37,t1.val_38,t1.val_39,t1.val_40,
				t1.val_41,t1.val_42,t1.val_43,t1.val_44,
				t1.val_45,t1.val_46,t1.val_47,t1.val_48,';

	set @SqlWhereAdditional = '';
	set @SqlRequstAdditionalField = ', ISNULL(DataSource_ID, 0) as DataSource_ID';

	set @SqlAdditionalFieldVirtual = ' ,t1.cal_01,t1.cal_02,t1.cal_03,t1.cal_04,
				t1.cal_05,t1.cal_06,t1.cal_07,t1.cal_08,
				t1.cal_09,t1.cal_10,t1.cal_11,t1.cal_12,
				t1.cal_13,t1.cal_14,t1.cal_15,t1.cal_16,
				t1.cal_17,t1.cal_18,t1.cal_19,t1.cal_20,
				t1.cal_21,t1.cal_22,t1.cal_23,t1.cal_24,
				t1.cal_25,t1.cal_26,t1.cal_27,t1.cal_28,
				t1.cal_29,t1.cal_30,t1.cal_31,t1.cal_32,
				t1.cal_33,t1.cal_34,t1.cal_35,t1.cal_36,
				t1.cal_37,t1.cal_38,t1.cal_39,t1.cal_40,
				t1.cal_41,t1.cal_42,t1.cal_43,t1.cal_44,
				t1.cal_45,t1.cal_46,t1.cal_47,t1.cal_48,ContrReplaceStatus, t1.DispatchDateTime, ManualEnterStatus, ManualValidStatus';

	if (@TypeTable > 10) set @sqlTableNumber = ltrim(str(@TypeTable - 10,2));

	--Указываем источник
	if (@DataSourceType is not null and @DataSourceType <> 0 and @isValidateOtherDataSource = 0 and @TypeTable <> 2 and @isCa = 0) begin
			--Иточник явно указан
			set @dataSource_ID = (select top 1 DataSource_ID from Expl_DataSource_List where DataSourceType = @DataSourceType);
			set @SqlWhereAdditional = ' and t1.DataSource_ID = @dataSource_ID'	
	end;

	set @SqlSelect = @SqlSelect2;

	if (@ClosedPeriod_ID is not null and @isCa = 0) begin --Чтение закрытого периода
			if (@TypeTable > 10) set @SqlTable = 'ArchCalcBit_30_Closed_' + @sqlTableNumber;
			else set @SqlTable = 'ArchCalc_30_Virtual_Closed';
			set @SqlRequstAdditionalField += @SqlAdditionalFieldVirtual
			set @SqlWhereAdditional = ' and t1.ClosedPeriod_ID = @ClosedPeriod_ID'	
	end else begin
			
		if (@TypeTable = 2 or @isCa = 1) begin 

			set @SqlRequstAdditionalField = ' ,null as cal_01,null as cal_02,null as cal_03,null as cal_04,
			null as cal_05,null as cal_06,null as cal_07,null as cal_08,
			null as cal_09,null as cal_10,null as cal_11,null as cal_12,
			null as cal_13,null as cal_14,null as cal_15,null as cal_16,
			null as cal_17,null as cal_18,null as cal_19,null as cal_20,
			null as cal_21,null as cal_22,null as cal_23,null as cal_24,
			null as cal_25,null as cal_26,null as cal_27,null as cal_28,
			null as cal_29,null as cal_30,null as cal_31,null as cal_32,
			null as cal_33,null as cal_34,null as cal_35,null as cal_36,
			null as cal_37,null as cal_38,null as cal_39,null as cal_40,
			null as cal_41,null as cal_42,null as cal_43,null as cal_44,
			null as cal_45,null as cal_46,null as cal_47,null as cal_48,cast(0 as bigint) as ContrReplaceStatus, 0 as DataSource_ID, DispatchDateTime, 
			cast(0 as bigint) as ManualEnterStatus, cast(0 as bigint) as ManualValidStatus';

			--Малые ТИ читаем только из старой таблицы
			if (@TypeTable = 2) set @SqlTable = 'ArchCalc_30_Month_Values'; 
			--КА
			else begin 
				set @idName = 't1.ContrTI_ID'
				set @SqlTable = 'ArchComm_Contr_30_Import_From_XML';
			end

		end else if (@TypeTable > 10 ) begin
			--set @SqlTable = 'ArchCalcBit_30_Virtual_' + @sqlTableNumber;
			if (@IsReadCalculatedValues = 0 and @DataSourceType = 0) begin 
			--При таких настройках запрашиваем данные из таблиц автоматизированного сбора
				set @SqlTable = 'ArchBit_30_Values_' + @sqlTableNumber;
				set @SqlWhereAdditional = '';
				set @SqlRequstAdditionalField = ' ,cast(0 as bigint) as ContrReplaceStatus, 0 as DataSource_ID, DispatchDateTime,cast(0 as bigint) as ManualEnterStatus, cast(0 as bigint) as ManualValidStatus ';
				set @SqlOrderByFields = ' order by TI_ID, EventDate, ChannelType';
			end else begin
				set @SqlTable = 'ArchCalcBit_30_Virtual_' + @sqlTableNumber;
				set @SqlRequstAdditionalField += @SqlAdditionalFieldVirtual
			end

		end else begin
			set @SqlTable = 'ArchCalc_30_Virtual';
			set @SqlRequstAdditionalField += @SqlAdditionalFieldVirtual
			
		end;
	end;

if (@isReturnPreviousDispatchDateTime = 1 and @isCa = 0) begin

	--Возвращаем дату/время чтения данных за предыдущие сутки (нужно для округления)
	set @SqlRequstAdditionalField = @SqlRequstAdditionalField + N', (select Max(DispatchDateTime) from ' + @SqlTable + ' where TI_ID = t1.TI_ID and EventDate = DATEADD(day, -1, t1.EventDate) and ChannelType=t1.ChannelType ' 
	+ case when @TypeTable = 2 or @isCa = 1 or (@IsReadCalculatedValues = 0 and @DataSourceType = 0) then ' ' else 'and DataSource_ID = t1.DataSource_ID ' end +
	+ @SqlWhereAdditional + ') as PreviousDispatchDateTime '
	
end else begin
	set @SqlRequstAdditionalField = @SqlRequstAdditionalField + N', NULL as PreviousDispatchDateTime '
end

SET @ParmDefinition = N'@TI_ID int,@DateStart DateTime,@DateEnd DateTime,@ChannelType tinyint,@isCoeffEnabled bit,@DataSourceType tinyint,@ClosedPeriod_ID uniqueidentifier,@isValidateOtherDataSource bit, @TP_ID int, @dataSource_ID int, @IsChannelsInverted bit, @date date'


SET @SQLString = @SqlSelect + ' 1 as Coeff ' + @SqlRequstAdditionalField + ',' + @idName + ' as TI_ID,@ChannelType as ChannelType,c.StartChannelStatus,c.FinishChannelStatus' +
	' from ' + @SqlTable + ' t1 
	outer apply dbo.usf2_ArchCalcChannelInversionStatus(@TI_ID, @ChannelType, @DateStart, @DateEnd, @IsChannelsInverted) c
	where (' + @idName + '=@TI_ID and t1.EventDate between @date and @DateEnd 
	and t1.ChannelType=c.ChannelType and t1.EventDate<=c.FinishChannelStatus and DATEADD(minute, 1439, t1.EventDate)>=c.StartChannelStatus) '+ @SqlWhereAdditional
	+ @SqlOrderByFields

	--if (@TypeTable = 2 or @isCa = 1) print @SQLString;

	EXEC sp_executesql @SQLString, @ParmDefinition,@TI_ID, @DateStart, @DateEnd, @ChannelType, @isCoeffEnabled, @DataSourceType, @ClosedPeriod_ID, 
		@isValidateOtherDataSource, @TP_ID, @dataSource_ID,@IsChannelsInverted, @date;
end

go
   grant EXECUTE on usp2_Arch_30_Select to [UserCalcService]
go