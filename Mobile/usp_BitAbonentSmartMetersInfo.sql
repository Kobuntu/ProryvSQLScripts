if exists (select 1
          from sysobjects
          where  id = object_id('usp2_BitAbonentSmartMetersInfo')
          and type in ('P','PC'))
   drop procedure usp2_BitAbonentSmartMetersInfo
go

SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

-- ======================================================================================
-- Автор:
--
--		Малышев Игорь
--
-- Дата создания:
--
--		Ноябрь, 2011
--
-- Описание:
--
--		Собираем информацию по абоненту, его расходы за последние 6 месяцев 
--		и последние показания счетчиков для мобильной версии АРМа
--
-- ======================================================================================

create proc [dbo].[usp2_BitAbonentSmartMetersInfo]

	@CurrentDateTime datetime,
	@BitAbonent_ID BITABONENT_ID_TYPE
	
	
as
begin

	set nocount on
	set quoted_identifier, ansi_nulls, ansi_warnings, arithabort, concat_null_yields_null, ansi_padding on
	set numeric_roundabort off
	set transaction isolation level read uncommitted
	
	declare 
	
	-------------------------------------------------------------------------------
	--Информация по абоненту
	@BitAbonentSurname varchar(255),
	@BitAbonentCode varchar(128),
	@BitAbonentName varchar(255),
	@BitAbonentMiddleName varchar(255),
	@TI_ID TI_ID_TYPE,
	@titype tinyint,
	@dt dateTime,
	@ConsumerDataSource_ID tinyint; --Идентификатор источника потребителя
	
	Select top 1  @BitAbonentCode=BitAbonentCode, @BitAbonentSurname = al.BitAbonentSurname, @BitAbonentMiddleName = BitAbonentMiddleName, @BitAbonentName = BitAbonentName,
	@TI_ID = alti.TI_ID
	from dbo.InfoBit_Abonents_List al
	left join dbo.InfoBit_Abonents_To_TI alti 
	on alti.BitAbonent_ID = al.BitAbonent_ID
	where al.BitAbonent_ID = @BitAbonent_ID 
	and (alti.StartDateTime <= @CurrentDateTime and (alti.FinishDateTime is null OR @CurrentDateTime <= alti.FinishDateTime))
	order by alti.StartDateTime desc
	
	select distinct @BitAbonentSurname as BitAbonentSurname, @BitAbonentMiddleName as BitAbonentMiddleName
	,@BitAbonentName as BitAbonentName, @BitAbonentCode as BitAbonentCode, TIName as Flat,
	ps.StringName as House,
	h3.StringName as Street,
	h2.StringName as Region,
	h1.StringName as City,
	ti.TIType as  TIType,
	ti.AIATSCode,ti.AOATSCode,ti.RIATSCode,ti.ROATSCode,ti.TI_ID
	into #abonentTable
	from Info_TI ti
	left join Dict_PS ps on ps.PS_ID = ti.PS_ID
	left join Dict_HierLev3 h3 on h3.HierLev3_ID = ps.HierLev3_ID
	left join Dict_HierLev2 h2 on h2.HierLev2_ID = h3.HierLev2_ID
	left join Dict_HierLev1 h1 on h1.HierLev1_ID = h2.HierLev1_ID
	where TI_ID = @TI_ID 
	
	set @titype = (select TIType from #abonentTable);
	
	--Ручные данные всегда лежат в данных потребителя, вытаскиваем идентификатор
	set @ConsumerDataSource_ID = ISNULL((select top 1 DataSource_ID from Expl_DataSource_List where DataSourceType = 3), 3);

	--Информация по абоненту
	select BitAbonentSurname, BitAbonentName, BitAbonentMiddleName, BitAbonentCode
	, '' as BitAbonentPhoneNumber, '' as BitAbonentEmail,City + ' ' + Region as BitAbonentAddressLine1
	,House + ', ' + Flat as  BitAbonentAddressLine2, 'Нет льгот' as Benefit, TI_ID  from #abonentTable
	-------------------------------------------------------------------------------
	--Информация по тарифу абонента
	declare 
	@Tariff_ID TARIFF_ID_TYPE,
	@monthMask int, @dayMask int, @dowMask int;
	
	set @monthMask = DATEPART(month, @CurrentDateTime) - 1; --Для маски по месяцу
	set @dayMask = DATEPART(day, @CurrentDateTime) - 1; --Для маски по дню
	set @dowMask = DATEPART(DW, @CurrentDateTime) - 1; --Для маски по дням недели
	
	select top  1 @Tariff_ID = Tariff_ID 
	from dbo.DictTariffs_ToTI tti
	where tti.TI_ID = @TI_ID 
	and (tti.StartDateTime <= @CurrentDateTime and (tti.FinishDateTime is null OR @CurrentDateTime <= tti.FinishDateTime))
	order by tti.StartDateTime desc
	
	select StringName as TariffName, TariffGroupName, TariffTypeAddStringName as TariffCategoryName
	,City as TariffRegion from dbo.DictTariffs_Tariffs dt, #abonentTable
	where dt.Tariff_ID = @Tariff_ID
	
	--Информация по тарифным зонам и по ценам
	select distinct dz.TariffZone_ID as TariffZone_ID, dz.StringName as ZoneFullName
	,ChannelType1, ChannelType2, ChannelType3, ChannelType4
	,case when PATINDEX('%полупик%', dz.StringName) > 0 then 'ПП' else SUBSTRING(dz.StringName,1,1) end as ZoneShortName
	into #zoneTable
	from dbo.DictTariffs_Zones dz
	where dz.Tariff_ID = @Tariff_ID 
	and (dz.StartDateTime <= @CurrentDateTime and (dz.FinishDateTime is null OR @CurrentDateTime <= dz.FinishDateTime))
	
	create table #DateCross
	(
		dt datetime
	);

	set @CurrentDateTime = floor(cast(@CurrentDateTime as float));
	set @CurrentDateTime = DATEADD(day,-DAY(@CurrentDateTime)+1, @CurrentDateTime);
	set @dt = DATEADD(month,-6, @CurrentDateTime);

	while @dt <= @CurrentDateTime
		begin
			insert #DateCross values (@dt) 
			set @dt = dateadd(month, 1, @dt)
		end

	--select * from #DateCross;	
	
	select dt.dt, dz.TariffZone_ID, dz.ZoneFullName as ZoneFullName, dr.Price as Price, dz.ChannelType1 as ChannelType
	,ZoneShortName
	into #zonePrice
	from #DateCross dt, #zoneTable dz
	left join dbo.DictTariffs_Rates dr on dr.TariffZone_ID = dz.TariffZone_ID 
	and (dr.StartDateTime <= @CurrentDateTime and (dr.FinishDateTime is null OR @CurrentDateTime <= dr.FinishDateTime))
	
	select distinct zp.TariffZone_ID, zp.ZoneFullName, zp.Price, zp.ZoneShortName, zp.ChannelType  from #zonePrice zp;
	
	--Информация по интервалам действия зон
	select distinct dz.TariffZone_ID, zti.MonthMask, zti.DayMask, zti.DowMask, zti.HalfHoursMask 
	from #zoneTable dz
	left join dbo.DictTariffs_Zones_Time_Intervals zti on zti.TariffZone_ID = dz.TariffZone_ID
	where zti.StartDateTime <= @CurrentDateTime and (zti.FinishDateTime is null OR @CurrentDateTime <= zti.FinishDateTime)
	-- Маскируем, отображаем только период для текущего месяца, текущего дня, текущего дня недели
	and dbo.sfclr_Utils_BitOperations2(zti.MonthMask,@monthMask) = 1 
	and dbo.sfclr_Utils_BitOperations2(zti.DayMask,@dayMask) = 1 
	and dbo.sfclr_Utils_BitOperations2(zti.DowMask,@dowMask) = 1 
	
	-------------------------------------------------------------------------------
	--Архив последних значений
	--Определяемся с диапазоном
	declare 
	@DateStart DateTime,
	@DateEnd DateTime,
	@ChannelType1 TI_CHANNEL_TYPE,
	@ChannelType2 TI_CHANNEL_TYPE,
	@ChannelType3 TI_CHANNEL_TYPE,
	@ChannelType4 TI_CHANNEL_TYPE,
	@AIATSCode int, @AOATSCode int, @RIATSCode int, @ROATSCode int,
	@tableName varchar(255);
	
	create table #tmp2
	(
		ti_id int, 
		channelType tinyint,
		EventDateTime DateTime, 
		[Data] float, 
		[Row] int
	);
	
	create table #tmp3
	(
		СhannelType tinyint,
		EventDateTime DateTime, 
		[Data] float 
	);

	create table #tmp4
	(
		СhannelType tinyint,
		EventDateTime DateTime, 
		[ManualEnterData] float 
	);
	
	DECLARE @ParmDefinition NVARCHAR(1000);
	SET @ParmDefinition = N'@ti_id int,@ChannelType1 tinyint, @DateStart datetime, @DateEnd datetime';
	
	DECLARE @ParmDefinition1 NVARCHAR(1000);
	SET @ParmDefinition1 = N'@ti_id int,@ChannelType1 tinyint,@ConsumerDataSource_ID tinyint';
	
	DECLARE @SQLString NVARCHAR(4000);
	
	IF @titype>10 BEGIN
		set @tableName = 'dbo.ArchCalcBit_Integrals_Virtual_' + + ltrim(str(@TIType - 10,2));
	END ELSE BEGIN
		set @tableName = 'dbo.ArchCalc_Integrals_Virtual';
	END;
	
	set @DateStart = DATEADD(month,-6, @CurrentDateTime);
	set @DateEnd = DATEADD(month,1, @CurrentDateTime); --Берем последние 6 месяцев
	
	select @AIATSCode=AIATSCode,@AOATSCode=AOATSCode,@RIATSCode=RIATSCode,@ROATSCode=ROATSCode from #abonentTable;
	
	declare t cursor LOCAL STATIC FORWARD_ONLY READ_ONLY for select ChannelType1, ChannelType2, ChannelType3, ChannelType4 from #zoneTable
	open t;
	FETCH NEXT FROM t into @ChannelType1, @ChannelType2, @ChannelType3, @ChannelType4
	WHILE @@FETCH_STATUS = 0
	BEGIN
	
		--set @ChannelType1 = dbo.usf2_ReverseTariffChannel(0, @ChannelType1, @AIATSCode,@AOATSCode,@RIATSCode,@ROATSCode, @TI_ID, @DateStart, @DateEnd);
		--Только источник автоматизированных данных

		--Значения за последние полгода
		SET @SQLString = 'insert into #tmp2
		select  arh.ti_id, arh.channelType, arh.EventDateTime, [Data]
		,row_number() over (PARTITION BY arh.ti_id, arh.channelType
		,DatePart(year,arh.EventDateTime), DatePart(month,arh.EventDateTime) order by arh.ti_id, arh.channelType
		,DatePart(year,arh.EventDateTime), DatePart(month,arh.EventDateTime)) as [Row] 
		from  ' + @tableName + ' arh
		where arh.TI_ID = @TI_ID and arh.IntegralType = 0 and (arh.ChannelType = @ChannelType1) and arh.EventDateTime between @DateStart and @DateEnd and DataSource_ID = 0 and Data >= 0' 
		
		EXEC sp_executesql @SQLString, @ParmDefinition, @ti_id ,@ChannelType1,@DateStart, @DateEnd;
		
		--Последние значения автоматизированного ввода
		SET @SQLString = N'insert into #tmp3 select top 1 ChannelType, EventDateTime, Data
			from ' + @tableName + ' where TI_ID = @TI_ID and ChannelType = @ChannelType1 and DataSource_ID = 0 and Data >= 0
			order by EventDateTime desc';
		EXEC sp_executesql @SQLString, @ParmDefinition1,@TI_ID, @ChannelType1, @ConsumerDataSource_ID;

		--Последние значения ручного ввода пользователя
		SET @SQLString = N'insert into #tmp4 select top 1 ChannelType, EventDateTime, ManualEnterData
			from ' + @tableName + ' where TI_ID = @TI_ID and ChannelType = @ChannelType1 and DataSource_ID = @ConsumerDataSource_ID and ManualEnterData is not null
			order by EventDateTime desc';
		EXEC sp_executesql @SQLString, @ParmDefinition1,@TI_ID, @ChannelType1, @ConsumerDataSource_ID;
		
	FETCH NEXT FROM t into @ChannelType1, @ChannelType2, @ChannelType3, @ChannelType4
	end;
	CLOSE t
	DEALLOCATE t;
	
	with cte as
	(
		select  ti_id, channelType, EventDateTime, [Data],
		ROW_NUMBER() OVER (ORDER BY  channelType, EventDateTime) as [Row]
		from #tmp2 
		where [Row] = 1
	)
	
	--Расходы за месяц
	select #zonePrice.ZoneFullName, DatePart(year,#zonePrice.dt) as [Year], DatePart(MONTH,#zonePrice.dt) as [Month]
	, ISNULL(arch.MonthZoneValue, 0) as MonthZoneValue, ISNULL(arch.MonthZoneValue * #zonePrice.Price, 0) as PriceValue
	, arch.StartMonthIntegralValue, arch.EndMonthIntegralValue
	from #zonePrice
	left join (
		select c1.ChannelType, case when DatePart(day, c2.EventDateTime)<3 then DateAdd(month,-1, c2.EventDateTime) else c2.EventDateTime end as MonthYear
		,(c2.[Data] * dbo.usf2_Info_CoeffTransformators(c2.TI_ID, c2.EventDateTime, 0, 1) - c1.[Data] * dbo.usf2_Info_CoeffTransformators(c1.TI_ID, c1.EventDateTime, 0, 1)) / 1000 as MonthZoneValue --Разница с предыдущим месяцем
		,c1.Data as StartMonthIntegralValue, c2.Data as EndMonthIntegralValue
		from cte c1
		left join  cte c2 
		on c1.[TI_ID] = c2.[TI_ID]
		and c1.[ChannelType] = c2.[ChannelType]
		and c1.[Row] = c2.[Row] - 1 
		where  DateDiff(day, c1.EventDateTime,c2.EventDateTime) < 33 and (DATEPART(day, c2.EventDateTime) < 3 or DATEPART(day, c2.EventDateTime) >= 27) --Фильтр по которому неправильные данные не отображаем
		and (c2.Data - c1.Data) > 0 --переход через 0 отдельно обыгрываем
	) arch on arch.channelType = #zonePrice.ChannelType and arch.MonthYear = #zonePrice.dt
	
	order by #zonePrice.dt, arch.channelType;
	
	-------------------------------------------------------------------------------
	--Последние показания счетчиков
	select #zoneTable.ZoneShortName, #tmp3.EventDateTime, #tmp3.Data / 1000 as Data
	from #tmp3
	left join #zoneTable on #tmp3.СhannelType = #zoneTable.ChannelType1;

	--Последние ручные показания счетчиков
	select #zoneTable.ZoneShortName, #tmp4.EventDateTime, #tmp4.ManualEnterData / 1000 as Data
	from #tmp4
	left join #zoneTable on #tmp4.СhannelType = #zoneTable.ChannelType1;

	
	drop table #zoneTable
	drop table #zonePrice
	drop table #tmp2;
	drop table #tmp3;
	drop table #tmp4;
	
end

go
   grant EXECUTE on usp2_BitAbonentSmartMetersInfo to [UserCalcService]
go
