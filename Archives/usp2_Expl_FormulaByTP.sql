if exists (select 1
          from sysobjects
          where  id = object_id('usp2_Expl_FormulaByTP')
          and type in ('P','PC'))
   drop procedure usp2_Expl_FormulaByTP
go

if exists (select 1
          from sysobjects
          where  id = object_id('usp2_Info_LinkedFormulasTp')
          and type in ('P','PC'))
   drop procedure usp2_Info_LinkedFormulasTp
GO

if exists (select 1
          from sysobjects
          where  id = object_id('usp2_ExplDoc_Residues_TP')
          and type in ('P','PC'))
   drop procedure usp2_ExplDoc_Residues_TP
go

--Обновляем тип
--Удаляем если есть
IF  EXISTS (SELECT * FROM sys.types st JOIN sys.schemas ss ON st.schema_id = ss.schema_id WHERE st.name = N'TpChannelType' AND ss.name = N'dbo')
DROP TYPE [dbo].[TpChannelType]
-- Пересоздаем заново
CREATE TYPE [dbo].[TpChannelType] AS TABLE 
(
	TP_ID int NOT NULL, 
	ChannelType tinyint NOT NULL,
	IsMoneyOurSide bit NOT NULL,
	ClosedPeriod_ID uniqueidentifier NULL,
	Section_ID int
)
go
grant EXECUTE on TYPE::TpChannelType to [UserCalcService]
go
grant EXECUTE on TYPE::TpChannelType to [UserSlave61968Service]
go

-- ======================================================================================
-- Автор:
--
--		Малышев Игорь
--
-- Дата создания:
--
--		Май, 2013
--
-- Описание:
--
--		Читаем список формул по списку ТП
--
-- ======================================================================================

create proc [dbo].[usp2_Expl_FormulaByTP]
(	
	@tpArray TpChannelType READONLY, --Точки поставок
	@dtStart datetime,
	@dtEnd datetime,
	@typeArray varchar(4000),
	@formulas varchar(4000) = null --Формулы, которые надо в любом случае вернуть
)
AS
BEGIN 
	set nocount on
	set quoted_identifier, ansi_nulls, ansi_warnings, arithabort, concat_null_yields_null, ansi_padding on
	set numeric_roundabort off
	set transaction isolation level read uncommitted

	select Items as formulaType 
	into #types
	from usf2_Utils_Split(@typeArray, ',')

	select Item
	into #formulas
	from usf2_Utils_SplitString(@formulas, ',')
	
	--Формулы, для указанных ТП
	--Открытый 
	select tps.*, z.MsTimeZoneID
	into #result1
	from 
	(
		--Наша сторона
		select fl.TP_ID, fl.Formula_UN, fl.ChannelType, ForAutoUse, FormulaName, FormulaType_ID, StartDateTime, FinishDateTime, cast(null as uniqueidentifier) as ClosedPeriod_ID, 
		cast(1 as bit) as IsOurSide, HighLimit, LowerLimit, tp.Section_ID, 
		fl.ApplyDateTime as LatestApplyDateTimeFormulaList, fd.ApplyDateTime as LatestApplyDateTimeFormulaListDescription, fl.UnitDigit
		from Info_TP2_OurSide_Formula_List fl
		join @tpArray tp on tp.TP_ID = fl.TP_ID and tp.ChannelType = fl.ChannelType
		cross apply 
		(
			select top 1 Max(ApplyDateTime) as ApplyDateTime from Info_TP2_OurSide_Formula_Description fd where fd.Formula_UN = fl.Formula_UN
		) fd
		where ClosedPeriod_ID is null and tp.IsMoneyOurSide = 1 and ForAutoUse <> 0 and FormulaType_ID in (select formulaType from #types)
		and StartDateTime<=@dtEnd and (FinishDateTime is null or FinishDateTime > @dtStart)
		union all
		--Сторона КА
		select fl.TP_ID, fl.Formula_UN, fl.ChannelType, ForAutoUse, FormulaName, FormulaType_ID, StartDateTime, FinishDateTime, cast(null as uniqueidentifier) as ClosedPeriod_ID, 
		cast(0 as bit) as IsOurSide, HighLimit, LowerLimit, tp.Section_ID,
		fl.ApplyDateTime as LatestApplyDateTimeFormulaList, fd.ApplyDateTime as LatestApplyDateTimeFormulaListDescription, fl.UnitDigit
		from Info_TP2_Contr_Formula_List fl
		join @tpArray tp on tp.TP_ID = fl.TP_ID and tp.ChannelType = fl.ChannelType
		join Info_TP2_Contr_Formula_Description fd on fd.Formula_UN = fl.Formula_UN
		where ClosedPeriod_ID is null and tp.IsMoneyOurSide = 0 and ForAutoUse <> 0 and FormulaType_ID in (select formulaType from #types)
		and StartDateTime<=@dtEnd and (FinishDateTime is null or FinishDateTime > @dtStart)
		union all
		--Закрытый, наша сторона
		select fl.TP_ID, fl.Formula_UN, fl.ChannelType, ForAutoUse, FormulaName, FormulaType_ID, StartDateTime, FinishDateTime, fl.ClosedPeriod_ID, 
		cast(1 as bit) as IsOurSide, HighLimit, LowerLimit, tp.Section_ID,
		NULL as LatestApplyDateTimeFormulaList, NULL as LatestApplyDateTimeFormulaListDescription, null as UnitDigit
		from Info_TP2_OurSide_Formula_List_Closed fl
		join @tpArray tp on tp.TP_ID = fl.TP_ID and tp.ChannelType = fl.ChannelType and tp.ClosedPeriod_ID = fl.ClosedPeriod_ID
		where tp.ClosedPeriod_ID is not null and tp.IsMoneyOurSide = 1  and ForAutoUse <> 0 and FormulaType_ID in (select formulaType from #types)
		and StartDateTime<=@dtEnd and (FinishDateTime is null or FinishDateTime > @dtStart)
		union all
		--Формулы указанные напрямую наша сторона
		select fl.TP_ID, fl.Formula_UN, fl.ChannelType, ForAutoUse, FormulaName, FormulaType_ID, StartDateTime, FinishDateTime, cast(null as uniqueidentifier) as ClosedPeriod_ID, 
		cast(1 as bit) as IsOurSide, HighLimit, LowerLimit, tp.Section_ID,
		fl.ApplyDateTime as LatestApplyDateTimeFormulaList, fd.ApplyDateTime as LatestApplyDateTimeFormulaListDescription, fl.UnitDigit
		from Info_TP2_OurSide_Formula_List fl
		join #formulas f on f.Item = fl.Formula_UN
		cross apply 
		(
			select top 1 Max(ApplyDateTime) as ApplyDateTime from Info_TP2_OurSide_Formula_Description fd where fd.Formula_UN = fl.Formula_UN
		) fd
		cross apply
		(
		 select top 1 Section_ID from Info_Section_Description2
		 where TP_ID = fl.TP_ID
		) tp
		union all
		--Формулы указанные напрямую сторона КА
		select fl.TP_ID, fl.Formula_UN, fl.ChannelType, ForAutoUse, FormulaName, FormulaType_ID, StartDateTime, FinishDateTime, cast(null as uniqueidentifier) as ClosedPeriod_ID, 
		cast(0 as bit) as IsOurSide, HighLimit, LowerLimit, tp.Section_ID,
		fl.ApplyDateTime as LatestApplyDateTimeFormulaList, fd.ApplyDateTime as LatestApplyDateTimeFormulaListDescription, fl.UnitDigit
		from Info_TP2_Contr_Formula_List fl
		join #formulas f on f.Item = fl.Formula_UN
		cross apply 
		(
			select top 1 Max(ApplyDateTime) as ApplyDateTime from Info_TP2_Contr_Formula_Description fd where fd.Formula_UN = fl.Formula_UN
		) fd
		cross apply
		(
		 select top 1 Section_ID from Info_Section_Description2
		 where TP_ID = fl.TP_ID
		) tp
	) tps
	join Info_Section_Description2 sd on sd.TP_ID = tps.TP_ID
	join Info_Section_List sl on sl.Section_ID = sd.Section_ID
	left join Dict_TimeZone_Zones z on z.TimeZone_ID = sl.TimeZone_ID

	--Первый результат
	select * from #result1
	order by TP_ID, ChannelType, FormulaType_ID, StartDateTime

	--Перечень входящих формул, для наших формул

	create table #result2
	(
		InnerLevel int,
		Formula_UN varchar(22),
		StringNumber int,
		OperBefore nvarchar(255),
		UsedFormula_UN varchar(22),
		TI_ID int,
		ChannelType tinyint, 
		TP_ID int,
		Section_ID int,
		ContrTI_ID int,
		OperAfter nvarchar(255), 
		FormulaName nvarchar(255),
		ForAutoUse tinyint,
		IsIntegral bit,
		StartDateTime DateTime, 
		FinishDateTime DateTime NULL, 
		FormulaType_ID tinyint,
		ClosedPeriod_ID uniqueidentifier,
		MainFormula_UN varchar(22),
		FormulaConstant_UN varchar(22),
		UnitDigit bigint NULL,
	)

	declare @Formula_UN varchar(22), @IsOurSide bit, @ClosedPeriod_ID uniqueidentifier;
	declare t cursor LOCAL STATIC FORWARD_ONLY READ_ONLY for select distinct Formula_UN, IsOurSide, ClosedPeriod_ID from #result1
	open t;
	FETCH NEXT FROM t into @Formula_UN, @IsOurSide, @ClosedPeriod_ID

	WHILE @@FETCH_STATUS = 0
	BEGIN
		if (@IsOurSide = 1)	insert into #result2 exec dbo.usp2_Info_FormulaSelectFSK @Formula_UN, @DTStart, @DTEnd, @ClosedPeriod_ID
		else insert into #result2 exec dbo.usp2_Info_FormulaSelectContr @Formula_UN, @DTStart, @DTEnd
	
		FETCH NEXT FROM t into @Formula_UN, @IsOurSide, @ClosedPeriod_ID
	END;

	CLOSE t
	DEALLOCATE t

	select * from #result2;

	drop table #types
	drop table #formulas
	drop table #result1
	drop table #result2

END
go
   grant EXECUTE on usp2_Expl_FormulaByTP to [UserCalcService]
go

-- ======================================================================================
-- Автор:
--
--		Малышев Игорь
--
-- Дата создания:
--
--		Март, 2015
--
-- Описание:
--
--		Выборка связанных между собой формул по ТП
--
-- ======================================================================================

create proc [dbo].[usp2_Info_LinkedFormulasTp]
(	
	@tps TpChannelType READONLY, --Точки поставок
	@dtStart DateTime = null, --Расчетный период для фильтра по ценовым категориям
	@dtEnd DateTime = null, --Расчетный период для фильтра по ценовым категориям
	@isSelectFormulasParams bit = 0,
	@typeArray varchar(4000)
)
AS
BEGIN 
	set nocount on
	set quoted_identifier, ansi_nulls, ansi_warnings, arithabort, concat_null_yields_null, ansi_padding on
	set numeric_roundabort off
	set transaction isolation level read uncommitted

	select Items as formulaType 
	into #types
	from usf2_Utils_Split(@typeArray, ',')

	select lf.*, tp.Section_ID into #tmp
	from @tps tp
	join Info_TP_LinkedFormulas_List lf on lf.TP_ID = tp.TP_ID and lf.ChannelType = tp.ChannelType
	where tp.ClosedPeriod_ID is null and lf.StartDateTime <= ISNULL(@dtEnd, '21000101') and ISNULL(lf.FinishDateTime,'21000101') >= ISNULL(@dtStart, '20110101')

	select lf.*, tp.Section_ID into #closed
	from @tps tp
	join Info_TP_LinkedFormulas_List_Closed lf on lf.TP_ID = tp.TP_ID and lf.ChannelType = tp.ChannelType and lf.ClosedPeriod_Id = tp.ClosedPeriod_ID
	where tp.ClosedPeriod_ID is not null and lf.StartDateTime <= ISNULL(@dtEnd, '21000101') and ISNULL(lf.FinishDateTime,'21000101') >= ISNULL(@dtStart, '20110101')


	--Пока читаем только формулы нашей стороны
	select * 
	into #result1
	from 
	(
		--Наша сторона
		select tp.LinkedFormula_UN, fl.TP_ID, d.Formula_UN, fl.ChannelType, ForAutoUse, FormulaName, FormulaType_ID, fl.StartDateTime, fl.FinishDateTime, cast(null as uniqueidentifier) as ClosedPeriod_ID, cast(1 as bit) as IsOurSide, HighLimit, LowerLimit, tp.Section_ID
		from #tmp  tp
		join Info_TP_LinkedFormulas_OurSide_Description d on d.LinkedFormula_UN = tp.LinkedFormula_UN 
		join Info_TP2_OurSide_Formula_List fl on fl.Formula_UN = d.Formula_UN
		where fl.StartDateTime<=ISNULL(@dtEnd, '21000101') and ISNULL(fl.FinishDateTime,'21000101') >= ISNULL(@dtStart, '20110101')
		union all
		--Закрытый, наша сторона
		select tp.LinkedFormula_UN,fl.TP_ID, d.Formula_UN, fl.ChannelType, ForAutoUse, FormulaName, FormulaType_ID, fl.StartDateTime, fl.FinishDateTime, d.ClosedPeriod_ID, cast(1 as bit) as IsOurSide, HighLimit, LowerLimit, tp.Section_ID 
		from #closed tp
		join Info_TP_LinkedFormulas_OurSide_Description_Closed d on d.LinkedFormula_UN = tp.LinkedFormula_UN 
		join Info_TP2_OurSide_Formula_List_Closed fl on fl.Formula_UN = d.Formula_UN
		where fl.StartDateTime<=ISNULL(@dtEnd, '21000101') and ISNULL(fl.FinishDateTime,'21000101') >= ISNULL(@dtStart, '20110101')
	) tps

	--Первый результат (обычные формулы которые входят в составные)
	select * from #result1
	order by LinkedFormula_UN, ClosedPeriod_ID, TP_ID, ChannelType, StartDateTime, FormulaType_ID

	if (@isSelectFormulasParams = 1) begin
		--Перечень входящих формул, для наших формул
		create table #result2
		(
			InnerLevel int,
			Formula_UN varchar(22),
			StringNumber int,
			OperBefore nvarchar(255),
			UsedFormula_UN varchar(22),
			TI_ID int,
			ChannelType tinyint, 
			TP_ID int,
			Section_ID int,
			ContrTI_ID int,
			OperAfter nvarchar(255), 
			FormulaName nvarchar(255),
			ForAutoUse tinyint,
			IsIntegral bit,
			StartDateTime DateTime, 
			FinishDateTime DateTime NULL, 
			FormulaType_ID tinyint,
			ClosedPeriod_ID uniqueidentifier,
			MainFormula_UN varchar(22)
		)

		declare @Formula_UN varchar(22), @IsOurSide bit, @ClosedPeriod_ID uniqueidentifier;
		declare t cursor LOCAL STATIC FORWARD_ONLY READ_ONLY for select distinct Formula_UN, IsOurSide, ClosedPeriod_ID from #result1
		open t;
		FETCH NEXT FROM t into @Formula_UN, @IsOurSide, @ClosedPeriod_ID

		WHILE @@FETCH_STATUS = 0
		BEGIN
			if (@IsOurSide = 1)	insert into #result2 exec dbo.usp2_Info_FormulaSelectFSK @Formula_UN, @DTStart, @DTEnd, @ClosedPeriod_ID
			else insert into #result2 exec dbo.usp2_Info_FormulaSelectContr @Formula_UN, @DTStart, @DTEnd
	
			FETCH NEXT FROM t into @Formula_UN, @IsOurSide, @ClosedPeriod_ID
		END;

		CLOSE t
		DEALLOCATE t

		--Второй результат (фломулы входящие в основные)
		select * from #result2;
	end 

	--Третий результат (параметры и списки составных формул)
	select [TP_ID]
      ,[ChannelType]
      ,[StartDateTime]
      ,null as [ClosedPeriod_ID]
      ,[FinishDateTime]
      ,[LinkedFormula_UN]
      ,[LinkType]
      ,[ApplyDateTime]
      ,[User_ID] from #tmp
	union all
	select [TP_ID]
      ,[ChannelType]
      ,[StartDateTime]
      ,[ClosedPeriod_ID]
      ,[FinishDateTime]
      ,[LinkedFormula_UN]
      ,[LinkType]
      ,[ApplyDateTime]
      ,[User_ID] from #closed

	drop table #types
	drop table #tmp
	drop table #closed
end
go
   grant EXECUTE on usp2_Info_LinkedFormulasTp to [UserCalcService]
go

-- ======================================================================================
-- Автор:
--
--		Малышев Игорь
--
-- Дата создания:
--
--		Апрель, 2018
--
-- Описание:
--
--		Выбираем остатки по списку ТП
--
-- ======================================================================================

CREATE proc [dbo].[usp2_ExplDoc_Residues_TP]
(	
	@tpIds TpChannelType READONLY, --Точки поставок
	@DateStart DateTime, --Начало
	@IsReadCalculatedValues bit,
	@HalfHoursShiftClientFromServer int = 0, --Смещение количество получасовок между сервером и клиентом для 80020
	@UseLossesCoefficient bit = 0, --Использовать ли коэфф. потерь для ТИ
	@UseRoundedTi bit = 0 --Были использованы округленные значения ТИ
)
AS
BEGIN 
	set nocount on
	set quoted_identifier, ansi_nulls, ansi_warnings, arithabort, concat_null_yields_null, ansi_padding on
	set numeric_roundabort off
	set transaction isolation level read uncommitted

	SELECT r.[TP_ID]
      ,r.[EventDate]
      ,r.[ChannelType]
      ,r.[DataSource_ID]
      ,r.[HalfHoursShiftFromUTC]
	  ,r.[UseLossesCoefficient]
      ,case when @IsReadCalculatedValues = 1 then ISNULL(r.[CAL],r.[VAL]) else r.[VAL] end as VAL
      ,r.[LatestDispatchDateTime]
	  ,r.LatestApplyDateTimeOurSideFormulaList
	  ,r.LatestApplyDateTimeOurSideFormulaListDescription
	  ,r.LatestApplyDateTimeContrFormulaList
	  ,r.LatestApplyDateTimeContrFormulaListDescription
	  ,r.ChangeDateTime
	  , cast(case when exists(select top 1 1 from [dbo].[ExplDoc_Residues_TP_XML80020] where TP_ID = a.TP_ID and ChannelType = a.ChannelType 
			and EventDate < r.EventDate
			and DataSource_ID = r.DataSource_ID and HalfHoursShiftFromUTC = r.HalfHoursShiftFromUTC and UseLossesCoefficient = r.UseLossesCoefficient and UseRoundedTi = r.UseRoundedTi
			and (LatestDispatchDateTime > r.LatestDispatchDateTime
				or (ChangeDateTime is not null and ChangeDateTime > ISNULL(r.ChangeDateTime, r.LatestDispatchDateTime))
				or (LatestApplyDateTimeOurSideFormulaList is not null and LatestApplyDateTimeOurSideFormulaList > ISNULL(r.LatestApplyDateTimeOurSideFormulaList, r.LatestDispatchDateTime))
				or (LatestApplyDateTimeOurSideFormulaListDescription is not null and LatestApplyDateTimeOurSideFormulaListDescription > ISNULL(r.LatestApplyDateTimeOurSideFormulaListDescription, r.LatestDispatchDateTime))
			)
			)
		then 1
		else 0
		end as bit) as HaveLaterEntry
	FROM @tpIds a
	join [dbo].[ExplDoc_Residues_TP_XML80020] r on r.TP_ID = a.TP_ID and r.ChannelType = a.ChannelType
	where r.EventDate = DateAdd(day, -1, floor(cast(DATEADD(minute, -ISNULL(@HalfHoursShiftClientFromServer,0) * 30, @DateStart) as float))) 
	and r.HalfHoursShiftFromUTC = ISNULL(@HalfHoursShiftClientFromServer,0) and UseLossesCoefficient = @UseLossesCoefficient and UseRoundedTi = @UseRoundedTi	
end
go
   grant EXECUTE on usp2_ExplDoc_Residues_TP to [UserCalcService]
go