if exists (select 1
          from sysobjects
          where  id = object_id('usp2_Info_GetBalanceFreeHierarchyParams')
          and type in ('P','PC'))
   drop procedure usp2_Info_GetBalanceFreeHierarchyParams
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
--		Октябрь 2015
--
-- Описание:
--
--		Данные необходимые для обсчетов универсальных балансов 
--
-- ======================================================================================
create proc [dbo].[usp2_Info_GetBalanceFreeHierarchyParams]
(	
	@balanceFreeHierarchyUNs varchar(4000),	--Перечень идентификаторов универсальных балансов
	@datestart datetime, --Начальная дата
	@dateend datetime, --Конечная дата
	@isGenerateDoc bit --Нужна доп информация для отчета
)

AS
declare
@Num int,
@TI_ID int,
@ChannelType int

begin

	set nocount on
	set quoted_identifier, ansi_nulls, ansi_warnings, arithabort, concat_null_yields_null, ansi_padding on
	set numeric_roundabort off
	set transaction isolation level read uncommitted
	
	select distinct Item as balanceFreeHierarchyUN into #items from dbo.usf2_Utils_SplitString(@balanceFreeHierarchyUNs, ',')
	
	--Информация по балансу
	CREATE TABLE #balanceList(
	[BalanceFreeHierarchy_UN] varchar(22) NOT NULL,
	[BalanceFreeHierarchyObject_UN] varchar(22) NOT NULL,
	[BalanceFreeHierarchyType_ID] tinyint NOT NULL,
	[ForAutoUse] [bit] NOT NULL,
	[HighLimit] [float] NULL,
	[LowerLimit] [float] NULL,
	[BalanceFreeHierarchyName] [nvarchar](255) NOT NULL,
	[User_ID] varchar(22) NOT NULL,
	[DispatchDateTime] [datetime] NOT NULL,
	[BalanceFreeHierarchyHeader_UN] uniqueidentifier NULL,
	[HighLimitValue] [float] NULL,
	[LowerLimitValue] [float] NULL);

	merge into #balanceList 
	using (select [BalanceFreeHierarchy_UN]
      ,[BalanceFreeHierarchyObject_UN]
      ,[BalanceFreeHierarchyType_ID]
      ,[ForAutoUse]
      ,[HighLimit]
      ,[LowerLimit]
      ,[BalanceFreeHierarchyName]
      ,[User_ID]
      ,[DispatchDateTime]
      ,[BalanceFreeHierarchyHeader_UN]
      ,[HighLimitValue]
      ,[LowerLimitValue] from Info_Balance_FreeHierarchy_List where BalanceFreeHierarchy_UN in (select balanceFreeHierarchyUN from #Items)
	  ) as bl on 1=0
	WHEN NOT MATCHED THEN
	INSERT([BalanceFreeHierarchy_UN]
      ,[BalanceFreeHierarchyObject_UN]
      ,[BalanceFreeHierarchyType_ID]
      ,[ForAutoUse]
      ,[HighLimit]
      ,[LowerLimit]
      ,[BalanceFreeHierarchyName]
      ,[User_ID]
      ,[DispatchDateTime]
      ,[BalanceFreeHierarchyHeader_UN]
      ,[HighLimitValue]
      ,[LowerLimitValue]) Values([BalanceFreeHierarchy_UN]
      ,[BalanceFreeHierarchyObject_UN]
      ,[BalanceFreeHierarchyType_ID]
      ,[ForAutoUse]
      ,[HighLimit]
      ,[LowerLimit]
      ,[BalanceFreeHierarchyName]
      ,[User_ID]
      ,[DispatchDateTime]
      ,[BalanceFreeHierarchyHeader_UN]
      ,[HighLimitValue]
      ,[LowerLimitValue])
	output inserted.[BalanceFreeHierarchy_UN]
      ,inserted.[BalanceFreeHierarchyObject_UN]
      ,inserted.[BalanceFreeHierarchyType_ID]
      ,inserted.[ForAutoUse]
      ,inserted.[HighLimit]
      ,inserted.[LowerLimit]
      ,inserted.[BalanceFreeHierarchyName]
      ,inserted.[User_ID]
      ,inserted.[DispatchDateTime]
      ,inserted.[BalanceFreeHierarchyHeader_UN]
      ,inserted.[HighLimitValue]
      ,inserted.[LowerLimitValue];


	--Привязки балансов к объектам
	select distinct bl.BalanceFreeHierarchy_UN, o.* from #balanceList bl
	join [dbo].[Info_Balance_FreeHierarchy_Objects] o on o.BalanceFreeHierarchyObject_UN = bl.BalanceFreeHierarchyObject_UN
	
	drop table #balanceList;

	--Названия подразделов
	if (@isGenerateDoc = 1) begin
		
		select *
		from Dict_Balance_FreeHierarchy_Subsections
		where BalanceFreeHierarchySubsection_UN in (
			select distinct BalanceFreeHierarchySubsection_UN 
			from Info_Balance_FreeHierarchy_Description
			where BalanceFreeHierarchy_UN in (select balanceFreeHierarchyUN from #Items) 
				and BalanceFreeHierarchySubsection_UN is not null
			union
			select distinct BalanceFreeHierarchySubsection2_UN 
			from Info_Balance_FreeHierarchy_Description
			where BalanceFreeHierarchy_UN in (select balanceFreeHierarchyUN from #Items) 
				and BalanceFreeHierarchySubsection2_UN is not null
			union
			select distinct BalanceFreeHierarchySubsection3_UN 
			from Info_Balance_FreeHierarchy_Description
			where BalanceFreeHierarchy_UN in (select balanceFreeHierarchyUN from #Items) 
				and BalanceFreeHierarchySubsection3_UN is not null
		)
	end

	select d.*, ISNULL(MeasuringComplexError, 1) as MeasuringComplexError, MeterSerialNumber, Voltage, ISNULL(CoeffTransformation, 1) as CoeffTransformation, 
	s.MetaString1, s.MetaString2, s.MetaString3, [Name], ISNULL(CoeffLosses, 1) as CoeffLosses
	from Info_Balance_FreeHierarchy_Description d 
	join Dict_Balance_FreeHierarchy_Sections s on s.BalanceFreeHierarchySection_UN = d.BalanceFreeHierarchySection_UN
	cross apply	dbo.usf2_Info_GetBalanceItemParams(ISNULL(TI_ID, IntegralTI_ID), TP_ID, Formula_UN, OurFormula_UN, 
	ContrFormula_UN, PTransformator_ID, PReactor_ID, Section_ID, FormulaConstant_UN, @datestart, @dateend)
	where BalanceFreeHierarchy_UN in (select balanceFreeHierarchyUN from #Items)
end
go
   grant exec on usp2_Info_GetBalanceFreeHierarchyParams to [UserCalcService]
go
   grant exec on usp2_Info_GetBalanceFreeHierarchyParams to UserExportService
go  
   grant exec on usp2_Info_GetBalanceFreeHierarchyParams to UserDeclarator
go