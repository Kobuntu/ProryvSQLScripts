if exists (select 1
          from sysobjects
          where  id = object_id('usp2_WriteExpDocResiduesSection')
          and type in ('P','PC'))
   drop procedure usp2_WriteExpDocResiduesSection
go
/****** Object:  StoredProcedure [dbo].[usp2_InfoFormulaSelect]    Script Date: 10/02/2008 22:40:23 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO


--Создаем тип, если его еще нет
IF  EXISTS (SELECT * FROM sys.types st JOIN sys.schemas ss ON st.schema_id = ss.schema_id WHERE st.name = N'DBResiduesSectionTable' AND ss.name = N'dbo')
DROP TYPE [dbo].DBResiduesSectionTable

CREATE TYPE [dbo].[DBResiduesSectionTable] AS TABLE(
	[Section_ID] [int] NOT NULL,
	[EventDate] [smalldatetime] NOT NULL,
	[DataSource_ID] [int] NOT NULL,
	[VAL] [float] NULL,
	[LatestDispatchDateTime] [datetime] NOT NULL,
	[LatestApplyDateTimeOurSideFormulaList] [datetime] NULL,
	[LatestApplyDateTimeOurSideFormulaListDescription] [datetime] NULL,
	[LatestApplyDateTimeContrFormulaList] [datetime] NULL,
	[LatestApplyDateTimeContrFormulaListDescription] [datetime] NULL
)
go

grant EXECUTE on TYPE::DBResiduesSectionTable to UserCalcService
go
grant EXECUTE on TYPE::DBResiduesSectionTable to UserExportService
go
grant EXECUTE on TYPE::DBResiduesSectionTable to UserImportService
go
grant EXECUTE on TYPE::DBResiduesSectionTable to UserWebMonitoringService
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
--		Пишем остатки от округления 80020 по сечениям
--
-- ======================================================================================

create proc [dbo].[usp2_WriteExpDocResiduesSection]
	@ResiduesSectionTable DBResiduesSectionTable readonly,
	@HalfHoursShiftFromUTC int,
	@IsReadCalculatedValues bit
as
begin

set nocount on
set quoted_identifier, ansi_nulls, ansi_warnings, arithabort, concat_null_yields_null, ansi_padding on
set numeric_roundabort off

MERGE ExplDoc_Residues_Section_XML80020 a
	USING @ResiduesSectionTable m 
	ON a.Section_ID = m.Section_ID and a.EventDate = m.EventDate and a.DataSource_ID = m.DataSource_ID 
	and HalfHoursShiftFromUTC=@HalfHoursShiftFromUTC 
	WHEN MATCHED THEN 
	UPDATE set 
		VAL = case when @IsReadCalculatedValues=0 then m.VAL else a.VAL end,
		CAL = case when @IsReadCalculatedValues=1 then m.VAL else a.CAL end,
		LatestDispatchDateTime = m.LatestDispatchDateTime,
		LatestApplyDateTimeOurSideFormulaList = m.LatestApplyDateTimeOurSideFormulaList,
		LatestApplyDateTimeOurSideFormulaListDescription = m.LatestApplyDateTimeOurSideFormulaListDescription,
		LatestApplyDateTimeContrFormulaList = m.LatestApplyDateTimeContrFormulaList,
		LatestApplyDateTimeContrFormulaListDescription = m.LatestApplyDateTimeContrFormulaListDescription 
	WHEN NOT MATCHED THEN 
	INSERT ([Section_ID]
    ,[EventDate]
    ,[DataSource_ID]
    ,[HalfHoursShiftFromUTC]
    ,[VAL]
    ,[CAL]
    ,[LatestDispatchDateTime]
	,[LatestApplyDateTimeOurSideFormulaList]
	,[LatestApplyDateTimeOurSideFormulaListDescription]
	,[LatestApplyDateTimeContrFormulaList]
	,[LatestApplyDateTimeContrFormulaListDescription]) 
	values (m.Section_ID, m.EventDate, m.DataSource_ID, @HalfHoursShiftFromUTC,

	case when @IsReadCalculatedValues=0 then m.VAL else NULL end,
	case when @IsReadCalculatedValues=1 then m.VAL else NULL end, 
	m.LatestDispatchDateTime, m.LatestApplyDateTimeOurSideFormulaList, m.LatestApplyDateTimeOurSideFormulaListDescription,
	m.LatestApplyDateTimeContrFormulaList, m.LatestApplyDateTimeContrFormulaListDescription);

end
go
   grant EXECUTE on usp2_WriteExpDocResiduesSection to [UserCalcService]
go
   