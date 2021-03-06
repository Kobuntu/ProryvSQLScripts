if exists (select 1
          from sysobjects
          where  id = object_id('usp2_WriteExpDocResiduesTp')
          and type in ('P','PC'))
   drop procedure usp2_WriteExpDocResiduesTp
go
/****** Object:  StoredProcedure [dbo].[usp2_InfoFormulaSelect]    Script Date: 10/02/2008 22:40:23 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO


--Создаем тип, если его еще нет
IF  EXISTS (SELECT * FROM sys.types st JOIN sys.schemas ss ON st.schema_id = ss.schema_id WHERE st.name = N'DBResiduesTpTable' AND ss.name = N'dbo')
DROP TYPE [dbo].[DBResiduesTpTable]

CREATE TYPE [dbo].[DBResiduesTpTable] AS TABLE(
	[TP_ID] [int] NOT NULL,
	[ChannelType] [tinyint] NOT NULL,
	[EventDate] [smalldatetime] NOT NULL,
	[DataSource_ID] [int] NOT NULL,
	[UseLossesCoefficient] [bit] NOT NULL,
	[VAL] [float] NULL,
	[LatestDispatchDateTime] [datetime] NOT NULL,
	[LatestApplyDateTimeOurSideFormulaList] [datetime] NULL,
	[LatestApplyDateTimeOurSideFormulaListDescription] [datetime] NULL,
	[LatestApplyDateTimeContrFormulaList] [datetime] NULL,
	[LatestApplyDateTimeContrFormulaListDescription] [datetime] NULL,
	[ChangeDateTime] [datetime] NULL
)
GO

grant EXECUTE on TYPE::DBResiduesTpTable to UserCalcService
go
grant EXECUTE on TYPE::DBResiduesTpTable to UserExportService
go
grant EXECUTE on TYPE::DBResiduesTpTable to UserImportService
go
grant EXECUTE on TYPE::DBResiduesTpTable to UserWebMonitoringService
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
--		Пишем остатки от округления 80020 по точкам ТП
--
-- ======================================================================================

create proc [dbo].[usp2_WriteExpDocResiduesTp]
	@ResiduesTpTable DBResiduesTpTable readonly,
	@HalfHoursShiftFromUTC int,
	@IsReadCalculatedValues bit,
	@UseRoundedTi bit
as
begin

set nocount on
set quoted_identifier, ansi_nulls, ansi_warnings, arithabort, concat_null_yields_null, ansi_padding on
set numeric_roundabort off

declare @ChangeDateTime datetime;
set @ChangeDateTime = GETDATE()

MERGE ExplDoc_Residues_TP_XML80020 a
	USING (select distinct [TP_ID]
	,[ChannelType]
    ,[EventDate]
    ,[DataSource_ID]
	,[UseLossesCoefficient]
    ,[VAL]
    ,[LatestDispatchDateTime]
	,[LatestApplyDateTimeOurSideFormulaList]
	,[LatestApplyDateTimeOurSideFormulaListDescription]
	,[LatestApplyDateTimeContrFormulaList]
	,[LatestApplyDateTimeContrFormulaListDescription] from @ResiduesTpTable) m 
	ON a.TP_ID = m.TP_ID and a.EventDate = m.EventDate and a.ChannelType = m.ChannelType and a.DataSource_ID = m.DataSource_ID 
	and HalfHoursShiftFromUTC=@HalfHoursShiftFromUTC and a.UseLossesCoefficient = m.UseLossesCoefficient 
	and a.UseRoundedTi = @UseRoundedTi
	WHEN MATCHED THEN 
	UPDATE set 
		VAL = case when @IsReadCalculatedValues=0 then m.VAL else a.VAL end,
		CAL = case when @IsReadCalculatedValues=1 then m.VAL else a.CAL end,
		--Обновляем время изменения только если реально изменились данные
		LatestDispatchDateTime = case 
			when (@IsReadCalculatedValues=0 and m.Val <> a.Val)
			or (@IsReadCalculatedValues=1 and m.Val <> a.Cal)
			then m.LatestDispatchDateTime 
			else a.LatestDispatchDateTime 
		end,
		LatestApplyDateTimeOurSideFormulaList = m.LatestApplyDateTimeOurSideFormulaList,
		LatestApplyDateTimeOurSideFormulaListDescription = m.LatestApplyDateTimeOurSideFormulaListDescription,
		LatestApplyDateTimeContrFormulaList = m.LatestApplyDateTimeContrFormulaList,
		LatestApplyDateTimeContrFormulaListDescription = m.LatestApplyDateTimeContrFormulaListDescription,
		--Обновляем время изменения всегда ( или сделать только если реально изменились данные ???)
		ChangeDateTime = @ChangeDateTime 
	WHEN NOT MATCHED THEN 
	INSERT ([TP_ID]
	,[ChannelType]
    ,[EventDate]
    ,[DataSource_ID]
    ,[HalfHoursShiftFromUTC]
	,[UseLossesCoefficient]
    ,[VAL]
    ,[CAL]
    ,[LatestDispatchDateTime]
	,[LatestApplyDateTimeOurSideFormulaList]
	,[LatestApplyDateTimeOurSideFormulaListDescription]
	,[LatestApplyDateTimeContrFormulaList]
	,[LatestApplyDateTimeContrFormulaListDescription]
	,[UseRoundedTi]) 
	values (m.TP_ID, m.ChannelType, m.EventDate, m.DataSource_ID, @HalfHoursShiftFromUTC, m.UseLossesCoefficient,

	case when @IsReadCalculatedValues=0 then m.VAL else NULL end,
	case when @IsReadCalculatedValues=1 then m.VAL else NULL end, 
	m.LatestDispatchDateTime, m.LatestApplyDateTimeOurSideFormulaList, m.LatestApplyDateTimeOurSideFormulaListDescription,
	m.LatestApplyDateTimeContrFormulaList, m.LatestApplyDateTimeContrFormulaListDescription, @UseRoundedTi);

end
go
   grant EXECUTE on usp2_WriteExpDocResiduesTp to [UserCalcService]
go
   