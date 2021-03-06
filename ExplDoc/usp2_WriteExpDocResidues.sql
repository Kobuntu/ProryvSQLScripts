if exists (select 1
          from sysobjects
          where  id = object_id('usp2_WriteExpDocResidues')
          and type in ('P','PC'))
   drop procedure usp2_WriteExpDocResidues
go
/****** Object:  StoredProcedure [dbo].[usp2_InfoFormulaSelect]    Script Date: 10/02/2008 22:40:23 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO


--Создаем тип, если его еще нет
IF  EXISTS (SELECT * FROM sys.types st JOIN sys.schemas ss ON st.schema_id = ss.schema_id WHERE st.name = N'DBResiduesTable' AND ss.name = N'dbo')
DROP TYPE [dbo].[DBResiduesTable]

CREATE TYPE [dbo].[DBResiduesTable] AS TABLE(
	[TI_ID] [int] NOT NULL,
	[EventDate] [smalldatetime] NOT NULL,
	[ChannelType] [tinyint] NOT NULL,
	[DataSource_ID] [int] NOT NULL,
	[VAL] [float] NULL,
	[LatestDispatchDateTime] [datetime] NOT NULL,
	[UseLossesCoefficient] bit  NOT NULL
)
GO

grant EXECUTE on TYPE::DBResiduesTable to UserCalcService
go
grant EXECUTE on TYPE::DBResiduesTable to UserExportService
go
grant EXECUTE on TYPE::DBResiduesTable to UserImportService
go
grant EXECUTE on TYPE::DBResiduesTable to UserWebMonitoringService
go

-- ======================================================================================
-- Автор:
--
--		Малышев Игорь
--
-- Дата создания:
--
--		Июнь, 2015
--
-- Описание:
--
--		Пишем остатки от округления 80020
--
-- ======================================================================================

create proc [dbo].[usp2_WriteExpDocResidues]
	@ResiduesTable DBResiduesTable readonly,
	@HalfHoursShiftFromUTC int,
	@IsReadCalculatedValues bit
as
begin

set nocount on
set quoted_identifier, ansi_nulls, ansi_warnings, arithabort, concat_null_yields_null, ansi_padding on
set numeric_roundabort off

MERGE ExplDoc_Residues_XML80020 a
	USING @ResiduesTable m 
	ON a.TI_ID = m.TI_ID and a.EventDate = m.EventDate and a.ChannelType = m.ChannelType and a.DataSource_ID = m.DataSource_ID 
	and HalfHoursShiftFromUTC=@HalfHoursShiftFromUTC and a.UseLossesCoefficient = m.UseLossesCoefficient
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
		end
	WHEN NOT MATCHED THEN 
	INSERT ([TI_ID]
    ,[EventDate]
    ,[ChannelType]
    ,[DataSource_ID]
    ,[HalfHoursShiftFromUTC]
    ,[VAL]
    ,[CAL]
    ,[LatestDispatchDateTime],[UseLossesCoefficient]) values (m.TI_ID, m.EventDate, m.ChannelType, m.DataSource_ID, @HalfHoursShiftFromUTC, 
	case when @IsReadCalculatedValues=0 then m.VAL else NULL end,
	case when @IsReadCalculatedValues=1 then m.VAL else NULL end, 
	m.LatestDispatchDateTime, m.UseLossesCoefficient);

end
go
   grant EXECUTE on usp2_WriteExpDocResidues to [UserCalcService]
go
   