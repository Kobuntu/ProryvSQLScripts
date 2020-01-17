
if exists (select 1
          from sysobjects
          where  id = object_id('usp2_Forecast_GetTimeRules')
          and type in ('P','PC'))
   drop procedure usp2_Forecast_GetTimeRules
go

-- =============================================
-- Author:		Александр Карташев
-- Create date: Март 2017
-- Description:	Получение правил для объектов на дату
-- =============================================
CREATE PROCEDURE [dbo].[usp2_Forecast_GetTimeRules]
	-- Add the parameters for the stored procedure here
	@eventdate date ,
	@objectun nvarchar(max)
AS
BEGIN
	-- SET NOCOUNT ON added to prevent extra result sets from
	-- interfering with SELECT statements.
set nocount on
set quoted_identifier, ansi_nulls, ansi_warnings, arithabort, concat_null_yields_null, ansi_padding on
set numeric_roundabort off
set transaction isolation level read uncommitted


    -- Insert statements for procedure here
	


  SELECT   Forecast_Objects.ForecastObject_UN AS ForecastObject_UN,
  ISNULL(ISNULL(Forecast_Objects_PlanTimeRules_Exceptions.MaxMinutesPlanEventTime,
  ISNULL(Forecast_Objects_PlanTimeRules.MaxMinutesPlanEventTime,Forecast_Objects.MaxMinutesPlanEventTime)),NULL)  AS MaxMinutesPlanEventTime,
  ISNULL(ISNULL(Forecast_Objects_PlanTimeRules_Exceptions.MaxMinutesEventTimeFact,
  ISNULL(Forecast_Objects_PlanTimeRules.MaxMinutesEventTimeFact,Forecast_Objects.MaxMinutesEventTimeFact)),NULL)  AS MaxMinutesEventTimeFact
  ,
  @eventdate as EventDate
  FROM Forecast_Objects 
 LEFT JOIN Forecast_Objects_PlanTimeRules ON Forecast_Objects_PlanTimeRules.StartDate >= @eventdate AND Forecast_Objects_PlanTimeRules.FinishDate <= @eventdate 
 LEFT JOIN Forecast_Objects_PlanTimeRules_Exceptions on Forecast_Objects_PlanTimeRules_Exceptions.ForecastObject_UN  = Forecast_Objects.ForecastObject_UN AND Forecast_Objects_PlanTimeRules_Exceptions.EventDate = @eventdate
 where Forecast_Objects.ForecastObject_UN  in ( SELECT * FROM usf2_Utils_SplitString(@objectun,','))
END


go
   grant EXECUTE on usp2_Forecast_GetTimeRules to [UserCalcService]
go