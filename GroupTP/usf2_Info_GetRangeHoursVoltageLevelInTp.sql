set ANSI_NULLS ON
set QUOTED_IDENTIFIER ON
go
if exists (select 1
          from sysobjects
          where  id = object_id('usf2_Info_GetRangeHoursVoltageLevelInTp')
          and type in ('IF', 'FN', 'TF'))
   drop function usf2_Info_GetRangeHoursVoltageLevelInTp
go

-- ======================================================================================
-- Автор:
--
--		Малышев Игорь
--
-- Дата создания:
--
--		Январь, 2014
--
-- Описание:
--
--		Формируем список индексов действия тарифного расписания ТП в заданном диапазоне дат
--
-- ======================================================================================
create FUNCTION [dbo].[usf2_Info_GetRangeHoursVoltageLevelInTp]
(	
	@TP_ID int,
	@StartDateTime DateTime,
	@FinishDateTime DateTime,
	@ClosedPeriod_ID uniqueidentifier = null --Закрытый период (если null читаем из открытого периода)
)
RETURNS varchar(max)
AS
begin
declare 
@RangeInSection nvarchar(max), @roundedStartDateTime datetime;
set @RangeInSection = '';
set @roundedStartDateTime = dbo.usf2_Utils_DateTimeRoundToHalfHour(@StartDateTime,1);

if (@ClosedPeriod_ID is null) begin
	select @RangeInSection += convert(varchar, VoltageLevel) + ',' 
	+ convert(varchar, @StartDateTime, 120) + ','+ convert(varchar, StartDateTime, 120) + ',' +
			convert(varchar, @FinishDateTime, 120) + ','+ convert(varchar, ISNULL(FinishDateTime, '21000101'), 120) + ';'   
	from Info_TP_VoltageLevel where TP_ID = @TP_ID 
	and StartDateTime <= @FinishDateTime and ISNULL(FinishDateTime, '21000101') >= @roundedStartDateTime
end else begin
	select @RangeInSection += convert(varchar, VoltageLevel) + ',' 
	+ convert(varchar, @StartDateTime, 120) + ','+ convert(varchar, StartDateTime, 120) + ',' +
			convert(varchar, @FinishDateTime, 120) + ','+ convert(varchar, ISNULL(FinishDateTime, '21000101'), 120) + ';'  
	from Info_TP_VoltageLevel_Closed where TP_ID = @TP_ID and ClosedPeriod_ID = @ClosedPeriod_ID
	and StartDateTime <= @FinishDateTime and ISNULL(FinishDateTime, '21000101') >= @roundedStartDateTime
end

return @RangeInSection;
end
go
grant EXECUTE on usf2_Info_GetRangeHoursVoltageLevelInTp to [UserCalcService]
go