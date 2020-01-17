if exists (select 1
          from sysobjects
          where  id = object_id('usf2_Info_GetRangeTpInSection')
          and type in ('IF', 'FN', 'TF'))
   drop function usf2_Info_GetRangeTpInSection
go
-- ======================================================================================
-- Автор:
--
--		Малышев Игорь
--
-- Дата создания:
--
--		Ноябрь, 2013
--
-- Описание:
--
--		Формируем список дат привязки ТП к сечению
--
-- ======================================================================================

create FUNCTION [dbo].[usf2_Info_GetRangeTpInSection]
(	
	@Section_ID int,
	@TP_ID int,
	@ClosedPeriod_ID uniqueidentifier,
	@StartDateTime DateTime,
	@FinishDateTime DateTime,
	@ToIndexes bit = 1
)
RETURNS varchar(max)
AS
begin
declare 
@RangeInSection nvarchar(max);
set @RangeInSection = CONVERT(varchar, @ToIndexes) + ';';

--Возвращаем индексы, с которых и по которые читаем данные
if (@ToIndexes = 1) begin
	if (@ClosedPeriod_ID is null) 
		select @RangeInSection += convert(varchar, @StartDateTime, 120) + ','+ convert(varchar, StartDateTime, 120) + ',' +
			convert(varchar, @FinishDateTime, 120) + ','+ convert(varchar, ISNULL(FinishDateTime, '21000101'), 120) + ';' 
		from Info_Section_Description2 where Section_ID = @Section_ID and TP_ID = @TP_ID 
		and StartDateTime <=@FinishDateTime and ISNULL(FinishDateTime, '21000101') >= @StartDateTime
	else select @RangeInSection += convert(varchar, @StartDateTime, 120) + ','+ convert(varchar, StartDateTime, 120) + ',' +
			convert(varchar, @FinishDateTime, 120) + ','+ convert(varchar, ISNULL(FinishDateTime, '21000101'), 120) + ';'  
		from Info_Section_Description_Closed where Section_ID = @Section_ID and TP_ID = @TP_ID and ClosedPeriod_ID = @ClosedPeriod_ID
		and StartDateTime <=@FinishDateTime and ISNULL(FinishDateTime, '21000101') >= @StartDateTime
end else begin
	if (@ClosedPeriod_ID is null) 
		select @RangeInSection += convert(varchar, StartDateTime, 120)+','+ ISNULL(convert(varchar, FinishDateTime, 120), '')  + ';' 
		from Info_Section_Description2 where Section_ID = @Section_ID and TP_ID = @TP_ID 
		and StartDateTime <=@FinishDateTime and ISNULL(FinishDateTime, '21000101') >= @StartDateTime
	else select @RangeInSection += convert(varchar, StartDateTime, 120)+','+ ISNULL(convert(varchar, FinishDateTime, 120), '')  + ';' 
		from Info_Section_Description_Closed where Section_ID = @Section_ID and TP_ID = @TP_ID and ClosedPeriod_ID = @ClosedPeriod_ID
		and StartDateTime <=@FinishDateTime and ISNULL(FinishDateTime, '21000101') >= @StartDateTime
end

return @RangeInSection;
end
GO

grant exec on usf2_Info_GetRangeTpInSection to [UserCalcService]
GO