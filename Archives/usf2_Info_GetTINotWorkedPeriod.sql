if exists (select 1
          from sysobjects
          where  id = object_id('usf2_Info_GetTINotWorkedPeriod')
          and type in ('IF', 'FN', 'TF'))
   drop function usf2_Info_GetTINotWorkedPeriod
go
-- ======================================================================================
-- Автор:
--
--		Малышев Игорь
--
-- Дата создания:
--
--		Мафй, 2014
--
-- Описание:
--
--		Формируем список индексов получасовок когда ТИ не работала
--
-- ======================================================================================

create FUNCTION [dbo].[usf2_Info_GetTINotWorkedPeriod]
(	
	@TI_ID int,
	@StartDateTime DateTime,
	@FinishDateTime DateTime
)
RETURNS varchar(max)
AS
begin
declare 
@RangeInSection nvarchar(max);
set @RangeInSection = ',;';

--Возвращаем индексы, с которых и по которые читаем данные
			select @RangeInSection += convert(varchar, @StartDateTime, 120) + ','+ convert(varchar, StartDateTime, 120) + ',' +
			convert(varchar, @FinishDateTime, 120) + ','+ convert(varchar, ISNULL(DateAdd(minute, 29, FinishDateTime), '21000101'), 120) + ';'  
		from ArchComm_IntegralTINotWorkedPeriod where TI_ID = @TI_ID
		and StartDateTime <=@FinishDateTime and ISNULL(FinishDateTime, '21000101') >= @StartDateTime
return @RangeInSection;
end
GO

grant exec on usf2_Info_GetTINotWorkedPeriod to [UserCalcService]
GO