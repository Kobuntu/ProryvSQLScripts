set ANSI_NULLS ON
set QUOTED_IDENTIFIER ON
go
if exists (select 1
          from sysobjects
          where  id = object_id('usf2_Utils_CorrectDateTimeByDaylight')
          and type in ('IF', 'FN', 'TF'))
   drop function usf2_Utils_CorrectDateTimeByDaylight
go

-- ======================================================================================
-- Автор:
--
--		Малышев Игорь
--
-- Дата создания:
--
--		Май, 2009
--
-- Описание:
--
--		Корректируем зимнее время на летнее время и на смещение относительно москвы, прибавляем дополнительное количество получасовок
--
-- ======================================================================================


create FUNCTION [dbo].[usf2_Utils_CorrectDateTimeByDaylight]
(	@Custom_DT DateTime,
	@SummerOrWinter tinyint, --Зимнее (0) или летнее(1) время 
	@OffsetFromMoscow int, --Смещение относительно Москвы в минутах
	@HalfHoursNumbers int -- Количество получасовок на которое корректируем дополнительно
)
RETURNS datetime
as
begin
return case when (@SummerOrWinter=1) and 
			(DateAdd(n,@HalfHoursNumbers * 30,@Custom_DT) >= dbo.usf2_Utils_GetDaylightSavingsTimeStart(DateAdd(n,@HalfHoursNumbers * 30,@Custom_DT)) and DateAdd(n,@HalfHoursNumbers * 30,@Custom_DT) < dbo.usf2_Utils_GetDaylightSavingsTimeEnd(DateAdd(n,@HalfHoursNumbers * 30,@Custom_DT))) 
			then DateAdd(n,@OffsetFromMoscow + 60 ,DateAdd(n,@HalfHoursNumbers * 30,@Custom_DT)) 
			else DateAdd(n, @OffsetFromMoscow,DateAdd(n,@HalfHoursNumbers * 30,@Custom_DT)) end
end

go
grant EXECUTE on usf2_Utils_CorrectDateTimeByDaylight to [UserCalcService]
go
