set ANSI_NULLS ON
set QUOTED_IDENTIFIER ON
go

if exists (select 1
          from sysobjects
          where  id = object_id('usf2_Utils_GetDaylightSavingsTimeStart')
          and type in ('IF', 'FN', 'TF'))
   drop function usf2_Utils_GetDaylightSavingsTimeStart
go
-- ======================================================================================
-- Автор:
--
--		Малышев Игорь
--
-- Дата создания:
--
--		Апрель, 2009
--
-- Описание:
--
--		Возвращаем дату и время перехода на лето
--
-- ======================================================================================


create FUNCTION [dbo].[usf2_Utils_GetDaylightSavingsTimeStart]
(@Custom_DT DateTime)
RETURNS datetime
as
begin
declare @delta int
--Определяем смещение вызванное различными региональными настройками
set @delta = 0
if (@@DATEFIRST < 7) set @delta = @@DATEFIRST;

return case datepart(weekday,convert(datetime,'01.03.'+cast(YEAR(@Custom_DT) as varchar),104) + @delta)
when 1 then convert(datetime,'29.03.'+cast(YEAR(@Custom_DT) as varchar)+' 2:00:00',104)
when 2 then convert(datetime,'28.03.'+cast(YEAR(@Custom_DT) as varchar)+' 2:00:00',104)
when 3 then convert(datetime,'27.03.'+cast(YEAR(@Custom_DT) as varchar)+' 2:00:00',104)
when 4 then convert(datetime,'26.03.'+cast(YEAR(@Custom_DT) as varchar)+' 2:00:00',104)
when 5 then convert(datetime,'25.03.'+cast(YEAR(@Custom_DT) as varchar)+' 2:00:00',104)
when 6 then convert(datetime,'31.03.'+cast(YEAR(@Custom_DT) as varchar)+' 2:00:00',104)
when 7 then convert(datetime,'30.03.'+cast(YEAR(@Custom_DT) as varchar)+' 2:00:00',104)
end
end

go
grant EXECUTE on usf2_Utils_GetDaylightSavingsTimeStart to [UserCalcService]
go