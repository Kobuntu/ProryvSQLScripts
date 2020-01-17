if exists (select 1
          from sysobjects
          where  id = object_id('usf2_Utils_WeekByPeriod')
          and type in ('IF', 'FN', 'TF'))
   drop function usf2_Utils_WeekByPeriod
go

-- ======================================================================================
-- Автор:
--
--		Малышев Игорь
--
-- Дата создания:
--
--		Март, 2012
--
-- Описание:
--
--		Функция возвращает таблицу дней по неделям
--
-- ======================================================================================
create FUNCTION [dbo].[usf2_Utils_WeekByPeriod] (
		@DateStart DateTime,@dateEnd DateTime
)
      RETURNS @tbl TABLE (
	[dtFrom] datetime primary key,
	[dtTo] datetime, 
	[WeekNumber] int
	
)
   BEGIN
declare 
@date datetime,
@n int;

set @date = @dateEnd 
set @n = 0;

while @date >= @dateStart
	begin
		insert @tbl values (DATEADD(day, -7, @date), @date, @n);
		set @date = dateadd(day, -7, @date)
		set @n = @n+1;
	end
RETURN
END

go
grant select on usf2_Utils_WeekByPeriod to [UserCalcService]
go