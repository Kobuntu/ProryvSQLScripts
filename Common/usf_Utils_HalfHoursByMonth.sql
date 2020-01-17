set ANSI_NULLS ON
set QUOTED_IDENTIFIER ON
go
if exists (select 1
          from sysobjects
          where  id = object_id('usf2_Utils_HalfHoursByMonth')
          and type in ('IF', 'FN', 'TF'))
   drop function usf2_Utils_HalfHoursByMonth
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
--		Функция возвращает таблицу получасовок за месяц;
--
-- ======================================================================================
create FUNCTION [dbo].[usf2_Utils_HalfHoursByMonth] (
		@MonthYear DateTime --Месяц, год за который надо получить получасовки
)
      RETURNS @tbl TABLE (
	[dt] datetime
)
   BEGIN
declare @date datetime
set @date = DateAdd(s,-DatePart(s,@MonthYear),DateAdd(n,-DatePart(n,@MonthYear),DateAdd(hh,-DatEPart(hh,@MonthYear),DateAdd(d,-Day(@MonthYear )+1,@MonthYear))))

while @date < DateAdd(m,1,@MonthYear)
	begin
		insert @tbl values (@date) --(dateadd(n, - @DaylightDelta, @date))
		set @date = dateadd(n, 30, @date)
	end
RETURN
END

go
grant select on usf2_Utils_HalfHoursByMonth to [UserCalcService]
go