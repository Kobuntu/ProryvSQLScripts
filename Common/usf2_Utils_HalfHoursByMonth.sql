if exists (select 1
          from sysobjects
          where  id = object_id('usf2_Utils_MonthByPeriod')
          and type in ('IF', 'FN', 'TF'))
   drop function usf2_Utils_MonthByPeriod
go

-- ======================================================================================
-- Автор:
--
--		Малышев Игорь
--
-- Дата создания:
--
--		Февраль, 2009
--
-- Описание:
--
--		Функция возвращает таблицу месяц, год за указанный период
--
-- ======================================================================================
CREATE FUNCTION [dbo].[usf2_Utils_MonthByPeriod] (
		@DateStart DateTime,@dateEnd DateTime 
)
      RETURNS @tbl TABLE (
	[MonthYear] datetime primary key
	WITH (ALLOW_ROW_LOCKS = OFF, ALLOW_PAGE_LOCKS = OFF)
)
   BEGIN
declare @date datetime
set @DateStart = floor(cast(@DateStart as float))
set @dateEnd = floor(cast(@dateEnd as float))
set @date = @DateStart

while @date <= @dateEnd
	begin
		insert @tbl values (@date) --(dateadd(n, - @DaylightDelta, @date))
		set @date = dateadd(MONTH, 1, @date)
	end
RETURN
END

go
grant select on usf2_Utils_MonthByPeriod to [UserCalcService]
go
grant select on usf2_Utils_MonthByPeriod to [UserMaster61968Service]
go