if exists (select 1
          from sysobjects
          where  id = object_id('usf2_Utils_HalfHoursByPeriod')
          and type in ('IF', 'FN', 'TF'))
   drop function usf2_Utils_HalfHoursByPeriod
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
--		Функция возвращает таблицу распределенных дней 
--
-- ======================================================================================
create FUNCTION [dbo].[usf2_Utils_HalfHoursByPeriod] (
		@DateStart DateTime,@dateEnd DateTime 
)
      RETURNS @tbl TABLE (
	[dt] datetime primary key
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
		set @date = dateadd(d, 1, @date)
	end
RETURN
END
go
grant select on usf2_Utils_HalfHoursByPeriod to [UserCalcService]
go
grant select on usf2_Utils_HalfHoursByPeriod to [UserMaster61968Service]
go