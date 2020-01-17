if exists (select 1
          from sysobjects
          where  id = object_id('usf2_Utils_HoursByPeriod')
          and type in ('IF', 'FN', 'TF'))
   drop function usf2_Utils_HoursByPeriod
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
--		Функция возвращает таблицу распределенных часов 
--
-- ======================================================================================
create FUNCTION [dbo].[usf2_Utils_HoursByPeriod] (
		@DateStart DateTime,@dateEnd DateTime
)
      RETURNS @tbl TABLE (
	[dt] datetime primary key
)
   BEGIN
declare @date datetime
set @date = @DateStart --Заранее округляем DateAdd(hour, DatePart(hour, @DateStart), floor(cast(@DateStart as float)));

while @date <= @dateEnd
	begin
		insert @tbl values (@date)
		set @date = dateadd(hh, 1, @date)
	end
RETURN
END
go
grant select on usf2_Utils_HoursByPeriod to [UserCalcService]
go