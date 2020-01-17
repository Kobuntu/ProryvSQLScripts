if exists (select 1
          from sysobjects
          where  id = object_id('usf2_Utils_days_to_table')
          and type in ('IF', 'FN', 'TF'))
   drop function usf2_Utils_days_to_table
go
-- ======================================================================================
-- Автор:
--
--		Малышев Игорь
--
-- Дата создания:
--
--		Сентябрь, 2008
--
-- Описание:
--
--		Функция возвращает таблицу из строки где заданные параметры (ТИ и номер канала) разделены ;
--		Структуры (ТИ и номер канала) быть разделены ;
--		ТИ и номер канала в структуре должны быть разделены ,
--
-- ======================================================================================
create FUNCTION [dbo].[usf2_Utils_days_to_table] (@MonthYear DateTime)
      RETURNS @tbl TABLE (listpos int IDENTITY(1, 1) NOT NULL,
                          [CalendarDayDate]DateTime --Дата календаря
						) AS

   BEGIN
 declare
@date Datetime,
@DayInMonth int

set @DayInMonth=(SELECT DAY(DATEADD(Month, 1, @MonthYear) - DAY(DATEADD(Month, 1, @MonthYear))))

set @date = DateAdd(d,-Day(@MonthYear)+1,@MonthYear)
	while @date <= DateAdd(d,@DayInMonth-1,@MonthYear)
		begin
		insert @tbl ([CalendarDayDate]) values (@date)
		set @date = dateadd(d, 1, @date)
	end
	RETURN
   END
   
go
grant select on usf2_Utils_days_to_table to [UserCalcService]
go