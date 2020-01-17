set ANSI_NULLS ON
set QUOTED_IDENTIFIER ON
go
if exists (select 1
          from sysobjects
          where  id = object_id('usf2_Utils_NumberWeeksFromDateDiff')
          and type in ('IF', 'FN', 'TF'))
   drop function usf2_Utils_NumberWeeksFromDateDiff
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
--		Функция возвращает разницу в неделях между датой @@CurrDate и @dateEnd
--
-- ======================================================================================
create FUNCTION [dbo].[usf2_Utils_NumberWeeksFromDateDiff] (
		@CurrDate DateTime,@dateEnd DateTime
)
RETURNS int
as
BEGIN
RETURN Floor((DateDiff(day, @CurrDate, @dateEnd)) / 7);
END
go
grant EXECUTE on usf2_Utils_NumberWeeksFromDateDiff to [UserCalcService]
go