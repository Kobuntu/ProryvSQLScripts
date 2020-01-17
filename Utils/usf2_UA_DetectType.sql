set ANSI_NULLS ON
set QUOTED_IDENTIFIER ON
go
if exists (select 1
          from sysobjects
          where  id = object_id('usf2_UA_DetectType')
          and type in ('IF', 'FN', 'TF'))
   drop function usf2_UA_DetectType
go

-- ======================================================================================
-- Автор:
--
--		Малышев Игорь
--
-- Дата создания:
--
--		Июль, 2014
--
-- Описание:
--
--		Преобразование записи вида i=2 к нужному типу
--
-- ======================================================================================
create FUNCTION [dbo].[usf2_UA_DetectType]
(	
	@variableType varchar(max)
)
RETURNS int
AS
begin
declare @n varchar(100), @i int;
set @i = charindex('i=', @variableType);
if @i < 0 return -1;

set @n = substring(@variableType, @i + 2,len(@variableType) - @i);
if (ISNUMERIC(@n)<>1)  return -1;

return CAST(@n AS INT);
end

go
grant EXECUTE on usf2_UA_DetectType to [UserCalcService]
go