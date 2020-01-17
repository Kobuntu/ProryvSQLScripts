if exists (select 1
          from sysobjects
          where  id = object_id('usf2_Utils_SplitNumbered')
          and type in ('IF', 'FN', 'TF'))
   drop function usf2_Utils_SplitNumbered
go

-- ======================================================================================
-- Автор:
--
--		Малышев Игорь
--
-- Дата создания:
--
--		Октябрь, 2011
--
-- Описание:
--
--		Аналог ф-ии Split, только еще возвращаем порядквый номер записи и отсеиваем пустые
--
-- ======================================================================================
CREATE FUNCTION [dbo].[usf2_Utils_SplitNumbered](@Str nvarchar(4000), @Delimiter char(1)) 
RETURNS @Results TABLE (val int, number int) 
AS BEGIN 
DECLARE 
	@Counter int,
	@Slice int,
	@number int
	 
	SET @Counter = 1
	SET @number = 0

	IF @Str IS NULL RETURN 

	WHILE @Counter !=0 	BEGIN 
		SELECT @Counter = CHARINDEX(@Delimiter,@Str) 

		IF @Counter !=0 
			SELECT @Slice = cast(str(LEFT(@Str,@Counter - 1)) as int) 
		ELSE 
			SELECT @Slice = cast(str(@Str) as int) 

		if (@Slice <> '') BEGIN
			INSERT INTO @Results(val, number) VALUES(cast(str(@Slice ,20) as int), @number) 
			SET @number = @number + 1
		END

		SET @Str = RIGHT(@Str,LEN(@Str) - @Counter) 

		IF LEN(@Str) = 0 BREAK 
	END 
	RETURN 
END

go
grant select on usf2_Utils_SplitNumbered to [UserCalcService]
go