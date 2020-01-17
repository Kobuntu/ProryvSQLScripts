if exists (select 1
          from sysobjects
          where  id = object_id('usf2_Utils_Split')
          and type in ('IF', 'FN', 'TF'))
   drop function usf2_Utils_Split
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
--		Аналог ф-ии Split
--
-- ======================================================================================
CREATE FUNCTION [dbo].[usf2_Utils_Split](@Str nvarchar(4000), @Delimiter char(1)) 
RETURNS @Results TABLE (Items int) 
AS BEGIN 
DECLARE 
	@Counter int,
	@Slice int 
	SET @Counter = 1 

	IF @Str IS NULL RETURN 

	WHILE @Counter !=0 	BEGIN 
		SELECT @Counter = CHARINDEX(@Delimiter,@Str) 

		IF @Counter !=0 
			SELECT @Slice = cast(str(LEFT(@Str,@Counter - 1)) as int) 
		ELSE 
			SELECT @Slice = cast(str(@Str) as int) 

		INSERT INTO @Results(Items) VALUES(cast(str(@Slice ,20) as int)) 

		SET @Str = RIGHT(@Str,LEN(@Str) - @Counter) 

		IF LEN(@Str) = 0 BREAK 
	END 
	RETURN 
END

go
grant select on usf2_Utils_Split to [UserCalcService]
go