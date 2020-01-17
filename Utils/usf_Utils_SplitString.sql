if exists (select 1
          from sysobjects
          where  id = object_id('usf2_Utils_SplitString')
          and type in ('IF', 'FN', 'TF'))
   drop function usf2_Utils_SplitString
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
CREATE function [dbo].[usf2_Utils_SplitString] 
    (
        @Str nvarchar(max), @Delimiter char(1)
    )
   RETURNS @Results TABLE ([Item] nvarchar(1000)) 
AS BEGIN 
DECLARE 
	@Counter int,
	@Slice nvarchar(1000) 
	SET @Counter = 1 

	IF @Str IS NULL RETURN 

	WHILE @Counter <> 0 	BEGIN 
		SET @Counter = CHARINDEX(@Delimiter,@Str) 

		IF @Counter <>0 
			SET @Slice = LEFT(@Str,@Counter - 1)
		ELSE 
			SET @Slice = @Str 

		INSERT INTO @Results([Item]) VALUES(@Slice) 

		SET @Str = RIGHT(@Str,LEN(@Str) - @Counter) 

		IF LEN(@Str) = 0 BREAK 
	END 
	RETURN 
END

go
grant select on usf2_Utils_SplitString to [UserCalcService]
go