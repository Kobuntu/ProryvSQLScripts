if exists (select 1
          from sysobjects
          where  id = object_id('usf2_Utils_iter_threefloatlist_to_table')
          and type in ('IF', 'FN', 'TF'))
   drop function usf2_Utils_iter_threefloatlist_to_table
go

-- ======================================================================================
-- Автор:
--
--		Малышев Игорь
--
-- Дата создания:
--
--		Январь, 2011
--
-- Описание:
--
--		Функция возвращает таблицу из строки где заданные параметры (три флоата) разделены ;
--		Строка стоится так: 'EventDateTime|VAL|MAX_VAL|MIN_VAL;'
--
-- ======================================================================================
create FUNCTION [dbo].[usf2_Utils_iter_threefloatlist_to_table] (@list ntext)
      RETURNS @tbl TABLE (listpos int IDENTITY(1, 1) NOT NULL,  EventDate DateTime,
                          Float1  float, Float2  float, Float3  float) AS
   BEGIN
      DECLARE @pos      int,
              @textpos  int,
              @chunklen smallint,
              @str      nvarchar(4000),
			  @fpos1		int,
			  @fpos2		int,
			  @sfl		nvarchar(50),
			  @fl1		float,
			  @fl2		float,
			  @fl3		float,
              @tmpstr   nvarchar(4000),
              @leftover nvarchar(4000)

      SET @textpos = 1
      SET @leftover = ''
      WHILE @textpos <= datalength(@list) / 2
      BEGIN
         SET @chunklen = 4000 - datalength(@leftover) / 2
         SET @tmpstr = ltrim(@leftover + substring(@list, @textpos, @chunklen))
         SET @textpos = @textpos + @chunklen

         SET @pos = charindex(';', @tmpstr)
         WHILE @pos > 0
         BEGIN
            SET @str = substring(@tmpstr, 1, @pos - 1)
			SET @fpos1 = charindex('|', @str, charindex('|',@str) + 1);
			SET @sfl = substring(@str, 18,  @fpos1 - charindex('|',@str) - 1);
			SET @fl1 = Convert(float, replace(@sfl,',','.'), 120);

			SET @fpos2 = charindex('|', @str, charindex('|',@str, charindex('|',@str) + 1) + 1);
			SET @sfl = substring(@str, @fpos1 + 1,  @fpos2 - @fpos1 - 1);
			SET @fl2 = Convert(float, replace(@sfl,',','.'), 120);

			SET @sfl = substring(@str, @fpos2 + 1,  len(@str) - @fpos2);
			SET @fl3 = Convert(float, replace(@sfl,',','.'), 120);

            INSERT @tbl (EventDate, Float1, Float2, Float3) 
				VALUES(
						convert(DateTime, substring(@str, 0, 17),120),
						@fl1,
						@fl2,
						@fl3
				)
            SET @tmpstr = ltrim(substring(@tmpstr, @pos + 1, len(@tmpstr)))
            SET @pos = charindex(';', @tmpstr)
         END

         SET @leftover = @tmpstr
      END
      RETURN
   END
go
grant select on usf2_Utils_iter_threefloatlist_to_table to [UserCalcService]
go