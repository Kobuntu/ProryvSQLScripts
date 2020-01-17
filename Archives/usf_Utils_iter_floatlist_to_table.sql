if exists (select 1
          from sysobjects
          where  id = object_id('usf2_Utils_iter_floatlist_to_table')
          and type in ('IF', 'FN', 'TF'))
   drop function usf2_Utils_iter_floatlist_to_table
go

-- ======================================================================================
-- Автор:
--
--		Малышев Игорь
--
-- Дата создания:
--
--		Декабрь, 2008
--
-- Описание:
--
--		Функция возвращает таблицу из строки где заданные параметры (два флоата) разделены ;
--
-- ======================================================================================
create FUNCTION [dbo].[usf2_Utils_iter_floatlist_to_table] (@list ntext)
      RETURNS @tbl TABLE (listpos int IDENTITY(1, 1) NOT NULL,  TI_ID  int , EventDate DateTime,
                          Float1  float, Float2  nvarchar(40)) AS
   BEGIN
      DECLARE @pos      int,
              @textpos  int,
              @chunklen smallint,
              @str      nvarchar(max),
			  @nuberCharForChannel int,
              @tmpstr   nvarchar(max),
              @leftover nvarchar(max)

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
			--Количество символов отведенное под канал
			set @nuberCharForChannel = 3 - charindex(',',substring(@str,Len(@str)-2,3))

            INSERT @tbl (TI_ID,EventDate, Float1, Float2) 
				VALUES(
						convert(int, substring(@str, 1, 10)),
						convert(DateTime, substring(@str, 12, 16),120),
						convert(float, replace(substring(@str, 29, len(@str)-29-@nuberCharForChannel),',','.'),120),
						convert(nvarchar(40), substring(@str,len(@str)-@nuberCharForChannel+1, @nuberCharForChannel+1))
				)
            SET @tmpstr = ltrim(substring(@tmpstr, @pos + 1, len(@tmpstr)))
            SET @pos = charindex(';', @tmpstr)
         END

         SET @leftover = @tmpstr
      END
      RETURN
   END
go
grant select on usf2_Utils_iter_floatlist_to_table to [UserCalcService]
go