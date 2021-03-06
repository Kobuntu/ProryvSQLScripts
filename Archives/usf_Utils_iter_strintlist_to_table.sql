if exists (select 1
          from sysobjects
          where  id = object_id('usf2_Utils_iter_strintlist_to_table')
          and type in ('IF', 'FN', 'TF'))
   drop function usf2_Utils_iter_strintlist_to_table
go

/****** Object:  UserDefinedFunction [dbo].[iter_intlist_to_table]    Script Date: 09/17/2008 15:58:42 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
-- ======================================================================================
-- Автор:
--
--		Малышев Игорь
--
-- Дата создания:
--
--		Сентябрь, 2009
--
-- Описание:
--
--		Функция возвращает таблицу из строки где заданные параметры (идентификатор формулы и ее тип) разделены ;
--
-- ======================================================================================
create FUNCTION [dbo].[usf2_Utils_iter_strintlist_to_table] (@list ntext)
      RETURNS @tbl TABLE (listpos int IDENTITY(1, 1) NOT NULL,
                          STRnumber  varchar(30) NOT NULL, CHnumber  int NOT NULL) AS
   BEGIN
      DECLARE @pos      int,
              @textpos  int,
              @chunklen smallint,
              @str      nvarchar(max),
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
            INSERT @tbl (STRnumber,CHnumber) VALUES(substring(@str, 1, charindex(',', @tmpstr)-1), convert(int, substring(@str, charindex(',', @tmpstr)+1,len(@tmpstr) )))
            SET @tmpstr = ltrim(substring(@tmpstr, @pos + 1, len(@tmpstr)))
            SET @pos = charindex(';', @tmpstr)
         END

         SET @leftover = @tmpstr
      END

      IF ltrim(rtrim(@leftover)) <> ''
         INSERT @tbl (STRnumber,CHnumber) VALUES(convert(int, substring(@leftover, 1, charindex(',', @leftover)-1)), substring(@leftover, charindex(',', @leftover)+1,len(@leftover)))

      RETURN
   END
   
   
go   
grant select on usf2_Utils_iter_strintlist_to_table to [UserCalcService]
go   