if exists (select 1
          from sysobjects
          where  id = object_id('usf2_Utils_iterCA_intlist_to_table')
          and type in ('IF', 'FN', 'TF'))
   drop function usf2_Utils_iterCA_intlist_to_table
go

create FUNCTION [dbo].[usf2_Utils_iterCA_intlist_to_table] (@list ntext)
      RETURNS @tbl TABLE (listpos int IDENTITY(1, 1) NOT NULL,
                          TInumber  int NOT NULL, CHnumber  int NOT NULL, IsCAorOsOV int NOT NULL) AS
   BEGIN
      DECLARE @pos      int,
              @textpos  int,
			  @TI_ID    int,		
			  @IsCA     int,
              @chunklen smallint,
              @str      nvarchar(4000),
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
			SET @TI_ID =  convert(int, substring(@str, 1, charindex(',', @tmpstr)-1))
			SET @IsCA = convert(int, substring(@str, charindex(',', @tmpstr, charindex(',', @tmpstr)+1) + 1 ,1 ))
--			if (@IsCA=0) begin
--				if ((select Count(OV_ID) from Hard_OV_List where TI_ID = @TI_ID) > 0 )	SET @IsCA = 2
--			end 
            INSERT @tbl (TInumber,CHnumber,IsCAorOsOV) VALUES(@TI_ID , 
							convert(int, substring(@str, charindex(',', @tmpstr)+1, charindex(',', @tmpstr, charindex(',', @tmpstr)+1) - charindex(',', @tmpstr) - 1 )),
							@IsCA)
            SET @tmpstr = ltrim(substring(@tmpstr, @pos + 1, len(@tmpstr)))
            SET @pos = charindex(';', @tmpstr)
         END

         SET @leftover = @tmpstr
      END

      IF ltrim(rtrim(@leftover)) <> ''
			INSERT @tbl (TInumber,CHnumber,IsCAorOsOV) VALUES(convert(int, substring(@leftover, 1, charindex(',', @leftover)-3)), 
							substring(@leftover, charindex(',', @leftover)+1,1),
							substring(@leftover, charindex(',', @leftover)+3,1))         
							

      RETURN
   END
   go
   grant select on usf2_Utils_iterCA_intlist_to_table to [UserCalcService]
go