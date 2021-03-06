if exists (select 1
          from sysobjects
          where  id = object_id('usp2_ArchTech_Quality_Read')
          and type in ('P','PC'))
 drop procedure usp2_ArchTech_Quality_Read
go

--Обновляем тип
--Удаляем если есть
IF  EXISTS (SELECT * FROM sys.types st JOIN sys.schemas ss ON st.schema_id = ss.schema_id WHERE st.name = N'TIDateTimeTableType' AND ss.name = N'dbo')
DROP TYPE [dbo].[TIDateTimeTableType]
-- Пересоздаем заново
CREATE TYPE [dbo].[TIDateTimeTableType] AS TABLE(
	[TI_ID] [dbo].[TI_ID_TYPE] NOT NULL,
	[EventDateTime] [datetime] NULL
)
GO

grant EXECUTE on TYPE::TIDateTimeTableType to [UserSlave61968Service]
go

grant EXECUTE on TYPE::TIDateTimeTableType to [UserCalcService]
go

IF NOT EXISTS(SELECT * FROM sys.types st JOIN sys.schemas ss ON st.schema_id = ss.schema_id WHERE st.name = N'PeriodId' AND ss.name = N'dbo')
CREATE TYPE [dbo].[PeriodId] AS TABLE(
	[ID] [int] NOT NULL,
	[StartDateTime] [datetime] NOT NULL,
	[FinishDateTime] [datetime] NULL,
	[Comment] nvarchar(1000)
)

grant EXECUTE on TYPE::PeriodId to [UserCalcService]
go

-- ======================================================================================
-- Автор:
--
--		Малышев Игорь
--
-- Дата создания:
--
--		Февраль, 2011
--
-- Описание:
--
--		Читаем последние мгновенные значения
--
-- ======================================================================================
create proc [dbo].[usp2_ArchTech_Quality_Read]
	@DTServerToLook datetime = null, -- Дата, время до которого ищем значения
	@TI_Array TIDateTimeTableType READONLY --Идентификаторы ТИ, время каждой ТИ с которого читаем данные по этой точке

as begin

set nocount on
set quoted_identifier, ansi_nulls, ansi_warnings, arithabort, concat_null_yields_null, ansi_padding on
set numeric_roundabort off
set transaction isolation level read uncommitted

declare 
@titype tinyint,
@table30Name nvarchar(100);

create table #t
(
TIType tinyint,
TI_ID int,
EventDateTime datetime NULL 
PRIMARY KEY CLUSTERED (TIType, TI_ID )
		WITH (ALLOW_ROW_LOCKS = OFF, ALLOW_PAGE_LOCKS = OFF)
)

insert into #t 
select ti.TIType, tis.TI_ID, EventDateTime
from @TI_Array tis
join Info_TI ti on ti.TI_ID = tis.TI_ID;

DECLARE @ParmDefinition NVARCHAR(1000);
SET @ParmDefinition = N'@titype tinyint,@DTServerToLook DateTime'
DECLARE @SQLString NVARCHAR(4000);


declare t cursor LOCAL STATIC FORWARD_ONLY READ_ONLY for select distinct TIType from #t
open t;
FETCH NEXT FROM t into @titype
	WHILE @@FETCH_STATUS = 0 BEGIN
		
		--Смотрим на тип точки
		if (@TIType < 10) begin
			set @table30Name = 'ArchTech_Quality_Values'
		end else begin
			set @table30Name = 'ArchBit_Quality_Values_' + ltrim(str(@TIType - 10,2));
		end;

		SET @SQLString = N'select #t.[TI_ID],it.COEFI,it.COEFU, a.EventDateTime,[MeterEventDateTime]
			,[VAL_100],[VAL_101],[VAL_102],[VAL_103],[VAL_104],[VAL_105],[VAL_110],[VAL_111],[VAL_112],[VAL_113],[VAL_114],[VAL_115]
			,[VAL_120],[VAL_121],[VAL_122],[VAL_123],[VAL_124],[VAL_125],[VAL_130],[VAL_131],[VAL_132],[VAL_133],[VAL_134],[VAL_135]
			,[VAL_140],[VAL_141],[VAL_142],[VAL_143],[VAL_144],[VAL_145],[VAL_150],[VAL_151],[VAL_152],[VAL_153],[VAL_154],[VAL_155]
			,[VAL_160],[VAL_161],[VAL_162],[VAL_163],[VAL_164],[VAL_165],[VAL_170],[VAL_171],[VAL_172],[VAL_173],[VAL_174],[VAL_175]
			,[VAL_180],[VAL_181],[VAL_182],[VAL_183],[VAL_184],[VAL_185],[VAL_190],[VAL_191],[VAL_192],[VAL_193],[VAL_194],[VAL_195]
			,[VAL_200],[VAL_201],[VAL_202],[VAL_203],[VAL_204],[VAL_205],[VAL_210],[VAL_211],[VAL_212],[VAL_213],[VAL_214],[VAL_215]
			,[VAL_220],[VAL_225],[VAL_226],[VAL_227],[VAL_230],[VAL_231],[VAL_232],[VAL_235],[VAL_236],[VAL_237],[VAL_240],[VAL_241]
			,[VAL_242],[VAL_250],[VAL_251],[VAL_252],[VAL_253],[VAL_254],[VAL_255]

			,[VAL_224],[VAL_229]
			,[VAL_233],[VAL_234]
			,[VAL_256],[VAL_257],[VAL_258],[VAL_259],[VAL_260],[VAL_261],[VAL_262],[VAL_263],[VAL_264]
			,[VAL_265],[VAL_266],[VAL_267],[VAL_268],[VAL_269],[VAL_270],[VAL_271],[VAL_272],[VAL_273],[VAL_274]
			,[VAL_275],[VAL_276],[VAL_277],[VAL_278],[VAL_279]
			,[VAL_280],[VAL_281]
			,[VAL_290],[VAL_291],[VAL_292],[VAL_293]
			,a.[CUS_ID]
		FROM #t	left join [dbo].[' + @table30Name + '] a on a.TI_ID = #t.TI_ID 
		AND ((#t.EventDateTime is not null AND a.EventDateTime > #t.EventDateTime) OR (#t.EventDateTime is null AND a.EventDateTime = (select Max([EventDateTime]) from [dbo].['+ @table30Name + '] where TI_ID = #t.TI_ID))) 
		and ((@DTServerToLook is not null AND a.EventDateTime <= @DTServerToLook) OR (@DTServerToLook is null))
		outer apply
		(select top (1) * from dbo.Info_Transformators where TI_ID=#t.TI_ID and a.EventDateTime between StartDateTime and ISNULL(FinishDateTime, ''21000101'') order by StartDateTime desc
		) it
		where #t.TIType = @titype'
		
		--print @SQLString
		
		EXEC sp_executesql @SQLString, @ParmDefinition, @titype,@DTServerToLook;

	FETCH NEXT FROM t into @titype
	END

	CLOSE t
	DEALLOCATE t


drop table #t;

end
go
   grant EXECUTE on usp2_ArchTech_Quality_Read to [UserCalcService]
go
   grant EXECUTE on usp2_ArchTech_Quality_Read to [UserSlave61968Service]
go

