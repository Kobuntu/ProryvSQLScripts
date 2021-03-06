if exists (select 1
          from sysobjects
          where  id = object_id('usp2_Info_FindTI')
          and type in ('P','PC'))
   drop procedure usp2_Info_FindTI
go

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
--		Июнь, 2012
--
-- Описание:
--
--		Поиск ТИ
--
-- ======================================================================================
create proc [dbo].[usp2_Info_FindTI]
(
	@parrentHirerarchy int,
	@searchText nvarchar(1000),	--Текст для поиска
	@paramName nvarchar(255), -- Поле в котором ищем
	@parrentsList IntType readonly, --Родители по которым фильтруем
	@topFind int = 300, -- Предельное количество искомых ТИ
	@isFindByFullText bit = 1, -- Ищем по Full Text
	@TiTypeFilter tinyint = null, --Фильтр типа ТИ по которому нужен отбор
	@treeID int = null
)
as
begin
set nocount on
set quoted_identifier, ansi_nulls, ansi_warnings, arithabort, concat_null_yields_null, ansi_padding on
set numeric_roundabort off
set transaction isolation level read uncommitted
create table #tmp
(
	TI_ID int,
	TIName nvarchar(1024),
	PS_ID int, 
	TIType tinyint,
	Commercial bit,
	Voltage float,
	SectionNumber int,
	IsCa bit,
	TPCoef float,
	CoeffTrasformation float,
	IsChanelReverse bit,
	TP_ID int,
	AdditionalParent int,
	IsDeleted bit,
	MeterSerialNumber nvarchar(255),
	PhaseNumber tinyint null,
	TariffArray nvarchar(1000),
	CustomerKind tinyint null,
	AbsentChannelsMask tinyint null,
	MeterModel_ID int,
	AllowTariffWrite bit,
	IsSmallTI bit null,
	Pik nvarchar(4000)
PRIMARY KEY CLUSTERED (TI_ID)
		WITH (ALLOW_ROW_LOCKS = OFF, ALLOW_PAGE_LOCKS = OFF)
)

if (LOWER(@paramName) = 'pik') begin
	--Поиск по комментарию
	insert into #tmp
	exec usp2_Info_FindTIbyPik @parrentHirerarchy,@searchText,@parrentsList,@topFind,null, @TiTypeFilter
end else begin
	--Обычный поиск
	insert into #tmp
	exec usp2_Info_FindTIbyParamName @parrentHirerarchy,@searchText,@paramName,@parrentsList,@topFind,@isFindByFullText, null, @TiTypeFilter
	declare @c int;
	set @c = (select COUNT(*) from #tmp);
	--Если искали по full search и не нашли нужного количества, ищем по like, исключив из поиска уже найденные ТИ
	if (@isFindByFullText = 1 and @c < 40) begin
		--Уже найденные ТИ
		declare @TIArrayExcepted varchar(4000);
		set @TIArrayExcepted = '';
		select @TIArrayExcepted = @TIArrayExcepted + ltrim(str(TI_ID, 30)) + ',' from #tmp;
		set @topFind = @topFind - @c;
		insert into #tmp
		exec usp2_Info_FindTIbyParamName @parrentHirerarchy,@searchText,@paramName,@parrentsList,@topFind,0, @TIArrayExcepted, @TiTypeFilter, @treeID
	end
end
select * from #tmp
drop table #tmp
end
go
   grant EXECUTE on usp2_Info_FindTI to [UserCalcService]
go
