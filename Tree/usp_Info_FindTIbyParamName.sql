if exists (select 1
          from sysobjects
          where  id = object_id('usf_fts_parser')
                    and type in ('IF', 'FN', 'TF'))
   drop function usf_fts_parser
go

create function  usf_fts_parser 
    (@searchText nvarchar(max) )	
RETURNS @ret TABLE
 (
 keyword varbinary(100), 
 group_id int, 
 occurrence int, 
 special_term nvarchar(400),
 display_term nvarchar(1000)
)
with execute as owner 
begin

	set @searchText=replace (@searchText,'%','')
	--удаляем кавычки (заменяем на пробелы) и заключаем все выражение в кавычки (dm_fts_parser)
	set @searchText=replace (@searchText,'"',' ')
	set @searchText=replace (@searchText,'  ',' ')
	set @searchText= '"'+@searchText+'"'

	insert into @ret(keyword,group_id, occurrence,special_term,display_term)
	select
	distinct  keyword,group_id, occurrence,special_term,display_term
	FROM sys.dm_fts_parser( @searchText , 1049, null, 0)
	order by group_id, occurrence

	return
end
go 
grant select on usf_fts_parser to [UserCalcService]
go
grant select on usf_fts_parser to [UserDeclarator]
go



if exists (select 1
          from sysobjects
          where  id = object_id('dbo.usf2_Utils_ConvertStringQuery')
          and type in ('IF', 'FN', 'TF'))
   drop function dbo.usf2_Utils_ConvertStringQuery
go

-- ======================================================================================
-- Автор:
--
--		Карпов
--
-- Дата создания:
--
--		август, 2018
--
-- Описание:
--
--		преобразование поискового запроса для использования в contains или like
--
-- ======================================================================================
create function dbo.usf2_Utils_ConvertStringQuery 
 (@searchText nvarchar(4000)='', @GetLikeString bit = false)
 RETURNS nvarchar(4000)
 as 
 begin
 	
		--set @searchText = '"' + @searchText + '*"';
		
	set @searchText=replace (@searchText,'%','')
	--удаляем кавычки и заключаем все выражение в кавычки (dm_fts_parser)
	set @searchText=replace (@searchText,'"','')
	set @searchText= '"'+@searchText+'"'

	--находим все слова
	declare @allWords table (keyword varbinary(100), group_id int, occurrence int, special_term nvarchar(400),display_term nvarchar(1000))
	insert into @allWords(keyword,group_id, occurrence,special_term,display_term)
	select keyword,group_id, occurrence,special_term,display_term from usf_fts_parser (@searchText)


	--для Contains оставляем разрешенные
	declare @allowWords table 
	(keyword varbinary(100), group_id int, occurrence int,special_term nvarchar(400),display_term nvarchar(1000))
	insert into @allowWords 
	select distinct keyword,group_id, occurrence,special_term,display_term 
	from @allWords temp
	where 
	special_term like 'Exact Match' 
	and @searchText like '%'+temp.display_term+'%'  --
	and len (temp.display_term)>1
	and display_term not in (select distinct stopword from sys.fulltext_system_stopwords)
	--все таки исключаем nn т.к. иногда например при №15 вроде бы 15 разрешено, но не ищется..
	--будем эти слова искать в like
	and not exists 
		(select top 1 1 from @allWords aw1
		 where temp.keyword<> aw1.keyword and 
		aw1.display_term='nn'+temp.display_term)
	

	--формируем строку для Contains с * из разрешенных слов
	declare @st nvarchar(4000)
	set @st = ''
	select @st = @st + ' AND ("' + display_term + '*" OR "' + display_term + '")'
	from	 @allowWords
 
	if (@st is not null and LEN(@st) > 5) begin
		set @st = SUBSTRING(@st, 5, LEN(@st) - 4)
	end
	  
	  
	declare @stLike nvarchar(200)='%'
	declare @keyword varbinary(100), @group_id int, @occurrence int,@special_term nvarchar(400),@display_term nvarchar(1000)

	--в строку выборка криво работает 
	--туда повторяющиеся символы не выбираются а нам нужно в том же порядке что в запросе построить Like
	DECLARE denyWords_cursor CURSOR FOR 
	select keyword,group_id, occurrence,special_term,
	display_term = display_term 
	from @allWords temp
	where 
	temp.keyword not in (select keyword from @allowWords)
	and display_term not like 'nn%' -- nn исключаем, т.к. их дубли
	 
	order by group_id, occurrence
	OPEN denyWords_cursor
	FETCH NEXT FROM denyWords_cursor INTO  @keyword, @group_id, @occurrence,@special_term,@display_term
	WHILE @@FETCH_STATUS = 0
	BEGIN
	set @stLike=@stLike+@display_term+'%'
	FETCH NEXT FROM denyWords_cursor INTO  @keyword, @group_id, @occurrence,@special_term,@display_term
	END 
	CLOSE denyWords_cursor
	DEALLOCATE denyWords_cursor
	  

	 if (@GetLikeString=1)
		return @stlike
	 else 
		return @st


	 return ''
	end

	GO

grant execute on dbo.usf2_Utils_ConvertStringQuery to UserDeclarator
go
grant execute on dbo.usf2_Utils_ConvertStringQuery to UserCalcService
go



if exists (select 1
          from sysobjects
          where  id = object_id('usp2_Info_FindTIbyParamName')
          and type in ('P','PC'))
   drop procedure usp2_Info_FindTIbyParamName
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
--		Март, 2011
--
-- Описание:
--
--		Поиск по ТИ
--
-- ======================================================================================
create proc [dbo].[usp2_Info_FindTIbyParamName]
(
	@parrentHirerarchy int,
	@searchText nvarchar(1000),	--Текст для поиска
	@paramName nvarchar(255), -- Поле в котором ищем
	@parrentsList IntType readonly, --Родители по которым фильтруем
	@topFind int = 100, -- Предельное значение для поиска
	@isFindByFullText bit = 1, -- Ищем по Full Text
	@TIArrayExcepted varchar(4000) = null, -- Точки измерения, которые исключаем к запросу
	@TiTypeFilter tinyint = null, --Фильтр типа ТИ по которому нужен отбор
	@treeID int = null
)
as
begin
set nocount on
set quoted_identifier, ansi_nulls, ansi_warnings, arithabort, concat_null_yields_null, ansi_padding on
set numeric_roundabort off
set transaction isolation level read uncommitted

declare @IsParrentFilterEnabled bit;

create table #tblTIToExcept
(
	TI_ID int
)

if @parrentHirerarchy <> 18 and @parrentHirerarchy <> 20 and exists(select top 1 1 from @parrentsList) begin
	set @IsParrentFilterEnabled = 1;
end else begin
	set @IsParrentFilterEnabled = 0;
end;

declare @IsbyMeterNumberFind bit, @IsExistsTIToExcept bit, @isUseTreeId bit = 0;
if (LOWER(@paramName) = 'meterserialnumber') set @IsbyMeterNumberFind = 1;
else set @IsbyMeterNumberFind = 0;

if (@TIArrayExcepted is not null AND LEN(@TIArrayExcepted) > 1) begin
	set @IsExistsTIToExcept = 1;
	insert into #tblTIToExcept
	select Items from usf2_Utils_Split(@TIArrayExcepted, ',');
end else begin
	set @IsExistsTIToExcept = 0;
end

--Обрабатываем фильтр по идентификатору дерева
create table #tiFilter(TI_ID int null)
create table #psFilter(PS_ID int null)

if (@treeID is not null and @treeID >= 0)  begin 

	set @isUseTreeId = 1;
	
	insert into #tiFilter
	select TI_ID from [dbo].[Dict_FreeHierarchyTree] t
	join [dbo].[Dict_FreeHierarchyTree_Description] d on d.[FreeHierItem_ID]  = t.FreeHierItem_ID
	where t.FreeHierTree_ID = @treeID
	and TI_ID is not null 

	insert into #tiFilter
	select PS_ID from [dbo].[Dict_FreeHierarchyTree] t
	join [dbo].[Dict_FreeHierarchyTree_Description] d on d.[FreeHierItem_ID]  = t.FreeHierItem_ID
	where t.FreeHierTree_ID = @treeID
	and PS_ID is not null and IncludeObjectChildren = 1

end


set @paramName = LOWER(@paramName);

if (@paramName = 'meterserialnumber') begin

	set @searchText = '%' + @searchText + '%';

	select top(@topFind) ti.TI_ID as TI_ID,
	ti.TIName as TIName,
	ti.PS_ID as PS_ID, 
	ti.TIType,
	cast(ti.Commercial as bit) as Commercial,
	ti.Voltage,
	ISNULL(ti.SectionNumber, 0) as SectionNumber,
	Cast (0 as bit) as IsCa,
	ISNULL(ti.TPCoefOurSide,1) as TPCoef,
	cast (ISNULL(it.Coeff,1) as float(26)) as CoeffTrasformation,
	cast((case when ti.AIATSCode=2 then 1 else 0 end) as bit) as IsChanelReverse,
	ISNULL(ti.TP_ID,0) as TP_ID,
	0 as AdditionalParent,
	ti.Deleted as IsDeleted,
	case when hm.MeterType_ID = 2012 then dbo.usf2_Hard_Meters_Addx_RemoteDisplay(hm.MeterSerialNumber) 
	else hm.MeterSerialNumber end as MeterSerialNumber, 
	ti.PhaseNumber,
	dbo.usf2_Info_GetTariffsForTI(ti.TI_ID) as TariffArray,
	ti.CustomerKind,
	ti.AbsentChannelsMask,
	hm.MeterModel_ID,
	hm.AllowTariffWrite,
	ti.IsSmallTI,
	'' as Pik
	from HARD_METERS hm 
	--join dbo.Info_Meters_TO_TI mti on hm.Meter_id = mti.Meter_id 
	outer apply (
		select top 1 * from dbo.Info_Meters_TO_TI 
		where Meter_id = hm.Meter_id and StartDateTime<= GETDATE() and (FinishDateTime is null or FinishDateTime >= GETDATE())
		order by StartDateTime desc
	) mti
	join Info_ti ti on ti.TI_ID = mti.TI_ID
	outer apply
	(
		select top 1 StartDateTime,TI_ID, COEFU*COEFI as Coeff from Info_Transformators
		where TI_ID = ti.TI_ID order by StartDateTime desc
	) it 
	where hm.MeterSerialNumber like @searchText and ti.[Deleted] <> 1 
	--Необязательные условия
	and (@TiTypeFilter is null or ti.TiType = @TiTypeFilter) --Фильтр по типу ТИ
	and (@IsExistsTIToExcept=0 or ti.TI_ID not in (select TI_ID from #tblTIToExcept)) --Если есть ТИ на исключение к поиску
	and (@IsParrentFilterEnabled = 0 or ti.PS_ID in (select id from @parrentsList)) --Фильтр по родителю

	and (@isUseTreeId = 0 or ti.TI_ID in (select TI_ID from #tiFilter) or ti.PS_ID in (select PS_ID from #psFilter))--Нужно возвращать ТИ только из дерева
	--and (@paramName <> 'ti_id' or ti.TI_ID = @searchText) --Ищем по названию ТИ 
	-- Тут можно добавить поиск по другим полям
	;

end else begin

	declare  @stContains nvarchar(4000) =''
	declare  @stLike nvarchar(4000) =''

	--возвращяем условия для Contains и для like
	select @stContains=dbo.usf2_Utils_ConvertStringQuery(@searchText,0)
	select @stLike=dbo.usf2_Utils_ConvertStringQuery(@searchText,1)
	
	--пустое условие нельзя для Contains
	

	if (@stLike like '') 
		set  @stLike='%'
	 
	 set @stContains = isnull(@stContains,'')

	 if (@stContains like '') 
	 begin

			select top(@topFind) --тут вся выборка по fulltext
			ti.TI_ID as TI_ID,
			ti.TIName as TIName,
			ti.PS_ID as PS_ID, 
			ti.TIType,
			cast(ti.Commercial as bit) as Commercial,
			ti.Voltage,
			ISNULL(ti.SectionNumber, 0) as SectionNumber,
			Cast (0 as bit) as IsCa,
			ISNULL(ti.TPCoefOurSide,1) as TPCoef,
			cast (ISNULL(it.Coeff,1) as float(26)) as CoeffTrasformation,
			cast((case when ti.AIATSCode=2 then 1 else 0 end) as bit) as IsChanelReverse,
			ISNULL(ti.TP_ID,0) as TP_ID,
			0 as AdditionalParent,
			ti.Deleted as IsDeleted,
			case when im.MeterType_ID = 2012 then dbo.usf2_Hard_Meters_Addx_RemoteDisplay(im.MeterSerialNumber) 
			else im.MeterSerialNumber end as MeterSerialNumber,
			ti.PhaseNumber,
			dbo.usf2_Info_GetTariffsForTI(ti.TI_ID) as TariffArray,
			ti.CustomerKind,
			ti.AbsentChannelsMask,
			im.MeterModel_ID,
			im.AllowTariffWrite,
			ti.IsSmallTI,
			'' as Pik
			from Info_ti ti 
			outer apply (
				select top 1 hm.*
				from dbo.Info_Meters_TO_TI mti
				join HARD_METERS hm on hm.Meter_id = mti.Meter_id 
				where mti.TI_ID = ti.TI_ID
				order by StartDateTime desc
			) im
			outer apply
			(
				select top 1 StartDateTime,TI_ID, COEFU*COEFI as Coeff from Info_Transformators
				where TI_ID = ti.TI_ID order by StartDateTime desc
			) it 
			where 
			TIName like @stLike	and 
			ti.[Deleted] <> 1 
			--Необязательные условия
			and (@TiTypeFilter is null or ti.TiType = @TiTypeFilter) --Фильтр по типу ТИ
			and (@IsExistsTIToExcept=0 or ti.TI_ID not in (select TI_ID from #tblTIToExcept)) --Если есть ТИ на исключение к поиску
			--and (@IsParrentFilterEnabled = 0 or ti.PS_ID in (select id from @parrentsList)) --Фильтр по родителю
			and (@isUseTreeId = 0 or ti.TI_ID in (select TI_ID from #tiFilter) or ti.PS_ID in (select PS_ID from #psFilter))--Нужно возвращать ТИ только из дерева

		order by TIName
	end
	else
	begin

			select top(@topFind) --тут вся выборка по fulltext
			ti.TI_ID as TI_ID,
			ti.TIName as TIName,
			ti.PS_ID as PS_ID, 
			ti.TIType,
			cast(ti.Commercial as bit) as Commercial,
			ti.Voltage,
			ISNULL(ti.SectionNumber, 0) as SectionNumber,
			Cast (0 as bit) as IsCa,
			ISNULL(ti.TPCoefOurSide,1) as TPCoef,
			cast (ISNULL(it.Coeff,1) as float(26)) as CoeffTrasformation,
			cast((case when ti.AIATSCode=2 then 1 else 0 end) as bit) as IsChanelReverse,
			ISNULL(ti.TP_ID,0) as TP_ID,
			0 as AdditionalParent,
			ti.Deleted as IsDeleted,
			case when im.MeterType_ID = 2012 then dbo.usf2_Hard_Meters_Addx_RemoteDisplay(im.MeterSerialNumber) 
			else im.MeterSerialNumber end as MeterSerialNumber,
			ti.PhaseNumber,
			dbo.usf2_Info_GetTariffsForTI(ti.TI_ID) as TariffArray,
			ti.CustomerKind,
			ti.AbsentChannelsMask,
			im.MeterModel_ID,
			im.AllowTariffWrite,
			ti.IsSmallTI,
			'' as Pik
			from Info_ti ti 
			outer apply (
				select top 1 hm.*
				from dbo.Info_Meters_TO_TI mti
				join HARD_METERS hm on hm.Meter_id = mti.Meter_id 
				where mti.TI_ID = ti.TI_ID
				order by StartDateTime desc
			) im
			outer apply
			(
				select top 1 StartDateTime,TI_ID, COEFU*COEFI as Coeff from Info_Transformators
				where TI_ID = ti.TI_ID order by StartDateTime desc
			) it 
			where ((CONTAINS(ti.TIName, @stContains)) and TIName like @stLike)
			and  ti.[Deleted] <> 1 
			--Необязательные условия
			and (@TiTypeFilter is null or ti.TiType = @TiTypeFilter) --Фильтр по типу ТИ
			and (@IsExistsTIToExcept=0 or ti.TI_ID not in (select TI_ID from #tblTIToExcept)) --Если есть ТИ на исключение к поиску
			and (@IsParrentFilterEnabled = 0 or ti.PS_ID in (select id from @parrentsList)) --Фильтр по родителю
			
			and (@isUseTreeId = 0 or ti.TI_ID in (select TI_ID from #tiFilter) or ti.PS_ID in (select PS_ID from #psFilter))--Нужно возвращать ТИ только из дерева
		order by TIName
	end
end
end
go
   grant EXECUTE on usp2_Info_FindTIbyParamName to [UserCalcService]
go
   grant EXECUTE on usp2_Info_FindTIbyParamName to [UserDeclarator]
go
 
  