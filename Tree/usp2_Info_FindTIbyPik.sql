if exists (select 1
          from sysobjects
          where  id = object_id('usp2_Info_FindTIbyPik')
          and type in ('P','PC'))
   drop procedure usp2_Info_FindTIbyPik
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
--		Сентябрь, 2016
--
-- Описание:
--
--		Поиск по комментарию для ТИ
--
-- ======================================================================================
create proc [dbo].[usp2_Info_FindTIbyPik]
(
	@parrentHirerarchy int,
	@searchText nvarchar(1000),	--Текст для поиска
	@parrentsList IntType readonly, --Родители по которым фильтруем
	@topFind int = 100, -- Предельное значение для поиска
	@TIArrayExcepted varchar(4000) = null, -- Точки измерения, которые исключаем к запросу
	@TiTypeFilter tinyint = null --Фильтр типа ТИ по которому нужен отбор
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
declare @IsExistsTIToExcept bit;

if (@TIArrayExcepted is not null AND LEN(@TIArrayExcepted) > 1) begin
	set @IsExistsTIToExcept = 1;
	
	insert into #tblTIToExcept
	select Items from usf2_Utils_Split(@TIArrayExcepted, ',');
end else begin
	set @IsExistsTIToExcept = 0;
end

--set @searchText = '"' + @searchText + '*"';
declare @st nvarchar(1000);
set @st = '"' + @searchText + '*"';

select top (@topFind) ti.TI_ID as TI_ID,
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
	c.Comment as Pik
from [dbo].[Info_Balance_FreeHierarchy_Comment] c
join [dbo].[Info_Balance_FreeHierarchy_Objects] o on o.BalanceFreeHierarchyObject_UN = c.BalanceFreeHierarchyObject_UN
join Info_ti ti on ti.TI_ID = o.TI_ID and [Deleted] <> 1 
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

--where CommentType = 1 and Contains(Comment, @searchText) and o.TI_ID is not null
where CommentType = 1 and Contains(Comment, @st) and o.TI_ID is not null and Comment like '%' + @searchText + '%'

and (@TiTypeFilter is null or ti.TiType = @TiTypeFilter)
and (@IsExistsTIToExcept = 0 or ti.TI_ID not in (select TI_ID from #tblTIToExcept))
and (@IsParrentFilterEnabled = 0 or ti.PS_ID in (select id from @parrentsList))


end
go
   grant EXECUTE on usp2_Info_FindTIbyPik to [UserCalcService]
go
