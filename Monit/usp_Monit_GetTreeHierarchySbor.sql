if exists (select 1
          from sysobjects
          where  id = object_id('usp2_Monit_GetTreeHierarchySbor')
          and type in ('P','PC'))
   drop procedure usp2_Monit_GetTreeHierarchySbor
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
--		апрель, 2012
--
-- Описание:
--
--		Строим иерархию сбора счетчика
--
-- ======================================================================================
create proc [dbo].[usp2_Monit_GetTreeHierarchySbor]
	@TI_ID int, -- идентификатор точки
	@datestart DateTime,
	@dateend DateTime
as
begin

set nocount on
set quoted_identifier, ansi_nulls, ansi_warnings, arithabort, concat_null_yields_null, ansi_padding on
set numeric_roundabort off
set transaction isolation level read uncommitted

declare 
@Meter_ID int,
@Concentrator_ID int,
@PimaryTreeElement_ID uniqueidentifier,
@MaxPrimaryEventDate DateTime,
@MaxPrivaryDispatchDate DateTime;


set @Meter_ID = (select METER_ID from Info_Meters_TO_TI imtt where imtt.TI_ID = @TI_ID and imtt.StartDateTime = 
				(
					select max(StartDateTime)
					from dbo.Info_Meters_TO_TI
					where Info_Meters_TO_TI.TI_ID = @TI_ID
						and StartDateTime <= @dateend 
						and (FinishDateTime is null or FinishDateTime >= @datestart)
				));
	
	--Находим единственную запись где был обнаружен счетчик в самое последнее время	
	set @MaxPrimaryEventDate =(select Max(EventDateTime) from dbo.JournalDataCollect_Concentrators_Meters_Tree where Meter_ID = @Meter_ID);
	select  @MaxPrivaryDispatchDate = Max(DispatchDateTime), @Concentrator_ID = MAX(Concentrator_ID)
	from dbo.JournalDataCollect_Concentrators_Meters_Tree where Meter_ID=@Meter_ID
	and EventDateTime = @MaxPrimaryEventDate
	group by DispatchDateTime, Concentrator_ID;
	
	--Ищем счетчики через которые собирали
	WITH HierarchyTreeSbor (Concentrator_ID, EventDateTime, MeterSerialNumber, Meter_ID, ParentTreeElement_ID, TreeElement_ID, PLCMeterStatusFlags,  rn, MeterReadTimeout)
	as
	(
		SELECT child.Concentrator_ID, child.EventDateTime, child.MeterSerialNumber, child.Meter_ID, child.ParentTreeElement_ID, child.TreeElement_ID, child.PLCMeterStatusFlags, 0 as rn, child.MeterReadTimeout
		FROM [dbo].[JournalDataCollect_Concentrators_Meters_Tree] child
		where child.Concentrator_ID = @Concentrator_ID and child.Meter_ID=@Meter_ID and child.EventDateTime = @MaxPrimaryEventDate and DispatchDateTime = @MaxPrivaryDispatchDate

		UNION ALL --Рекурсия
		
		SELECT child.Concentrator_ID, child.EventDateTime, child.MeterSerialNumber, child.Meter_ID, child.ParentTreeElement_ID, child.TreeElement_ID, child.PLCMeterStatusFlags, c.rn + 1, child.MeterReadTimeout
		FROM [dbo].[JournalDataCollect_Concentrators_Meters_Tree] child
		join HierarchyTreeSbor c on c.Concentrator_ID = child.Concentrator_ID
		and c.ParentTreeElement_ID = child.TreeElement_ID-- and child.EventDateTime = c.EventDateTime
		where c.MeterSerialNumber <> child.MeterSerialNumber
	 )
		 
	select * into #tmpHeirarchy	from HierarchyTreeSbor h;
	declare @LastMeter int, @LastConcentrator int;
	select @LastMeter = Meter_ID, @LastConcentrator = Concentrator_ID
	from #tmpHeirarchy 
	where rn = (select MAX(rn) from #tmpHeirarchy)
	; --Последний счетчик по цепочке для него дальше строим дерево
	
	--Запоминаем дерево
	select @PimaryTreeElement_ID = TreeElement_ID, @Concentrator_ID = Concentrator_ID from #tmpHeirarchy where rn = 0;
	
	--Возвращаем дерево
	if (select Count(*) from #tmpHeirarchy) > 0 begin 
		--Если есть дерево возвращаем дерево
		select TI_ID, PLCMeterStatusFlags, EventDateTime, ParentTreeElement_ID, TreeElement_ID, rn, MeterReadTimeout from #tmpHeirarchy h
		cross apply
							(
								select top (1) *
								from dbo.Info_Meters_TO_TI
								where METER_ID = h.Meter_ID
									and StartDateTime <= @dateend 
									and (FinishDateTime is null or FinishDateTime >= @datestart)
								order by StartDateTime desc
							) imtt;
	end else begin 
		--Дерева нет, возвращаем входящее значение
		select @TI_ID as TI_ID, cast(0 as tinyint) as PLCMeterStatusFlags, 0 as rn
	end;	
	
	declare @commChannel smallint;
	
	if (@LastConcentrator IS NOT NULL) begin --Есть концентратор и ищем Е422
		select top 1 Concentrator_ID, StringName, LinkNumber, PS_ID
		from dbo.Hard_Concentrators hc
		join dbo.Hard_E422CommChannels_Links hl on hl.E422_ID = hc.E422_ID
		join dbo.Hard_CommChannels cc on cc.CommChannel_ID = hl.CommChannel_ID
		where Concentrator_ID = @LastConcentrator
	end else begin
		if (exists(select * from dbo.Hard_MetersE422_Links where Meter_ID = @Meter_ID))begin
			select top 1 -1 as Concentrator_ID, '' as StringName, -1 as LinkNumber, PS_ID
			from dbo.Hard_MetersE422_Links ml
			join dbo.Hard_E422CommChannels_Links hl on hl.E422_ID = ml.E422_ID
			join dbo.Hard_CommChannels cc on cc.CommChannel_ID = hl.CommChannel_ID
			where ml.Meter_ID = @Meter_ID
		end else begin
			declare @ps_id int;
			set @ps_id = (select top 1 PS_ID
			from dbo.Hard_MetersUSPD_Links ml
			join dbo.Hard_USPDCommChannels_Links hl on hl.USPD_ID = ml.USPD_ID
			join dbo.Hard_CommChannels cc on cc.CommChannel_ID = hl.CommChannel_ID
			where ml.Meter_ID = @Meter_ID)
			
			if (@ps_id is null) set @ps_id = (select ps_id from Info_TI where TI_ID = @TI_ID);
			
			select top 1 -1 as Concentrator_ID, '' as StringName, -1 as LinkNumber, @ps_id as PS_ID
		end;
	end;
	

--Строим резервное дерево

declare
@SecondaryParentTreeElement_ID uniqueidentifier,
@SecondaryEventDateTime DateTime;	

--Выбираем все возможные родители
select ParentTreeElement_ID, EventDateTime 
into #secondaryParrents
from JournalDataCollect_Concentrators_Meters_Tree
	where 
	Concentrator_ID = @Concentrator_ID
	and EventDateTime between @datestart and @dateend 
	and TreeElement_ID <> @PimaryTreeElement_ID
	and Meter_ID = @Meter_ID
	and PLCMeterStatusFlags = 0
	and TreeElement_ID = ParentTreeElement_ID
--order by EventDateTime desc;

declare
@secondaryCount int;

set @secondaryCount = (select Count(*) as secondaryCount from #secondaryParrents)

select @secondaryCount as secondaryCount

--Проверяем есть ли остальные родители
if @secondaryCount > 0 begin
	--Отфильтровываем последнего удачного родителя
	select @SecondaryParentTreeElement_ID = TreeElement_ID, @SecondaryEventDateTime = EventDateTime 
	from
	(
	select top 1 TreeElement_ID, j.EventDateTime 
	from JournalDataCollect_Concentrators_Meters_Tree j
	join #secondaryParrents s
	on j.TreeElement_ID = s.ParentTreeElement_ID and j.EventDateTime = s.EventDateTime
	and j.EventDateTime between @datestart and @dateend and j.Meter_ID not in (select Meter_ID from #tmpHeirarchy)
	and j.PLCMeterStatusFlags = 0
	where Concentrator_ID = @Concentrator_ID
	order by j.EventDateTime desc
	)jj;

	select @SecondaryParentTreeElement_ID as SecondaryParentTreeElement_ID;

	--Ищем счетчики через которые собирали, начиная от найденного вторичного родителя

	if (@SecondaryParentTreeElement_ID is not null) begin
		--Есть из чего выбирать
		WITH SecondaryHierarchyTreeSbor (Concentrator_ID, EventDateTime, MeterSerialNumber, Meter_ID, ParentTreeElement_ID, TreeElement_ID, PLCMeterStatusFlags,  rn)
		as
		(
			SELECT c.Concentrator_ID, c.EventDateTime, c.MeterSerialNumber, c.Meter_ID, c.ParentTreeElement_ID, c.TreeElement_ID, c.PLCMeterStatusFlags, 1 as rn
			FROM [dbo].[JournalDataCollect_Concentrators_Meters_Tree] c
			where c.Concentrator_ID = @Concentrator_ID and c.EventDateTime = @SecondaryEventDateTime and c.TreeElement_ID = @SecondaryParentTreeElement_ID  and PLCMeterStatusFlags = 0

			UNION ALL --Рекурсия
			
			SELECT child.Concentrator_ID, child.EventDateTime, child.MeterSerialNumber, child.Meter_ID, child.ParentTreeElement_ID, child.TreeElement_ID, child.PLCMeterStatusFlags, c.rn + 1
			FROM [dbo].[JournalDataCollect_Concentrators_Meters_Tree] child
			join SecondaryHierarchyTreeSbor c on c.Concentrator_ID = child.Concentrator_ID
			and  c.ParentTreeElement_ID = child.TreeElement_ID 
			and child.EventDateTime = c.EventDateTime
			and child.Concentrator_ID = @Concentrator_ID
			where c.MeterSerialNumber <> child.MeterSerialNumber
		 )

		
		select TI_ID, PLCMeterStatusFlags, EventDateTime, ParentTreeElement_ID, TreeElement_ID, rn from SecondaryHierarchyTreeSbor h
		cross apply
							(
								select top (1) *
								from dbo.Info_Meters_TO_TI
								where METER_ID = h.Meter_ID
									and StartDateTime <= @dateend 
									and (FinishDateTime is null or FinishDateTime >= @datestart)
								order by StartDateTime desc
							) imtt;
	end;
end;
drop table #tmpHeirarchy;
drop table #secondaryParrents;
end
go
   grant EXECUTE on usp2_Monit_GetTreeHierarchySbor to [UserCalcService]
go



