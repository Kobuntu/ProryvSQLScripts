if exists (select 1
          from sysobjects
          where  id = object_id('usf2_Info_GetSectionStatuses')
          and type in ('IF', 'FN', 'TF'))
   drop function usf2_Info_GetSectionStatuses
go

-- ======================================================================================
-- Автор:
--
--		Малышев Игорь
--
-- Дата создания:
--
--		Март, 2018
--
-- Описание:
--
--		Определяем есть ли для объекта сечения
--
-- ======================================================================================

create FUNCTION [dbo].[usf2_Info_GetSectionStatuses]
(	
	@ID int, --Идентификатор объекта
	@HierarchyType tinyint = 3 --Тип объекта 
)
RETURNS int
AS
begin

	if (@HierarchyType = 0) begin
		--Есть собственные сечения
		if (exists(select top 1 1 from Info_Section_List 
			where HierLev1_ID = @ID)) return 8;

		--Больше нет необходимости читать статусы дочерних
		----Есть у дочерних объектов
		--if (exists(select top 1 1 from Info_Section_List 
		--	where HierLev2_ID in (select HierLev2_ID from Dict_HierLev2 
		--		where HierLev1_ID = @ID))) 
		--	 return 8;

		----Есть у дочерних дочерних объектов
		--if (exists(select top 1 1 from Info_Section_List 
		--	where HierLev3_ID in (select HierLev3_ID from Dict_HierLev3 
		--		where HierLev2_ID in (select HierLev2_ID from Dict_HierLev2 
		--			where HierLev1_ID = @ID)))) 
		--	 return 8;

		----Есть у дочерних дочерних дочерних объектов
		--if (exists(select top 1 1 from Info_Section_List 
		--	where PS_ID in (select PS_ID from Dict_PS 
		--		where HierLev3_ID in (select HierLev3_ID from Dict_HierLev3 
		--			where HierLev2_ID in (select HierLev2_ID from Dict_HierLev2 
		--				where HierLev1_ID = @ID))))) 
		--	return 8;

	end else if (@HierarchyType = 1) begin

		--Есть собственные сечения
		if (exists(select top 1 1 from Info_Section_List 
			where HierLev2_ID = @ID)) return 8;

		--Больше нет необходимости читать статусы дочерних
		----Есть у дочерних объектов
		--if (exists(select top 1 1 from Info_Section_List 
		--	where HierLev3_ID in (select HierLev3_ID from Dict_HierLev3 
		--		where HierLev2_ID = @ID))) 
		--	 return 8;

		----Есть у дочерних дочерних объектов
		--if (exists(select top 1 1 from Info_Section_List 
		--	where PS_ID in (select PS_ID from Dict_PS 
		--		where HierLev3_ID in (select HierLev3_ID from Dict_HierLev3 
		--			where HierLev2_ID = @ID)))) 
		--	 return 8;

	end else if (@HierarchyType = 2) begin

		--Есть собственные сечения
		if (exists(select top 1 1 from Info_Section_List 
			where HierLev3_ID = @ID)) return 8;

		--Больше нет необходимости читать статусы дочерних
		----Есть у дочерних объектов
		--if (exists(select top 1 1 from Info_Section_List 
		--	where PS_ID in (select PS_ID from Dict_PS 
		--		where HierLev3_ID = @ID))) 
		--	 return 8;

	end else if (@HierarchyType = 3) begin
		if (exists(select top 1 1 from Info_Section_List 
			where PS_ID = @ID)) return 8;
	end

	return 0;
end
go
grant EXECUTE on usf2_Info_GetSectionStatuses to [UserCalcService]
go


if exists (select 1
          from sysobjects
          where  id = object_id('usf2_Info_GetTIStatusForPS')
          and type in ('IF', 'FN', 'TF'))
   drop function usf2_Info_GetTIStatusForPS
go

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
--		Определяем какие типы точек присутствуют на ПС
--
-- ======================================================================================

create FUNCTION [dbo].[usf2_Info_GetTIStatusForPS]
(	
	@ID int, --Идентификатор объекта
	@HierarchyType tinyint = 3 --Тип объекта 
)
RETURNS int
AS
begin
declare 
@result smallint

set  @result = 0;
		if (@HierarchyType = 3) begin
			--Проверка на точек на основе месячного расхода энергии
			select @result = @result |
			case when TIType = 2 and TreeCategory is not null then TreeCategory | 4
			 when TIType = 2 and TreeCategory is null then 128 | 4 -- TreeElectricity
			 when TreeCategory is not null then TreeCategory 
			 else 128 -- TreeElectricity
		end
		from info_ti where ps_id = @ID and (TreeCategory is not null or TIType = 2)
	end
	return @result
end
go
grant EXECUTE on usf2_Info_GetTIStatusForPS to [UserCalcService]
go

--Использующие процедуры

if exists (select 1
          from sysobjects
          where  id = object_id('usp2_Info_GetGlobalTree')
          and type in ('P','PC'))
   drop procedure usp2_Info_GetGlobalTree
go

SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO


create proc [dbo].[usp2_Info_GetGlobalTree]

as
begin
	set nocount on
	set quoted_identifier, ansi_nulls, ansi_warnings, arithabort, concat_null_yields_null, ansi_padding on
	set numeric_roundabort off
	set transaction isolation level read uncommitted

	--Собираем признаки принадлежности особенным деревьям и наличию малых ТИ
	select PS_ID, ISNULL(Max(TreeCategory & 128) + Max(TreeCategory & 256) + Max(TreeCategory & 512) + Max(TreeCategory & 1024), 0) --Это категории дерева
		+ Max(case when TIType = 2 then 4 else 0 end) --Это признак что есть малые ТИ
		as DBStatus 
	into #tistatuses from Info_TI group by PS_ID

	--Собираем все ПС, у которых есть УСПД
	select distinct Hard_CommChannels.PS_ID, 32 as DBStatus 
	into #pssWithUspd
	from hard_uspd 
	join Hard_USPDCommChannels_Links on Hard_USPDCommChannels_Links.USPD_ID=Hard_USPD.USPD_ID 
	join Hard_CommChannels on Hard_USPDCommChannels_Links.CommChannel_ID=Hard_CommChannels.CommChannel_ID 

	--Собираем все ПС, у которых есть Е422
	select distinct Hard_CommChannels.PS_ID, 64 as DBStatus  
	into #pssWithE422
	from Hard_E422 
	join Hard_E422CommChannels_Links on Hard_E422CommChannels_Links.E422_ID=Hard_E422.E422_ID 
	join Hard_CommChannels on Hard_E422CommChannels_Links.CommChannel_ID=Hard_CommChannels.CommChannel_ID 

	--Собираем признаки наличия сечений
	select distinct PS_ID, 
	(case when PS_ID is null then HierLev3_ID else null end) as HierLev3_ID, 
	(case when PS_ID is null and HierLev3_ID is null then HierLev2_ID else null end) as HierLev2_ID, 
	(case when PS_ID is null and HierLev3_ID is null and HierLev2_ID is null then HierLev1_ID else null end) as HierLev1_ID, 8 as DBStatus
	into #sections
	from Info_Section_List 
	group by PS_ID, HierLev3_ID, HierLev2_ID, HierLev1_ID

	--Уровень ПС
	select ps.PS_ID as Id, StringName as Name,ps.Description, cast(ISNULL(ps.HierLev3_ID, -1) as int) as ParentId,3 as DBType, 
			PSProperty, PSVoltage, PSType, 
			ISNULL(t.DBStatus, 0) | ISNULL(uspd.DBStatus, 0) | ISNULL(e422.DBStatus, 0) | ISNULL(s.DBStatus, 0)  as DBStatus
			into #pss
			from Dict_PS ps
			left join #tistatuses t on t.PS_ID = ps.PS_ID
			left join #pssWithUspd uspd on uspd.PS_ID = ps.PS_ID
			left join #pssWithE422 e422 on e422.PS_ID = ps.PS_ID
			left join #sections s on s.PS_ID = ps.PS_ID
			order by ps.HierLev3_ID, ps.PS_ID

	drop table #pssWithUspd
	drop table #pssWithE422
	drop table #tistatuses
	
	--Группируем по HierLev3_ID
	select ParentId, Max(DBStatus & 1) + Max(DBStatus & 2) + Max(DBStatus & 4) + Max(DBStatus & 8) + Max(DBStatus & 16) +
		Max(DBStatus & 32) + Max(DBStatus & 64) + Max(DBStatus & 128) + Max(DBStatus & 256) + Max(DBStatus & 512) + Max(DBStatus & 1024) as DBStatus
	into #Hier3Statuses
	from #pss
	group by ParentId


	--Уровень 3
	select h3.HierLev3_ID as Id, StringName as Name,h3.Description, cast(h3.HierLev2_ID as int) as ParentId,
		--dbo.usf2_Info_GetSectionStatuses(HierLev3_ID, 2) | 
		ISNULL(s.DBStatus, 0) |
		ISNULL((select top 1 DBStatus from #Hier3Statuses where ParentId = h3.HierLev3_ID),0) as DBStatus
		into #hiers3
		from Dict_HierLev3 h3
		left join #sections s on s.HierLev3_ID = h3.HierLev3_ID
		order by h3.HierLev2_ID, h3.HierLev3_ID

	drop table #Hier3Statuses

	--Группируем по HierLev2_ID
	select ParentId, Max(DBStatus & 1) + Max(DBStatus & 2) + Max(DBStatus & 4) + Max(DBStatus & 8) + Max(DBStatus & 16)  +
		Max(DBStatus & 32) + Max(DBStatus & 64) + Max(DBStatus & 128) + Max(DBStatus & 256) + Max(DBStatus & 512) + Max(DBStatus & 1024) as DBStatus
	into #Hier2Statuses
	from #hiers3
	group by ParentId

	--Уровень 2
	select h2.HierLev2_ID as Id, StringName as Name,h2.Description, cast(h2.HierLev1_ID as int) as ParentId,
	--dbo.usf2_Info_GetSectionStatuses(HierLev2_ID, 1)| 
	ISNULL(s.DBStatus, 0) |
	ISNULL((select top 1 DBStatus from #Hier2Statuses where ParentId = h2.HierLev2_ID),0) 
	as DBStatus
		into #hiers2
		from Dict_HierLev2 h2
		left join #sections s on s.HierLev2_ID = h2.HierLev2_ID
		order by h2.HierLev1_ID, h2.HierLev2_ID


	drop table #Hier2Statuses

	--Группируем по HierLev1_ID
	select ParentId, Max(DBStatus & 1) + Max(DBStatus & 2) + Max(DBStatus & 4) + Max(DBStatus & 8) + Max(DBStatus & 16)  +
		Max(DBStatus & 32) + Max(DBStatus & 64) + Max(DBStatus & 128) + Max(DBStatus & 256) + Max(DBStatus & 512) + Max(DBStatus & 1024) as DBStatus
	into #Hier1Statuses
	from #hiers2
	group by ParentId

	--Возвращаем результат 

	select * from #pss

	select * from #hiers3

	select * from #hiers2

	--Уровень 1
	select cast(h1.HierLev1_ID as int) as Id, StringName as Name,h1.Description, cast(0 as int) as ParentId,
	ISNULL(s.DBStatus, 0) |
	ISNULL((select top 1 DBStatus from #Hier1Statuses where ParentId = h1.HierLev1_ID),0) 
	as DBStatus
	from Dict_HierLev1 h1
	left join #sections s on s.HierLev1_ID = h1.HierLev1_ID
	order by h1.HierLev1_ID

	--drop table #Hier1Statuses
	drop table #pss
	drop table #hiers3
	drop table #hiers2

	drop table #sections

	--Классификации формул
	select * from Dict_FormulaClassification;

	select * from Dict_Hier_Names;

	
end
go
   grant EXECUTE on usp2_Info_GetGlobalTree to [UserCalcService]
go
