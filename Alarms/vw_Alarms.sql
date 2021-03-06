if exists (select 1
          from sysobjects
          where  id = object_id('usp2_FreeHierarchy_UpdateIncludedObjectChildren')
          and type in ('P','PC'))
   drop procedure usp2_FreeHierarchy_UpdateIncludedObjectChildren
go

if exists (select 1
          from sysobjects
          where  id = object_id('usf2_FreeHierarchyTree')
          and type in ('IF', 'FN', 'TF'))
   drop function usf2_FreeHierarchyTree
go

if exists (select 1
          from sysobjects
          where  id = object_id('usf2_FreeHierarchyTreeStandart')
          and type in ('IF', 'FN', 'TF'))
   drop function usf2_FreeHierarchyTreeStandart
go

--Эта часть относится к дереву иерархии
if exists (select 1
          from sysobjects
          where  id = object_id('usp2_Info_FindHierObject')
          and type in ('P','PC'))
   drop procedure usp2_Info_FindHierObject
go

if exists (select 1
          from sysobjects
          where  id = object_id('usf2_FreeHierarchy_GetStringPathFromNumeric')
          and type in ('IF', 'FN', 'TF'))
   drop function usf2_FreeHierarchy_GetStringPathFromNumeric
go

if exists (select 1
          from sysobjects
          where  id = object_id('usf2_Utils_iter_freeHierPath_to_table')
          and type in ('IF', 'FN', 'TF'))
   drop function usf2_Utils_iter_freeHierPath_to_table
go

if exists (select 1
          from sysobjects
          where  id = object_id('usf2_FreeHierarchyStandartObject')
          and type in ('IF', 'FN', 'TF'))
   drop function usf2_FreeHierarchyStandartObject
go

if exists (select 1
          from sysobjects
          where  id = object_id('vw_HierarchyObjects')
          and type in ('V'))
   drop view vw_HierarchyObjects
go

if exists (select 1
          from sysobjects
          where  id = object_id('vw_FreeHierarchyObjects')
          and type in ('V'))
   drop view vw_FreeHierarchyObjects
go

if exists (select 1
          from sysobjects
          where  id = object_id('usf2_Utils_ConvertFreeHierarchyToHierarchy')
          and type in ('IF', 'FN', 'TF'))
   drop function usf2_Utils_ConvertFreeHierarchyToHierarchy
go

-----------------------------------------------------

if exists (select 1
          from sysobjects
          where  id = object_id('vw_Alarms')
          and type in ('V'))
   drop view vw_Alarms
go

if exists (select 1
          from sysobjects
          where  id = object_id('vw_Alarms_Alexander')
          and type in ('V'))
   drop view vw_Alarms_Alexander
go

if exists (select 1
          from sysobjects
          where  id = object_id('usf2_Alarm_ObjectInfo')
          and type in ('IF', 'FN', 'TF'))
   drop function usf2_Alarm_ObjectInfo
go

if exists (select 1
          from sysobjects 
          where  id = object_id('usf2_FreeHierarchyObjectInfo')
          and type in ('IF', 'FN', 'TF'))
   drop function usf2_FreeHierarchyObjectInfo
go

-- ======================================================================================
-- Автор:
--
--		Малышев Игорь
--
-- Дата создания:
--
--		Август 2012
--
-- Описание:
--
--		Возвращыем информацию об объекте из таблицы Info_Balance_FreeHierarchy_Objects
--
-- ======================================================================================
CREATE FUNCTION [dbo].[usf2_FreeHierarchyObjectInfo]
(
	@FreeHierItem_ID int,
	@HierLev1_ID tinyint, 
	@HierLev2_ID int, 
	@HierLev3_ID int, 
	@PS_ID int, 
	@TI_ID int 
	--@Formula_UN varchar(22), 
	--@Section_ID int,  
	--@TP_ID int, 
	--@USPD_ID int, 
	--@XMLSystem_ID int, 
	--@JuridicalPerson_ID int, 
	--@JuridicalPersonContract_ID int, 
	--@DistributingArrangement_ID int, 
	--@BusSystem_ID int, 
	--@UANode_ID bigint, 
	--@OurFormula_UN varchar(22), 
	--@ForecastObject_UN varchar(22)
)
--returns @objectTable TABLE
--(
--	[ID] nvarchar(22), --Идентификатор объекта
--	[TypeHierarchy] tinyint, --Тип объекта, должен соответствовать enumTypeHierarchy 
--	[ObjectName] nvarchar(255) --Название объекта
--)
RETURNS TABLE
WITH SCHEMABINDING
AS
RETURN
	--if (@TI_ID is not null) begin 

		--insert into @objectTable 
		select top 1 o.TI_ID as ID, 4 as TypeHierarchy, o.TIName as ObjectName
		from [dbo].Info_TI o  WITH (NOLOCK)
		where @TI_ID is not null and o.TI_ID = @TI_ID

	--end else if (@PS_ID is not null) begin 

		union all

		--insert into @objectTable 
		select top 1 o.PS_ID,  3 as TypeHierarchy, o.StringName as ObjectName
		from [dbo].Dict_PS o  WITH (NOLOCK)
		where @PS_ID is not null and @TI_ID is null and o.PS_ID = @PS_ID

	--end	else if (@HierLev3_ID is not null) begin 
	
		union all

		--insert into @objectTable 
		select top 1 o.HierLev3_ID, 2, o.StringName  
		from [dbo].Dict_HierLev3 o  WITH (NOLOCK)
		where @HierLev3_ID is not null and @PS_ID is null and @TI_ID is null and o.HierLev3_ID = @HierLev3_ID

	--end else if (@HierLev2_ID is not null) begin
		union all

		--insert into @objectTable 
		select top 1 o.HierLev2_ID, 1, o.StringName  
		from [dbo].Dict_HierLev2 o  WITH (NOLOCK)
		where @HierLev2_ID is not null and @HierLev3_ID is null and @PS_ID is null and @TI_ID is null and o.HierLev2_ID = @HierLev2_ID

	--end else if (@HierLev1_ID is not null) begin

		union all
		--insert into @objectTable 
		select top 1 o.HierLev1_ID, 0, o.StringName 
		from [dbo].Dict_HierLev1 o  WITH (NOLOCK)
		where @HierLev1_ID is not null and @HierLev2_ID is null and @HierLev3_ID is null and @PS_ID is null and @TI_ID is null and o.HierLev1_ID = @HierLev1_ID
	
	--end else if (@FreeHierItem_ID is not null) begin

		union all
		--insert into @objectTable 
		select top 1 o.FreeHierItem_ID, 28, o.StringName   
		from [dbo].Dict_FreeHierarchyTree o  WITH (NOLOCK)
		where @FreeHierItem_ID is not null 
		and @HierLev1_ID is null and @HierLev2_ID is null and @HierLev3_ID is null and @PS_ID is null and @TI_ID is null 
		and o.FreeHierItem_ID = @FreeHierItem_ID

	--end
	--RETURN
--END
go
grant select on usf2_FreeHierarchyObjectInfo to [UserCalcService]
go
grant select on usf2_FreeHierarchyObjectInfo to [UserDeclarator]
go

-- ======================================================================================
-- Автор:
--
--		Малышев Игорь
--
-- Дата создания:
--
--		Сентябрь, 2019
--
-- Описание:
--
--		Конвертируем тип EnumFreeHierarchyItemType в enumTypeHierarchy

--
-- ======================================================================================

CREATE FUNCTION [dbo].[usf2_Utils_ConvertFreeHierarchyToHierarchy]
(	
	@FreeHierItemType tinyint --Тип в свободной иерархии
)
RETURNS tinyint
WITH SCHEMABINDING
AS
begin
	return case @FreeHierItemType
		when 1 then 0	--Dict_HierLev1
		when 2 then 1	--Dict_HierLev2
		when 3 then 2	--Dict_HierLev3
		when 4 then 3	--Dict_PS
		when 5 then 4	--Info_TI
		when 7 then 5	--Section
		when 8 then 8	--Info_TP
		when 14 then 9	--Formula_TP_OurSide
		when 15 then 10	--Formula_TP_CA
		when 6 then 11	--Formula
		when 11 then 16	--Dict_DirectConsumer
		when 13 then 19	--JuridicalPerson
		when 10 then 20	--Dict_JuridicalPersons_Contract
		when 0 then 28	--Node
		when 23 then 33	--UANode
		when 28 then 41	--FormulaConstant
		when 29 then 42	--ForecastObject
	end

	return 0
end
go
grant EXECUTE on usf2_Utils_ConvertFreeHierarchyToHierarchy to [UserCalcService]
go

grant EXECUTE on usf2_Utils_ConvertFreeHierarchyToHierarchy to [UserDeclarator]
go

--Эта часть относится к дереву иерархии

-- ======================================================================================
-- Автор:
--
--		Малышев Игорь
--
-- Дата создания:
--
--		Сентябрь, 2019
--
-- Описание:
--
--		Объекты из дерева свободной иерархии
--
-- ======================================================================================
create view [dbo].[vw_FreeHierarchyObjects] 
WITH SCHEMABINDING
AS
	--Узлы в дереве свободной иерархии
	select f.FreeHierTree_ID
	,case f.FreeHierItemType 
		when 0 then d.FreeHierItem_ID 
		when 1 then d.HierLev1_ID 
		when 2 then d.HierLev2_ID 
		when 3 then d.HierLev3_ID 
		when 4 then d.PS_ID 
		when 5 then d.TI_ID 
		--when 6 then d.Formula_UN
		when 9 then  d.USPD_ID 
		when 7 then  d.Section_ID 
		when 8 then  d.TP_ID 
		when 10 then d.JuridicalPersonContract_ID 
		when 12 then d.XMLSystem_ID 
		when 13 then d.JuridicalPerson_ID 
		when 18 then d.DistributingArrangement_ID 
		when 19 then d.BusSystem_ID 
		when 23 then d.UANode_ID 
		--when 14 then d.OurFormula_UN
		--when 29 then d.ForecastObject_UN
		else null
	end as ID
	,case f.FreeHierItemType 
		when 6 then d.Formula_UN
		when 14 then d.OurFormula_UN
		when 29 then d.ForecastObject_UN
		else ''
	end as StringId
		,dbo.usf2_Utils_ConvertFreeHierarchyToHierarchy(f.FreeHierItemType) as TypeHierarchy
		,f.FreeHierItemType 
		,f.[FreeHierItem_ID]
		,f.HierID.GetAncestor(1) as ParentHierID
		,f.StringName
		,d.IncludeObjectChildren
		,f.HierID
	from [dbo].[Dict_FreeHierarchyTree] f
	join [dbo].[Dict_FreeHierarchyTree_Description] d on d.FreeHierItem_ID = f.FreeHierItem_ID
	
GO
grant select on vw_FreeHierarchyObjects to [UserCalcService]
go
grant select on vw_FreeHierarchyObjects to [UserDeclarator]
go

-- ======================================================================================
-- Автор:
--
--		Малышев Игорь
--
-- Дата создания:
--
--		Декабрь 2019
--
-- Описание:
--
--		Аналог vw_HierarchyObjects, информация по стандартному объекту
--
-- ======================================================================================
create FUNCTION [dbo].[usf2_FreeHierarchyStandartObject]
(
	@TypeHierarchy int,
	@ID int, 
	@StringId varchar(22) 
)

RETURNS TABLE
WITH SCHEMABINDING
AS
RETURN
	--Объекты уровня 1
	select top 1 [HierLev1_ID] as ID, null as StringId, 0 as TypeHierarchy, StringName, 'Dict_HierLev1_' as ObjectTypeName
		,cast([HierLev1_ID] as varchar(255)) as MeterSerialNumber
		,null as ParentID, null as ParentTypeHierarchy --Информация о родителях
		,1 as FreeHierItemType
		,NULL as ParentName
		,null as ParentFreeHierItemType
	from [dbo].[Dict_HierLev1] with (nolock)
	where @TypeHierarchy = 0 and [HierLev1_ID] = @ID

	union all

	--Объекты уровня 2
	select top 1 [HierLev2_ID] as ID, null as StringId, 1 as TypeHierarchy, h2.StringName, 'Dict_HierLev2_' as ObjectTypeName
		,cast([HierLev2_ID] as varchar(255)) as MeterSerialNumber
		,h2.HierLev1_ID as ParentID, 0 as ParentTypeHierarchy
		,2 as FreeHierItemType
		,h1.StringName as ParentName
		,1 as ParentFreeHierItemType
	from [dbo].[Dict_HierLev2] h2 with (nolock)
	join [dbo].[Dict_HierLev1] h1 with (nolock) on h1.HierLev1_ID = h2.HierLev1_ID
	where @TypeHierarchy = 1 and [HierLev2_ID] = @ID

	union all

	--Объекты уровня 3
	select top 1 [HierLev3_ID] as ID, null as StringId, 2 as TypeHierarchy, h3.StringName, 'Dict_HierLev3' as ObjectTypeName
		,cast([HierLev3_ID] as varchar(255)) as MeterSerialNumber
		,h3.HierLev2_ID as ParentID, 1 as ParentTypeHierarchy
		,3 as FreeHierItemType
		,h2.StringName as ParentName
		,2 as ParentFreeHierItemType
	from [dbo].[Dict_HierLev3] h3 with (nolock)
	join [dbo].[Dict_HierLev2] h2 with (nolock) on h2.HierLev2_ID = h3.HierLev2_ID
	where @TypeHierarchy = 2 and [HierLev3_ID] = @ID

	union all

	--Объекты уровня 4
	select top 1 [PS_ID] as ID, null as StringId, 3 as TypeHierarchy, p.StringName, 'Dict_PS_' as ObjectTypeName
		,cast([PS_ID] as varchar(255)) as MeterSerialNumber
		,p.HierLev3_ID as ParentID, 2 as ParentTypeHierarchy
		,4 as FreeHierItemType
		,h3.StringName as ParentName
		,3 as ParentFreeHierItemType
	from [dbo].[Dict_PS] p with (nolock)
	join [dbo].[Dict_HierLev3] h3 with (nolock) on h3.HierLev3_ID = p.HierLev3_ID
	where @TypeHierarchy = 3 and [PS_ID] = @ID

	union all

	--Объекты ТИ
	select top 1 ti.[TI_ID] as ID, null as StringId, 4 as TypeHierarchy, ti.TIName as StringName, 'Info_TI' as ObjectTypeName
		,im.[MeterSerialNumber]
		,ti.PS_ID as ParentID, 3 as ParentTypeHierarchy
		,5 as FreeHierItemType
		,ps.StringName as ParentName
		,4 as ParentFreeHierItemType
	from [dbo].[Info_TI] ti with (nolock)
	join [dbo].[Dict_PS] ps with (nolock) on ps.PS_ID = ti.PS_ID
	outer apply (
				select top 1 hm.MeterSerialNumber
				from dbo.Info_Meters_TO_TI mti
				join dbo.HARD_METERS hm on hm.Meter_id = mti.Meter_id 
				where mti.TI_ID = ti.TI_ID
				order by StartDateTime desc
			) im
	where @TypeHierarchy = 4 and [TI_ID] = @ID

	union all

	--Объекты ТП
	select top 1 tp.[TP_ID] as ID, null as StringId, 8 as TypeHierarchy, StringName, 'Info_TP2' as ObjectTypeName
		,cast(tp.[TP_ID] as varchar(255)) as MeterSerialNumber
		,sd.Section_ID as ParentID, 5 as ParentTypeHierarchy
		,8 as FreeHierItemType
		,s.SectionName as ParentName
		,7 as ParentFreeHierItemType
	from [dbo].[Info_TP2] tp with (nolock)
	join [dbo].[Info_Section_Description2] sd on tp.TP_ID = sd.TP_ID
	join [dbo].[Info_Section_List] s on sd.Section_ID = s.Section_ID
	where @TypeHierarchy = 8 and tp.[TP_ID] = @ID AND StartDateTime <= GETDATE() and (FinishDateTime is null or FinishDateTime >=GETDATE())
	
	union all

	--Объекты сечения
	select top 1 s.[Section_ID] as ID, null as StringId, 5 as TypeHierarchy, SectionName, 'Info_Section_List' as ObjectTypeName
		,cast(s.[Section_ID] as varchar(255)) as MeterSerialNumber
		,cast(o.ID as int) as ParentID, o.TypeHierarchy as ParentTypeHierarchy
		,7 as FreeHierItemType
		,o.ObjectName as ParentName
		,null as ParentFreeHierItemType
	from [dbo].[Info_Section_List] s with (nolock)
	outer apply [dbo].usf2_FreeHierarchyObjectInfo(NULL, s.HierLev1_ID, s.HierLev2_ID, s.HierLev3_ID, s.PS_ID, NULL) o
	where @TypeHierarchy = 5 and [Section_ID] = @ID

	union all

	--УСПД
	select top 1 uspd.[USPD_ID] as ID, null as StringId, 32 as TypeHierarchy, ps.StringName as StringName, 'Hard_USPD' as ObjectTypeName
		,case when USPDSerialNumber is not null then USPDSerialNumber else cast([USPDIPMain] as varchar(255)) end as MeterSerialNumber
		,ps.PS_ID as ParentID, 3 as ParentTypeHierarchy
		,9 as FreeHierItemType
		,ps.StringName as ParentName
		,null as ParentFreeHierItemType
	from [dbo].[Hard_USPD] uspd with (nolock)
	join [dbo].Hard_USPDCommChannels_Links l on l.USPD_ID=uspd.USPD_ID
	join [dbo].Hard_CommChannels c on c.CommChannel_ID=l.CommChannel_ID
	join [dbo].Dict_PS ps on ps.PS_ID = c.PS_ID
	where @TypeHierarchy = 32 and uspd.[USPD_ID] = @ID

	union all

	--E422
	select top 1 uspd.[E422_ID] as ID, null as StringId, 40 as TypeHierarchy, ps.StringName as StringName, 'Hard_E422' as ObjectTypeName
		,case when E422SerialNumber is not null then E422SerialNumber else cast([E422IPMain] as varchar(255)) end as MeterSerialNumber
		,ps.PS_ID as ParentID, 3 as ParentTypeHierarchy
		,30 as FreeHierItemType
		,ps.StringName as ParentName
		,null as ParentFreeHierItemType
	from [dbo].[Hard_E422] uspd with (nolock)
	join [dbo].Hard_E422CommChannels_Links l on l.E422_ID=uspd.E422_ID
	join [dbo].Hard_CommChannels c on c.CommChannel_ID=l.CommChannel_ID
	join [dbo].Dict_PS ps on ps.PS_ID = c.PS_ID
	where @TypeHierarchy = 40 and uspd.[E422_ID] = @ID

	union all

	--формулы
	select top 1 null as ID, [Formula_UN] as StringID, 11 as TypeHierarchy, [FormulaName] as StringName, 'Info_Formula_List' as ObjectTypeName
		,[Formula_UN] as MeterSerialNumber
		,cast(o.ID as int) as ParentID, o.TypeHierarchy as ParentTypeHierarchy
		,6 as FreeHierItemType
		,o.ObjectName as ParentName
		,null as ParentFreeHierItemType
	from [dbo].[Info_Formula_List] fl with (nolock)
	outer apply [dbo].usf2_FreeHierarchyObjectInfo(fl.FreeHierItem_ID, fl.HierLev1_ID, fl.HierLev2_ID, fl.HierLev3_ID, fl.PS_ID, fl.TI_ID) o
	where @TypeHierarchy = 11 and [Formula_UN] = @StringId

	union all

	--Константы
	select top 1 null as ID, [FormulaConstant_UN] as StringID, 41 as TypeHierarchy, [FormulaConstantName] as StringName, 'Info_Formula_Constants' as ObjectTypeName
		,[FormulaConstant_UN] as MeterSerialNumber
		,cast(o.ID as int) as ParentID, o.TypeHierarchy as ParentTypeHierarchy
		,28 as FreeHierItemType
		,o.ObjectName as ParentName
		,null as ParentFreeHierItemType
	from [dbo].[Info_Formula_Constants] fc with (nolock)
	join [dbo].[Info_Balance_FreeHierarchy_Objects] fl on fl.BalanceFreeHierarchyObject_UN = fc.BalanceFreeHierarchyObject_UN
	outer apply [dbo].usf2_FreeHierarchyObjectInfo(fl.FreeHierItem_ID, fl.HierLev1_ID, fl.HierLev2_ID, fl.HierLev3_ID, fl.PS_ID, fl.TI_ID) o
	where @TypeHierarchy = 41 and [FormulaConstant_UN] = @StringId

	union all

	--Балансы
	select top 1 null as ID, [b].[BalanceFreeHierarchy_UN] as StringID, 45 as TypeHierarchy, b.BalanceFreeHierarchyName as StringName, 'Info_Balance_FreeHierarchy_List' as ObjectTypeName
		,[b].[BalanceFreeHierarchy_UN] as MeterSerialNumber
		,cast(o.ID as int) as ParentID, o.TypeHierarchy as ParentTypeHierarchy
		,50 as FreeHierItemType
		,o.ObjectName as ParentName
		,null as ParentFreeHierItemType
	from [dbo].[Info_Balance_FreeHierarchy_List] b with (nolock)
	join [dbo].[Info_Balance_FreeHierarchy_Objects] fl on fl.BalanceFreeHierarchyObject_UN = b.BalanceFreeHierarchyObject_UN
	outer apply [dbo].usf2_FreeHierarchyObjectInfo(fl.FreeHierItem_ID, fl.HierLev1_ID, fl.HierLev2_ID, fl.HierLev3_ID, fl.PS_ID, fl.TI_ID) o
	where @TypeHierarchy = 45 and [BalanceFreeHierarchy_UN] = @StringId

	union all

	--Узлы в дереве свободной иерархии
	select top 1 t.ID as ID, t.StringId , 28 as TypeHierarchy, t.StringName, 'Dict_FreeHierarchyTree' as ObjectTypeName
		,cast(t.ID as varchar(255)) as MeterSerialNumber
		,p.ID as ParentID, p.TypeHierarchy as ParentTypeHierarchy
		,0 as FreeHierItemType
		,p.StringName as ParentName
		,p.FreeHierItemType as ParentFreeHierItemType
	from dbo.vw_FreeHierarchyObjects t with (nolock)
	left join dbo.vw_FreeHierarchyObjects p on t.ParentHierID is not null and p.FreeHierTree_ID = t.FreeHierTree_ID and p.HierID = t.ParentHierID
	where @TypeHierarchy = 28 and t.ID = @ID and t.FreeHierItemType = 0
go
grant select on usf2_FreeHierarchyStandartObject to [UserCalcService]
go
grant select on usf2_FreeHierarchyStandartObject to [UserDeclarator]
go

-- ======================================================================================
-- Автор:
--
--		Малышев Игорь
--
-- Дата создания:
--
--		Июль, 2018
--
-- Описание:
--
-- Дополнительная информация по объекту с аварией
-- ======================================================================================
CREATE FUNCTION [dbo].[usf2_Alarm_ObjectInfo] (
			@Alarm_ID uniqueidentifier --Идентификатор аварии
)	
	RETURNS TABLE 
	WITH SCHEMABINDING
AS
RETURN
	--Авария по ТИ-------------------------------------
	select top 1 Convert(nvarchar(22), a.TI_ID) as ID, Convert(tinyint, 4) as TypeHierarchy, ti.TIName as ObjectName, 
	Convert(nvarchar(22), ps.PS_ID) as ParentID, Convert(tinyint, 3) as ParentTypeHierarchy, ps.StringName as ParentName,
	'Alarms_Archive_To_TI' as MQType
	from [dbo].[Alarms_Archive_To_TI] a WITH (NOLOCK)
	join [dbo].Info_TI ti WITH (NOLOCK) on ti.TI_ID = a.TI_ID
	join [dbo].Dict_PS ps WITH (NOLOCK) on ps.PS_ID = ti.PS_ID
	where Alarm_ID = @Alarm_ID
	
	union all
	
	----Авария по ПС-------------------------------------
	select Convert(nvarchar(22), a.PS_ID), Convert(tinyint,3), o.StringName, 
	Convert(nvarchar(22), p.HierLev3_ID), Convert(tinyint,2), p.StringName,
	'Alarms_Archive_To_PS' as MQType
	from [dbo].[Alarms_Archive_To_PS] a WITH (NOLOCK)
	join [dbo].Dict_PS o WITH (NOLOCK) on o.PS_ID = a.PS_ID
	join [dbo].Dict_HierLev3 p WITH (NOLOCK) on p.HierLev3_ID = o.HierLev3_ID
	where Alarm_ID = @Alarm_ID

	union all

	--Авария по балансу ПС-------------------------------------
	select Convert(nvarchar(22),a.BalancePS_UN), Convert(tinyint, 44), b.BalancePSName, Convert(nvarchar(22), o.PS_ID), 
	Convert(tinyint, 3), o.StringName,
	'Alarms_Archive_To_Balance_PS' as MQType
	from [dbo].[Alarms_Archive_To_Balance_PS] a WITH (NOLOCK)
	join [dbo].Info_Balance_PS_List_2 b WITH (NOLOCK) on b.BalancePS_UN = a.BalancePS_UN
	join [dbo].Dict_PS o WITH (NOLOCK) on o.PS_ID = b.PS_ID
	where Alarm_ID = @Alarm_ID

	union all
		
	--Авария по универсальному балансу-------------------------------------
	select Convert(nvarchar(22),a.BalanceFreeHierarchy_UN), Convert(tinyint, 45), bl.BalanceFreeHierarchyName, Convert(nvarchar(22),o.ID), 
	Convert(tinyint,o.TypeHierarchy), o.ObjectName,
	'Alarms_Archive_To_Balance_FreeHierarchy' as MQType
	from [dbo].[Alarms_Archive_To_Balance_FreeHierarchy] a WITH (NOLOCK)
	join [dbo].Info_Balance_FreeHierarchy_List bl WITH (NOLOCK) on bl.BalanceFreeHierarchy_UN = a.BalanceFreeHierarchy_UN
	join [dbo].Info_Balance_FreeHierarchy_Objects bo WITH (NOLOCK) on bo.BalanceFreeHierarchyObject_UN = bl.BalanceFreeHierarchyObject_UN
	cross apply [dbo].usf2_FreeHierarchyObjectInfo(bo.FreeHierItem_ID, bo.HierLev1_ID, bo.HierLev2_ID, bo.HierLev3_ID, bo.PS_ID, bo.TI_ID) o
	where Alarm_ID = @Alarm_ID

	union all
	
	--Авария по 61968-------------------------------------
	select Convert(nvarchar(22),a.Slave61968System_ID), Convert(tinyint, 25), ms.StringName, null, null, null,
	'Alarms_Archive_To_Master61968_SlaveSystems' as MQType 
	from [dbo].[Alarms_Archive_To_Master61968_SlaveSystems] a WITH (NOLOCK) 
	join [dbo].Master61968_SlaveSystems ms  WITH (NOLOCK) on ms.Slave61968System_ID = a.Slave61968System_ID
	where Alarm_ID = @Alarm_ID

	union all

	--Авария по формуле-------------------------------------
	select Convert(nvarchar(22),a.Formula_UN), Convert(tinyint, 11), fl.FormulaName, Convert(nvarchar(22),o.ID), Convert(tinyint,o.TypeHierarchy), o.ObjectName,
	'Alarms_Archive_To_Formula' as MQType 
	from [dbo].[Alarms_Archive_To_Formula] a WITH (NOLOCK)
	join [dbo].Info_Formula_List fl WITH (NOLOCK)on fl.Formula_UN = a.Formula_UN
	cross apply [dbo].usf2_FreeHierarchyObjectInfo(fl.FreeHierItem_ID, fl.HierLev1_ID, fl.HierLev2_ID, fl.HierLev3_ID, fl.PS_ID, fl.TI_ID) o
	where Alarm_ID = @Alarm_ID
go
grant select on usf2_Alarm_ObjectInfo to [UserCalcService]
go

-- ======================================================================================
-- Автор:
--
--		Малышев Игорь
--
-- Дата создания:
--
--		Июнь, 2018
--
-- Описание:
--
--		Журнал аварий
--
-- ======================================================================================
create view [dbo].[vw_Alarms] 
WITH SCHEMABINDING
AS

SELECT 
[t1].[Alarm_ID], [t1].[EventDateTime],
[t1].AlarmSeverity,
case [t1].[AlarmSeverity] 
	when 0 then 'Нет'  
	when 1 then 'Нормальный'
	when 2 then 'Предупреждение'
	when 3 then 'Критический'
	else 'Не_определено'
end as AlarmSeverityName,
[t1].[AlarmMessage], [t1].[AlarmDescription], [t1].[AlarmMessageShort],  
[t1].[AlarmDateTime], 

[t2].[StringName] AS [WorkFlowActivityName], [t3].[StringName] AS [SettingName], [t4].[UserFullName] AS [UserName] 

,o.ID, o.TypeHierarchy, o.ObjectName, o.ParentId, o.ParentTypeHierarchy, o.ParentName
, [t16].AlarmConfirmStatusCategory_ID
, [t16].AlarmConfirmStatusCategoryName 
, [t16].Comment
, [t1].[User_ID]
, [t1].Confirmed

FROM [dbo].[Alarms_Archive] AS [t1] WITH (NOLOCK)
LEFT JOIN [dbo].[Workflow_Activity_List] AS [t2] WITH (NOLOCK)  ON [t1].[WorkflowActivity_ID] = [t2].[WorkflowActivity_ID]
LEFT JOIN [dbo].[Alarms_Settings] AS [t3] WITH (NOLOCK)  ON [t1].[AlarmSetting_ID] = [t3].[AlarmSetting_ID]
LEFT JOIN [dbo].[Expl_Users] AS [t4] WITH (NOLOCK)  ON [t1].[User_ID] = [t4].[User_ID]

outer apply 
(
	select top 1 ID, o.TypeHierarchy, o.ObjectName, o.ParentId, o.ParentTypeHierarchy, o.ParentName from [dbo].usf2_Alarm_ObjectInfo([t1].Alarm_ID) o
) o

outer apply 
(
	select top 1 s.AlarmConfirmStatusCategory_ID, s.Comment, d.AlarmConfirmStatusCategoryName
	from [dbo].[Alarms_Archive_Confirm_Status] s WITH (NOLOCK) 
	left join [dbo].[Dict_Alarms_ConfirmStatus] d WITH (NOLOCK) on d.AlarmConfirmStatusCategory_ID = s.AlarmConfirmStatusCategory_ID
	where s.Alarm_ID = [t1].Alarm_ID and [t1].Confirmed = 1
	order by [ConfirmStatusDateTime] desc
)  [t16]

GO

   grant select on vw_Alarms to [UserCalcService]
go

-- ======================================================================================
-- Автор:
--
--		Александр Карташев
--
-- Дата создания:
--
--		Июнь, 2018
--
-- Описание:
--
--		Журнал аварий (альтернативный вариант)
--
-- ======================================================================================
create view [dbo].[vw_Alarms_Alexander] 
WITH SCHEMABINDING
as


SELECT 
--Alarms_Archive
[t1].[Alarm_ID], [t1].[EventDateTime], 
case [t1].[AlarmSeverity] 
	when 0 then 'Нет'  
	when 1 then 'Нормальный'
	when 2 then 'Предупреждение'
	when 3 then 'Критический'
	else ''
end as AlarmSeverityName,
[t1].[AlarmMessage], [t1].[AlarmDescription], [t1].[AlarmMessageShort],  
[t1].[AlarmDateTime], 

[t2].[StringName] AS [WorkFlowActivityName], [t3].[StringName] AS [SettingName], [t4].[UserFullName] AS [UserName] 

,(SELECT 
CASE WHEN toTI.TI_ID IS NOT NULL THEN CONVERT(nvarchar(22),toTI.TI_ID)
WHEN  toPS.PS_ID IS NOT NULL THEN CONVERT(nvarchar(22),toPS.PS_ID)    
WHEN  toBPS.BalancePS_UN IS NOT NULL THEN toBPS.BalancePS_UN
WHEN  toBFH.BalanceFreeHierarchy_UN IS NOT NULL THEN toBFH.BalanceFreeHierarchy_UN
WHEN  toFormula.Formula_UN IS NOT NULL THEN  toFormula.Formula_UN  
WHEN  toMaster61968_SlaveSystems.Slave61968System_ID IS NOT NULL THEN CONVERT(nvarchar,toMaster61968_SlaveSystems.Slave61968System_ID)
ELSE NULL END) as ID,
(SELECT 
CASE WHEN toTI.TI_ID IS NOT NULL THEN 4
WHEN  toPS.PS_ID IS NOT NULL THEN 3    
WHEN  toBPS.BalancePS_UN IS NOT NULL THEN 44
WHEN  toBFH.BalanceFreeHierarchy_UN IS NOT NULL THEN 45
WHEN  toFormula.Formula_UN IS NOT NULL THEN  11 
WHEN  toMaster61968_SlaveSystems.Slave61968System_ID IS NOT NULL THEN 11
ELSE NULL END) as TypeHierarchy
,(SELECT 
CASE WHEN toTI.TI_ID IS NOT NULL THEN TI_Info_TI.TIName
WHEN  toPS.PS_ID IS NOT NULL THEN PS_Dict_PS.StringName    
WHEN  toBPS.BalancePS_UN IS NOT NULL THEN BalancePSList.BalancePSName
WHEN  toBFH.BalanceFreeHierarchy_UN IS NOT NULL THEN Info_Balance_FreeHierarchy_List.BalanceFreeHierarchyName
WHEN  toFormula.Formula_UN IS NOT NULL THEN   Formula_Info_Formula_List.FormulaName
WHEN  toMaster61968_SlaveSystems.Slave61968System_ID IS NOT NULL THEN Master61968_SlaveSystems.StringName
ELSE NULL END) as ObjectName
,(SELECT 
CASE WHEN TI_Dict_PS.PS_ID IS NOT NULL THEN CONVERT(nvarchar(22),TI_Dict_PS.PS_ID)
WHEN  PS_Dict_HierLev3.HierLev3_ID IS NOT NULL THEN CONVERT(nvarchar(22),PS_Dict_HierLev3.HierLev3_ID)    
WHEN  Balance_Dict_PS.PS_ID IS NOT NULL THEN CONVERT(nvarchar(22), Balance_Dict_PS.PS_ID)
WHEN  toBFH.BalanceFreeHierarchy_UN IS NOT NULL THEN (SELECT ID FROM [dbo].usf2_FreeHierarchyObjectInfo(Balance_bo.FreeHierItem_ID, Balance_bo.HierLev1_ID, Balance_bo.HierLev2_ID, Balance_bo.HierLev3_ID, Balance_bo.PS_ID, Balance_bo.TI_ID) )
WHEN  toFormula.Formula_UN IS NOT NULL THEN   (SELECT ID FROM [dbo].usf2_FreeHierarchyObjectInfo(Formula_Info_Formula_List.FreeHierItem_ID, Formula_Info_Formula_List.HierLev1_ID, Formula_Info_Formula_List.HierLev2_ID, Formula_Info_Formula_List.HierLev3_ID, Formula_Info_Formula_List.PS_ID, Formula_Info_Formula_List.TI_ID) )
--WHEN  toMaster61968_SlaveSystems.Slave61968System_ID IS NOT NULL THEN NULL
ELSE NULL END) as ParentId
,(SELECT 
CASE WHEN TI_Dict_PS.PS_ID IS NOT NULL THEN 3
WHEN  PS_Dict_HierLev3.HierLev3_ID IS NOT NULL THEN 2    
WHEN  Balance_Dict_PS.PS_ID IS NOT NULL THEN 3
WHEN  toBFH.BalanceFreeHierarchy_UN IS NOT NULL THEN (SELECT TypeHierarchy FROM [dbo].usf2_FreeHierarchyObjectInfo(Balance_bo.FreeHierItem_ID, Balance_bo.HierLev1_ID, Balance_bo.HierLev2_ID, Balance_bo.HierLev3_ID, Balance_bo.PS_ID, Balance_bo.TI_ID))
WHEN  toFormula.Formula_UN IS NOT NULL THEN  (SELECT TypeHierarchy FROM [dbo].usf2_FreeHierarchyObjectInfo(Formula_Info_Formula_List.FreeHierItem_ID, Formula_Info_Formula_List.HierLev1_ID, Formula_Info_Formula_List.HierLev2_ID, Formula_Info_Formula_List.HierLev3_ID, Formula_Info_Formula_List.PS_ID, Formula_Info_Formula_List.TI_ID) )
--WHEN  toMaster61968_SlaveSystems.Slave61968System_ID IS NOT NULL THEN NULL
ELSE NULL END) as ParentTypeHierarchy
,(SELECT 
CASE WHEN toTI.TI_ID IS NOT NULL THEN TI_Dict_PS.StringName
WHEN  toPS.PS_ID IS NOT NULL THEN PS_Dict_HierLev3.StringName    
WHEN  toBPS.BalancePS_UN IS NOT NULL THEN Balance_Dict_PS.StringName
WHEN  toBFH.BalanceFreeHierarchy_UN IS NOT NULL THEN (SELECT ObjectName FROM [dbo].usf2_FreeHierarchyObjectInfo(Balance_bo.FreeHierItem_ID, Balance_bo.HierLev1_ID, Balance_bo.HierLev2_ID, Balance_bo.HierLev3_ID, Balance_bo.PS_ID, Balance_bo.TI_ID))
WHEN  toFormula.Formula_UN IS NOT NULL THEN  (SELECT ObjectName FROM [dbo].usf2_FreeHierarchyObjectInfo(Formula_Info_Formula_List.FreeHierItem_ID, Formula_Info_Formula_List.HierLev1_ID, Formula_Info_Formula_List.HierLev2_ID, Formula_Info_Formula_List.HierLev3_ID, Formula_Info_Formula_List.PS_ID, Formula_Info_Formula_List.TI_ID) )
--WHEN  toMaster61968_SlaveSystems.Slave61968System_ID IS NOT NULL THEN NULL
ELSE NULL END) as ParentName
-- o.ParentId, o.ParentTypeHierarchy, o.ParentName

--Alarms_Archive_Confirm_Status
,(
	select top 1 AlarmConfirmStatusCategoryName from [dbo].[Dict_Alarms_ConfirmStatus] WITH (NOLOCK)
	where AlarmConfirmStatusCategory_ID = [t16].AlarmConfirmStatusCategory_ID
) as AlarmConfirmStatusCategoryName
, [t16].Comment
, [t1].[User_ID]
, [t1].Confirmed

FROM [dbo].[Alarms_Archive] AS [t1] WITH (NOLOCK)
LEFT JOIN [dbo].[Workflow_Activity_List] AS [t2] WITH (NOLOCK) ON [t1].[WorkflowActivity_ID] = [t2].[WorkflowActivity_ID]
LEFT JOIN [dbo].[Alarms_Settings] AS [t3] WITH (NOLOCK) ON [t1].[AlarmSetting_ID] = [t3].[AlarmSetting_ID]
LEFT JOIN [dbo].[Expl_Users] AS [t4] WITH (NOLOCK) ON [t1].[User_ID] = [t4].[User_ID]
LEFT JOIN [dbo].Alarms_Archive_To_User AS toUser WITH (NOLOCK) on toUser.Alarm_ID = [t1].Alarm_ID

LEFT JOIN [dbo].Alarms_Archive_To_TI AS toTI  WITH (NOLOCK)ON toTI.Alarm_ID = [t1].Alarm_ID
LEFT JOIN [dbo].Info_TI AS TI_Info_TI  WITH (NOLOCK)ON TI_Info_TI.TI_ID = toTI.TI_ID
LEFT JOIN [dbo].Dict_PS AS TI_Dict_PS  WITH (NOLOCK)ON TI_Dict_PS.PS_ID = TI_Info_TI.PS_ID


LEFT JOIN [dbo].[Alarms_Archive_To_PS] AS toPS WITH (NOLOCK)  on toPS.Alarm_ID = [t1].Alarm_ID
LEFT JOIN [dbo].Dict_PS AS PS_Dict_PS  WITH (NOLOCK)ON PS_Dict_PS.PS_ID = toPS.PS_ID
LEFT JOIN [dbo].Dict_HierLev3 AS PS_Dict_HierLev3 WITH (NOLOCK)  on PS_Dict_HierLev3.HierLev3_ID = PS_Dict_PS.HierLev3_ID


LEFT JOIN [dbo].[Alarms_Archive_To_Balance_PS] AS toBPS WITH (NOLOCK)  on toBPS.Alarm_ID = [t1].Alarm_ID
LEFT JOIN [dbo].Info_Balance_PS_List_2 AS BalancePSList WITH (NOLOCK)  on toBPS.BalancePS_UN = BalancePSList.BalancePS_UN
LEFT JOIN [dbo].Dict_PS as Balance_Dict_PS WITH (NOLOCK) on Balance_Dict_PS.PS_ID = BalancePSList.PS_ID


LEFT JOIN [dbo].[Alarms_Archive_To_Balance_FreeHierarchy] AS toBFH WITH (NOLOCK)  on toBFH.Alarm_ID = [t1].Alarm_ID
LEFT JOIN [dbo].Info_Balance_FreeHierarchy_List AS Info_Balance_FreeHierarchy_List WITH (NOLOCK)  on toBFH.BalanceFreeHierarchy_UN = Info_Balance_FreeHierarchy_List.BalanceFreeHierarchy_UN
LEFT JOIN [dbo].Info_Balance_FreeHierarchy_Objects as Balance_bo WITH (NOLOCK) on Balance_bo.BalanceFreeHierarchyObject_UN = Info_Balance_FreeHierarchy_List.BalanceFreeHierarchyObject_UN
		--cross apply usf2_FreeHierarchyObjectInfo(bo.FreeHierItem_ID, bo.HierLev1_ID, bo.HierLev2_ID, bo.HierLev3_ID, bo.PS_ID, bo.TI_ID) o
		

LEFT JOIN [dbo].[Alarms_Archive_To_Formula] AS toFormula WITH (NOLOCK)  on toFormula.Alarm_ID = [t1].Alarm_ID
LEFT JOIN [dbo].Info_Formula_List AS Formula_Info_Formula_List WITH (NOLOCK)  on toFormula.Formula_UN = Formula_Info_Formula_List.Formula_UN
--cross apply usf2_FreeHierarchyObjectInfo(fl.FreeHierItem_ID, fl.HierLev1_ID, fl.HierLev2_ID, fl.HierLev3_ID, fl.PS_ID, fl.TI_ID) o
	

LEFT JOIN [dbo].[Alarms_Archive_To_Master61968_SlaveSystems] AS toMaster61968_SlaveSystems WITH (NOLOCK)  on toMaster61968_SlaveSystems.Alarm_ID = [t1].Alarm_ID
LEFT JOIN [dbo].Master61968_SlaveSystems AS Master61968_SlaveSystems WITH (NOLOCK)  on toMaster61968_SlaveSystems.Slave61968System_ID = Master61968_SlaveSystems.Slave61968System_ID

outer apply 
(
	select top 1 s.AlarmConfirmStatusCategory_ID, s.Comment from [dbo].[Alarms_Archive_Confirm_Status] s WITH (NOLOCK)  
	where [t1].Confirmed = 1 and s.Alarm_ID = [t1].Alarm_ID 
	--order by [ConfirmStatusDateTime] desc
)  [t16]

GO

   grant select on vw_Alarms_Alexander to [UserCalcService]
go

--Индексы для вьюхи

--Индексы для вьюхи

CREATE UNIQUE CLUSTERED INDEX [IX_FreeHierarchyObjects_ID] ON [dbo].[vw_FreeHierarchyObjects]
(
	[FreeHierTree_ID] ASC,
	[ID] ASC,
	[FreeHierItemType] ASC,
	[ParentHierID] ASC,
	[FreeHierItem_ID] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, IGNORE_DUP_KEY = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
GO

CREATE NONCLUSTERED INDEX [IX_FreeHierarchyObjects_StringName] ON [dbo].[vw_FreeHierarchyObjects]
(
	[StringName] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
GO

CREATE NONCLUSTERED INDEX [IX_FreeHierarchyObjects_StringID] ON [dbo].[vw_FreeHierarchyObjects]
(
	[FreeHierItemType] ASC,
	[ID] ASC,
	[StringId] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
GO

--Эта часть относится к дереву иерархии

-- ======================================================================================
-- Автор:
--
--		Малышев Игорь
--
-- Дата создания:
--
--		Февраль, 2019
--
-- Описание:
--
--		Стандартные объекты
--
-- ======================================================================================
create view [dbo].[vw_HierarchyObjects] 
WITH SCHEMABINDING
AS
	--Объекты уровня 1
	select [HierLev1_ID] as ID, null as StringId, 0 as TypeHierarchy, StringName, 'Dict_HierLev1_' as ObjectTypeName
		,cast([HierLev1_ID] as varchar(255)) as MeterSerialNumber
		,null as ParentID, null as ParentTypeHierarchy --Информация о родителях
		,1 as FreeHierItemType
		,NULL as ParentName
		,null as ParentFreeHierItemType
	from [dbo].[Dict_HierLev1] with (nolock)

	union all

	--Объекты уровня 2
	select [HierLev2_ID] as ID, null as StringId, 1 as TypeHierarchy, h2.StringName, 'Dict_HierLev2_' as ObjectTypeName
		,cast([HierLev2_ID] as varchar(255)) as MeterSerialNumber
		,h2.HierLev1_ID as ParentID, 0 as ParentTypeHierarchy
		,2 as FreeHierItemType
		,h1.StringName as ParentName
		,1 as ParentFreeHierItemType
	from [dbo].[Dict_HierLev2] h2 with (nolock)
	join [dbo].[Dict_HierLev1] h1 with (nolock) on h1.HierLev1_ID = h2.HierLev1_ID

	union all

	--Объекты уровня 3
	select [HierLev3_ID] as ID, null as StringId, 2 as TypeHierarchy, h3.StringName, 'Dict_HierLev3' as ObjectTypeName
		,cast([HierLev3_ID] as varchar(255)) as MeterSerialNumber
		,h3.HierLev2_ID as ParentID, 1 as ParentTypeHierarchy
		,3 as FreeHierItemType
		,h2.StringName as ParentName
		,2 as ParentFreeHierItemType
	from [dbo].[Dict_HierLev3] h3 with (nolock)
	join [dbo].[Dict_HierLev2] h2 with (nolock) on h2.HierLev2_ID = h3.HierLev2_ID

	union all

	--Объекты уровня 4
	select [PS_ID] as ID, null as StringId, 3 as TypeHierarchy, p.StringName, 'Dict_PS_' as ObjectTypeName
		,cast([PS_ID] as varchar(255)) as MeterSerialNumber
		,p.HierLev3_ID as ParentID, 2 as ParentTypeHierarchy
		,4 as FreeHierItemType
		,h3.StringName as ParentName
		,3 as ParentFreeHierItemType
	from [dbo].[Dict_PS] p with (nolock)
	join [dbo].[Dict_HierLev3] h3 with (nolock) on h3.HierLev3_ID = p.HierLev3_ID

	union all

	--Объекты ТИ
	select ti.[TI_ID] as ID, null as StringId, 4 as TypeHierarchy, ti.TIName as StringName, 'Info_TI' as ObjectTypeName
		,im.[MeterSerialNumber]
		,ti.PS_ID as ParentID, 3 as ParentTypeHierarchy
		,5 as FreeHierItemType
		,ps.StringName as ParentName
		,4 as ParentFreeHierItemType
	from [dbo].[Info_TI] ti with (nolock)
	join [dbo].[Dict_PS] ps with (nolock) on ps.PS_ID = ti.PS_ID
	outer apply (
				select top 1 hm.MeterSerialNumber
				from dbo.Info_Meters_TO_TI mti
				join dbo.HARD_METERS hm on hm.Meter_id = mti.Meter_id 
				where mti.TI_ID = ti.TI_ID
				order by StartDateTime desc
			) im

	union all

	--Объекты ТП
	select tp.[TP_ID] as ID, null as StringId, 8 as TypeHierarchy, StringName, 'Info_TP2' as ObjectTypeName
		,cast(tp.[TP_ID] as varchar(255)) as MeterSerialNumber
		,sd.Section_ID as ParentID, 5 as ParentTypeHierarchy
		,8 as FreeHierItemType
		,s.SectionName as ParentName
		,7 as ParentFreeHierItemType
	from [dbo].[Info_TP2] tp with (nolock)
	join [dbo].[Info_Section_Description2] sd on tp.TP_ID = sd.TP_ID
	join [dbo].[Info_Section_List] s on sd.Section_ID = s.Section_ID
	where StartDateTime <= GETDATE() and (FinishDateTime is null or FinishDateTime >=GETDATE())

	union all

	--Объекты сечения
	select s.[Section_ID] as ID, null as StringId, 5 as TypeHierarchy, SectionName, 'Info_Section_List' as ObjectTypeName
		,cast(s.[Section_ID] as varchar(255)) as MeterSerialNumber
		,cast(o.ID as int) as ParentID, o.TypeHierarchy as ParentTypeHierarchy
		,7 as FreeHierItemType
		,o.ObjectName as ParentName
		,null as ParentFreeHierItemType
	from [dbo].[Info_Section_List] s with (nolock)
	outer apply [dbo].usf2_FreeHierarchyObjectInfo(NULL, s.HierLev1_ID, s.HierLev2_ID, s.HierLev3_ID, s.PS_ID, NULL) o

	union all

	--УСПД
	select uspd.[USPD_ID] as ID, null as StringId, 32 as TypeHierarchy, ps.StringName as StringName, 'Hard_USPD' as ObjectTypeName
		,case when USPDSerialNumber is not null then USPDSerialNumber else cast([USPDIPMain] as varchar(255)) end as MeterSerialNumber
		,ps.PS_ID as ParentID, 3 as ParentTypeHierarchy
		,9 as FreeHierItemType
		,ps.StringName as ParentName
		,null as ParentFreeHierItemType
	from [dbo].[Hard_USPD] uspd with (nolock)
	join [dbo].Hard_USPDCommChannels_Links l on l.USPD_ID=uspd.USPD_ID
	join [dbo].Hard_CommChannels c on c.CommChannel_ID=l.CommChannel_ID
	join [dbo].Dict_PS ps on ps.PS_ID = c.PS_ID

	union all

	--E422
	select uspd.[E422_ID] as ID, null as StringId, 40 as TypeHierarchy, ps.StringName as StringName, 'Hard_E422' as ObjectTypeName
		,case when E422SerialNumber is not null then E422SerialNumber else cast([E422IPMain] as varchar(255)) end as MeterSerialNumber
		,ps.PS_ID as ParentID, 3 as ParentTypeHierarchy
		,30 as FreeHierItemType
		,ps.StringName as ParentName
		,null as ParentFreeHierItemType
	from [dbo].[Hard_E422] uspd with (nolock)
	join [dbo].Hard_E422CommChannels_Links l on l.E422_ID=uspd.E422_ID
	join [dbo].Hard_CommChannels c on c.CommChannel_ID=l.CommChannel_ID
	join [dbo].Dict_PS ps on ps.PS_ID = c.PS_ID

	union all

	--формулы
	select null as ID, [Formula_UN] as StringID, 11 as TypeHierarchy, [FormulaName] as StringName, 'Info_Formula_List' as ObjectTypeName
		,[Formula_UN] as MeterSerialNumber
		,cast(o.ID as int) as ParentID, o.TypeHierarchy as ParentTypeHierarchy
		,6 as FreeHierItemType
		,o.ObjectName as ParentName
		,null as ParentFreeHierItemType
	from [dbo].[Info_Formula_List] fl with (nolock)
	outer apply [dbo].usf2_FreeHierarchyObjectInfo(fl.FreeHierItem_ID, fl.HierLev1_ID, fl.HierLev2_ID, fl.HierLev3_ID, fl.PS_ID, fl.TI_ID) o

	union all

	--Константы
	select null as ID, [FormulaConstant_UN] as StringID, 41 as TypeHierarchy, [FormulaConstantName] as StringName, 'Info_Formula_Constants' as ObjectTypeName
		,[FormulaConstant_UN] as MeterSerialNumber
		,cast(o.ID as int) as ParentID, o.TypeHierarchy as ParentTypeHierarchy
		,28 as FreeHierItemType
		,o.ObjectName as ParentName
		,null as ParentFreeHierItemType
	from [dbo].[Info_Formula_Constants] fc with (nolock)
	join [dbo].[Info_Balance_FreeHierarchy_Objects] fl on fl.BalanceFreeHierarchyObject_UN = fc.BalanceFreeHierarchyObject_UN
	outer apply [dbo].usf2_FreeHierarchyObjectInfo(fl.FreeHierItem_ID, fl.HierLev1_ID, fl.HierLev2_ID, fl.HierLev3_ID, fl.PS_ID, fl.TI_ID) o

	union all

	--Балансы
	select null as ID, [b].[BalanceFreeHierarchy_UN] as StringID, 45 as TypeHierarchy, b.BalanceFreeHierarchyName as StringName, 'Info_Balance_FreeHierarchy_List' as ObjectTypeName
		,[b].[BalanceFreeHierarchy_UN] as MeterSerialNumber
		,cast(o.ID as int) as ParentID, o.TypeHierarchy as ParentTypeHierarchy
		,50 as FreeHierItemType
		,o.ObjectName as ParentName
		,null as ParentFreeHierItemType
	from [dbo].[Info_Balance_FreeHierarchy_List] b with (nolock)
	join [dbo].[Info_Balance_FreeHierarchy_Objects] fl on fl.BalanceFreeHierarchyObject_UN = b.BalanceFreeHierarchyObject_UN
	outer apply [dbo].usf2_FreeHierarchyObjectInfo(fl.FreeHierItem_ID, fl.HierLev1_ID, fl.HierLev2_ID, fl.HierLev3_ID, fl.PS_ID, fl.TI_ID) o
	
	union all

	--Узлы в дереве свободной иерархии
	select t.ID as ID, t.StringId , 28 as TypeHierarchy, t.StringName, 'Dict_FreeHierarchyTree' as ObjectTypeName
		,cast(t.ID as varchar(255)) as MeterSerialNumber
		,p.ID as ParentID, p.TypeHierarchy as ParentTypeHierarchy
		,0 as FreeHierItemType
		,p.StringName as ParentName
		,p.FreeHierItemType as ParentFreeHierItemType
	from dbo.vw_FreeHierarchyObjects t with (nolock)
	left join dbo.vw_FreeHierarchyObjects p on t.ParentHierID is not null and p.FreeHierTree_ID = t.FreeHierTree_ID and p.HierID = t.ParentHierID
	where t.FreeHierItemType = 0
	
GO
grant select on vw_HierarchyObjects to [UserCalcService]
go
grant select on vw_HierarchyObjects to [UserDeclarator]
go

-- ======================================================================================
-- Автор:
--
--		Малышев Игорь
--
-- Дата создания:
--
--		Октябрь 2019
--
-- Описание:
--
--		Строим дерево свободной иерархии со вложенными объектами
--
-- ======================================================================================
CREATE FUNCTION [dbo].[usf2_FreeHierarchyTree]
(
	@treeID int
)
RETURNS TABLE
AS RETURN
with tree (ID, StringId, TypeHierarchy, StringName, FreeHierItemType, 
		MeterSerialNumber, 
		ParentID, ParentTypeHierarchy, ParentName,
		FreeHierItemId, FreeHierTree_ID, IncludeObjectChildren, HierID, ParentHierID, ParentFreeHierItemType, IncludedObject, ToParentFreeHierPath) as
		(
			select f.ID, f.StringId, f.TypeHierarchy, f.StringName, f.FreeHierItemType
						--серийный номер ТИ
					, im.MeterSerialNumber as MeterSerialNumber
					, p.ParentID, p.ParentTypeHierarchy
					, p.ParentName
					, f.FreeHierItem_ID as FreeHierItemId, f.FreeHierTree_ID, f.IncludeObjectChildren, f.HierID, f.ParentHierID, p.ParentFreeHierItemType, cast(0 as bit) as IncludedObject
					--Путь до рута
					, cast(case when ID is null or ID <= 0 then StringId else ltrim(str(ID,15)) end + ',' + cast(TypeHierarchy as varchar(2)) + ',' + case when FreeHierItem_ID is null then '' else ltrim(str(FreeHierItem_ID,15)) end + ';' as nvarchar(1000)) as ToParentFreeHierPath
			from 
			[dbo].vw_FreeHierarchyObjects f
					outer apply
					(
					--Информация о родителе
						select top 1 ff.StringName as ParentName, ff.TypeHierarchy as ParentTypeHierarchy, ff.FreeHierItemType as ParentFreeHierItemType
						, ff.ID as ParentID, IncludeObjectChildren
						from [dbo].vw_FreeHierarchyObjects ff
						where ff.FreeHierTree_ID = f.FreeHierTree_ID and ff.HierID = f.ParentHierID
					) p 
					--Серийный номер ТИ
					outer apply (
							select top 1 hm.MeterSerialNumber
							from dbo.Info_Meters_TO_TI mti
							join dbo.HARD_METERS hm on hm.Meter_id = mti.Meter_id 
							where f.FreeHierItemType = 5 and mti.TI_ID = f.ID
							order by StartDateTime desc
						) im
			where f.FreeHierTree_ID = @treeID
			union all
			--Рекурсия на дочерние подгружаемые по IncludeObjectChildren
			select h.ID, h.StringId, cast(h.TypeHierarchy as tinyint), cast(h.StringName as nvarchar(255)), cast(h.FreeHierItemType as tinyint)
						--серийный номер ТИ
					, h.MeterSerialNumber
					, h.ParentID, cast(h.ParentTypeHierarchy as tinyint)
					, cast(h.ParentName as nvarchar(255))
					, NULL as FreeHierItemId, t.FreeHierTree_ID, cast(1 as bit), t.HierID, t.HierID as ParentHierID, t.FreeHierItemType, cast(1 as bit)
					--Путь до рута
					, cast(case when h.ID is null or h.ID <= 0 then h.StringId else ltrim(str(h.ID,15)) end + ',' + cast(h.TypeHierarchy as varchar(2)) + ',;' + t.ToParentFreeHierPath as nvarchar(1000))  from 
					tree t
					join [dbo].[vw_HierarchyObjects] h on h.ParentFreeHierItemType = t.FreeHierItemType and h.ParentID = t.ID
			where t.IncludeObjectChildren = 1 
			and h.TypeHierarchy in (0,1,2,3,4) --Здесь ограничение по типам объектов, если нужно будет искать ТП, до добавить тип 5
		)

		select * from tree
GO
grant select on usf2_FreeHierarchyTree to [UserCalcService]
go

-- ======================================================================================
-- Автор:
--
--		Малышев Игорь
--
-- Дата создания:
--
--		Октябрь 2019
--
-- Описание:
--
--		Строим стандартное дерево со вложенными объектами
--
-- ======================================================================================
CREATE FUNCTION [dbo].[usf2_FreeHierarchyTreeStandart]
(
	@treeID int
)
RETURNS TABLE
AS RETURN
with tree (ID, StringId, TypeHierarchy, StringName, FreeHierItemType, 
		MeterSerialNumber, 
		ParentID, ParentTypeHierarchy, ParentName,
		FreeHierItemId, FreeHierTree_ID, IncludeObjectChildren, HierID, ParentHierID, ParentFreeHierItemType, IncludedObject, ToParentFreeHierPath) as
		(
			select f.ID, f.StringId, f.TypeHierarchy, f.StringName, f.FreeHierItemType
						--серийный номер ТИ
					, f.MeterSerialNumber as MeterSerialNumber
					, f.ParentID, f.ParentTypeHierarchy
					, f.ParentName
					, NULL as FreeHierItemId, @treeID, cast(1 as bit) as IncludeObjectChildren, '/' as HierID, '/' as ParentHierID, NULL as ParentFreeHierItemType, 
					cast(0 as bit) as IncludedObject
					--Путь до рута
					, cast(ltrim(str(ID,15)) + ',' + cast(TypeHierarchy as varchar(2)) + ',;' as nvarchar(1000)) as ToParentFreeHierPath
			from vw_HierarchyObjects f
			where TypeHierarchy in (0)  --Дерево начинается с этого объекта
			
			union all
			--Рекурсия на дочерние 
			select h.ID, h.StringId, h.TypeHierarchy, h.StringName, h.FreeHierItemType
						--серийный номер ТИ
					, h.MeterSerialNumber
					, h.ParentID, h.ParentTypeHierarchy
					, h.ParentName
					, NULL as FreeHierItemId, t.FreeHierTree_ID, cast(1 as bit), t.HierID, t.HierID as ParentHierID, t.FreeHierItemType, cast(1 as bit)
					--Путь до рута
					, cast(case when h.ID is null then h.StringId else ltrim(str(h.ID,15)) end + ',' + cast(h.TypeHierarchy as varchar(2)) + ',;' + t.ToParentFreeHierPath as nvarchar(1000))  from 
					tree t
					join [dbo].[vw_HierarchyObjects] h on h.ParentTypeHierarchy = t.TypeHierarchy and h.ParentID = t.ID
			where t.TypeHierarchy not in (4,11,41,45) and h.TypeHierarchy in (0,1,2,3,4) -- -101
			--or (@treeID = -105 AND h.TypeHierarchy in 
			--(
			--11 , ----Ищем формулы 
			--41, --константы
			--45 --балансы
			-- 32 УСПД
			-- 40 Е422
			--))
		)

		select * from tree
GO

grant select on usf2_FreeHierarchyTreeStandart to [UserCalcService]
go
-- ======================================================================================
-- Автор:
--
--		Малышев Игорь
--
-- Дата создания:
--
--		Октябрь, 2019
--
-- Описание:
--
--		Наполняем таблицу-кэш Dict_FreeHierarchyIncludedObjectChildren
--
-- ======================================================================================
CREATE proc [dbo].[usp2_FreeHierarchy_UpdateIncludedObjectChildren]
(
@treeID int, --Идентификатор дерева
@isFullReload bit = 0 --Перенаполнить, даже если уже есть
)
as
begin
	--В таблице Dict_FreeHierarchyIncludedObjectChildren только стандартные деревья 
	--и объекты IncludedObjects, которых нет в Dict_FreeHierarchyTree, но должны подгружаться в деревья свободной иерархии на клиенте

	set nocount on
	--Если данные есть, то пропускаем
	if (@isFullReload<>1 AND exists(select top 1 1 from [dbo].[Dict_FreeHierarchyIncludedObjectChildren] where [FreeHierTree_ID] = @treeID)) return 

	if (@isFullReload = 1) begin

		delete from [dbo].[Dict_FreeHierarchyIncludedObjectChildren] where FreeHierTree_ID = @treeID
	end

	if (@treeID >=0) begin 
		--Деревья свободной иерархии
		select f.ID, f.StringId, f.TypeHierarchy, f.StringName, f.FreeHierItemType, im.MeterSerialNumber,
		p.ID as ParentID, p.TypeHierarchy as ParentTypeHierarchy, p.StringName as ParentName,
		f.FreeHierItem_ID as FreeHierItemId, @treeID as FreeHierTree_ID, f.IncludeObjectChildren, f.HierID, f.ParentHierID as ParentHierID, p.FreeHierItemType as ParentFreeHierItemType, cast(0 as bit) as IncludedObject,
		cast([dbo].[usf2_FreeHierarchy_GetPath](case when f.ID is null or f.ID <= 0 then f.StringId else ltrim(str(f.ID,15)) end, f.TypeHierarchy, f.FreeHierItem_ID, @treeID) as nvarchar(1000)) as ToParentFreeHierPath
		into #ft
		from dbo.vw_FreeHierarchyObjects f
		--@objectIds ids  
			--Дерево свободной иерархии
			--join dbo.vw_FreeHierarchyObjects f on f.FreeHierTree_ID = @treeID and ((ids.FreeHierItemId is not null and f.FreeHierItem_ID = ids.FreeHierItemId) or (ids.FreeHierItemId is null and f.TypeHierarchy = ids.TypeHierarchy and f.ID = ids.ID))
			left join [dbo].vw_FreeHierarchyObjects p on p.FreeHierTree_ID = @treeID and p.HierID = f.ParentHierID --Информация о родителе

			--Серийный номер ТИ
			outer apply (
					select top 1 hm.MeterSerialNumber
					from dbo.Info_Meters_TO_TI mti
					join dbo.HARD_METERS hm on hm.Meter_id = mti.Meter_id 
					where f.FreeHierItemType = 5 and mti.TI_ID = f.ID
					order by StartDateTime desc
				) im
		where f.FreeHierTree_ID = @treeID

		--Строим объекты подгружаемые по IncludeObjectChildren
		create table #io
		(
		ID bigint NOT NULL,
		StringId varchar(22) NOT NULL,
		TypeHierarchy int NOT NULL,
		StringName nvarchar(1024) NULL,
		ObjectTypeName varchar(31) NOT NULL,
		MeterSerialNumber varchar(255) NULL,
		ParentID bigint NOT NULL,
		ParentTypeHierarchy int NOT NULL,
		[FreeHierItemType] int NULL,
		[ParentName] nvarchar(1024) NULL,
		[ParentFreeHierItemType] int NULL,
		[ToParentFreeHierPath] nvarchar(1000) NULL,
		PRIMARY KEY CLUSTERED (ParentID, ParentTypeHierarchy, ID, StringId, TypeHierarchy) -- 1 0 1
		);

		insert into #io

		select ISNULL(h.[ID], -1)
			  ,ISNULL(h.[StringId], '')
			  ,h.[TypeHierarchy]
			  ,h.[StringName]
			  ,h.[ObjectTypeName]
			  ,h.[MeterSerialNumber]
			  ,h.[ParentID]
			  ,h.[ParentTypeHierarchy]
			  ,h.[FreeHierItemType]
			  ,h.[ParentName]
			  ,h.[ParentFreeHierItemType]
			  ,p.ToParentFreeHierPath
		from #ft p
		--Все дочерние
		join vw_HierarchyObjects h on h.ParentTypeHierarchy = p.TypeHierarchy and h.ParentID = p.ID
		where p.IncludeObjectChildren = 1 and h.TypeHierarchy in (1,2,3,4)
		or (@treeID = -101 AND 
			
			h.TypeHierarchy in 
			(
			11, ----Ищем формулы 
			41, --константы
			45 --балансы
			-- 32 УСПД
			-- 40 Е422
			))

		--Рекурсия по стандартному дереву, унаследованному по IncludeObjectChildren
		;with tree (ID, StringId, TypeHierarchy, StringName, FreeHierItemType, 
		MeterSerialNumber, 
		ParentID, ParentTypeHierarchy, ParentName,
		FreeHierItemId, FreeHierTree_ID, IncludeObjectChildren, HierID, ParentHierID, ParentFreeHierItemType, IncludedObject, ToParentFreeHierPath) as
		(
			select f.ID, f.StringId, f.TypeHierarchy, f.StringName, f.FreeHierItemType
						--серийный номер ТИ
					, f.MeterSerialNumber as MeterSerialNumber
					, f.ParentID, f.ParentTypeHierarchy
					, f.ParentName
					, NULL as FreeHierItemId, @treeID, cast(1 as bit) as IncludeObjectChildren, '/' as HierID, '/' as ParentHierID, NULL as ParentFreeHierItemType, 
					cast(0 as bit) as IncludedObject
					--Путь до рута, пока считаем что у родителей только целочисленные идентификаторы
					, cast(ltrim(str(ID,15)) + ',' + cast(TypeHierarchy as varchar(2)) + ',;'+ f.ToParentFreeHierPath as nvarchar(1000)) as ToParentFreeHierPath
			from #io f
			--where TypeHierarchy in (0)  --Дерево начинается с этого объекта
			
			union all
			--Рекурсия на дочерние 
			select h.ID, h.StringId, h.TypeHierarchy, h.StringName, h.FreeHierItemType
						--серийный номер ТИ
					, h.MeterSerialNumber
					, h.ParentID, h.ParentTypeHierarchy
					, h.ParentName
					, NULL as FreeHierItemId, t.FreeHierTree_ID, cast(1 as bit), t.HierID, t.HierID as ParentHierID, t.FreeHierItemType, cast(1 as bit)
					--Путь до рута
					, cast(case when h.ID is null or h.ID <= 0 then h.StringId else ltrim(str(h.ID,15)) end + ',' + cast(h.TypeHierarchy as varchar(2)) + ',;' + t.ToParentFreeHierPath as nvarchar(1000))  from 
					tree t
					join vw_HierarchyObjects h on h.ParentTypeHierarchy = t.TypeHierarchy and h.ParentID = t.ID
			where t.TypeHierarchy not in (11,41,45) and h.TypeHierarchy in (0,1,2,3,4) -- -101
			or (@treeID = -101 AND 
			
			h.TypeHierarchy in 
			(
			11, ----Ищем формулы 
			41, --константы
			45 --балансы
			-- 32 УСПД
			-- 40 Е422
			))
		)

		insert into [dbo].[Dict_FreeHierarchyIncludedObjectChildren]
			([ToParentFreeHierPath]
					   ,[FreeHierTree_ID]
					   ,[ParentHierID]
					   ,[TypeHierarchy]
					   ,[ID]
					   ,[StringId]
					   ,[StringName]
					   ,[MeterSerialNumber]
					   ,[FreeHierItemType]
					   ,[ParentID]
					   ,[ParentTypeHierarchy]
					   ,[ParentName]
					   ,[ParentFreeHierItemType])

		--Объекты из свободной иерархии
		select ToParentFreeHierPath, FreeHierTree_ID, ISNULL(ParentHierID, '/') as ParentHierID, TypeHierarchy,
		case when ID > 0 then ID else null end as ID, case when StringId <> '' then StringId else null end as StringId,  
		StringName, MeterSerialNumber, FreeHierItemType, 
		ParentID, ParentTypeHierarchy, ParentName, ParentFreeHierItemType
		from #ft
		--where ParentHierID is not null --Тут нужно проработать, т.к. исключается самый рутовый объект
		
		union all

		--Подгружаемые по IncludeObjectChildren
		select ToParentFreeHierPath, FreeHierTree_ID, ParentHierID,TypeHierarchy,
		case when ID > 0 then ID else null end as ID, case when StringId <> '' then StringId else null end as StringId,  
		StringName, MeterSerialNumber, FreeHierItemType, 
		ParentID, ParentTypeHierarchy, ParentName, ParentFreeHierItemType from tree
		order by ToParentFreeHierPath


		drop table #ft
		drop table #io

	end else begin

		--Стандартные деревья

		create table #hierCach 
		(
			TypeHierarchy int,
			ID int,
			ToParentFreeHierPath varchar(1000),

			PRIMARY KEY CLUSTERED (TypeHierarchy, ID)
		);

		declare @typeHierarchy int
		set @typeHierarchy = 0

		--Наполняем таблицу кэш родителей
		while (@typeHierarchy <= 3) begin
			insert into #hierCach (TypeHierarchy, ID, ToParentFreeHierPath)
			select TypeHierarchy
			, ID
			, cast(ID as varchar(22)) + ',' + cast(h.TypeHierarchy as varchar(2)) + ',;' +
			ISNULL((select top 1 ToParentFreeHierPath from #hierCach c where h.ParentTypeHierarchy is not null and c.TypeHierarchy = h.ParentTypeHierarchy
					and c.Id = h.ParentID), '') as ToParentFreeHierPath
			from vw_HierarchyObjects h 
			where h.TypeHierarchy = @typeHierarchy

			set @typeHierarchy = @typeHierarchy + 1
		end

		
		insert into [dbo].[Dict_FreeHierarchyIncludedObjectChildren]
			([ToParentFreeHierPath]
					   ,[FreeHierTree_ID]
					   ,[ParentHierID]
					   ,[TypeHierarchy]
					   ,[ID]
					   ,[StringId]
					   ,[StringName]
					   ,[MeterSerialNumber]
					   ,[FreeHierItemType]
					   ,[ParentID]
					   ,[ParentTypeHierarchy]
					   ,[ParentName]
					   ,[ParentFreeHierItemType])
		select 
		--Собственный идентификатор
		case when ID is not null then cast(ID as varchar(22)) else StringId end + ',' + cast(h.TypeHierarchy as varchar(2)) + ',;' + 
		--Идентификатор родителя
		ISNULL((select top 1 ToParentFreeHierPath from #hierCach c where c.TypeHierarchy = h.ParentTypeHierarchy
		and c.ID = h.ParentID), '') as ToParentFreeHierPath
		, @treeID, '/', h.TypeHierarchy
		, ID, h.StringId, StringName, MeterSerialNumber, FreeHierItemType
		, ParentID, ParentTypeHierarchy, ParentName, ParentFreeHierItemType
		from vw_HierarchyObjects h
		where h.TypeHierarchy in (0,1,2,3,4)
			or (@treeID = -101 AND 
			h.TypeHierarchy in 
			(
				11, ----Ищем формулы 
				41, --константы
				45, --балансы
				32, --УСПД
				40 --Е422
			))
		--order by TypeHierarchy desc

	end
end
go

   grant EXECUTE on usp2_FreeHierarchy_UpdateIncludedObjectChildren to [UserCalcService]
go   
	grant EXECUTE on usp2_FreeHierarchy_UpdateIncludedObjectChildren to UserDeclarator
go

CREATE FUNCTION [dbo].[usf2_Utils_iter_freeHierPath_to_table] (@list ntext)
      RETURNS @tbl TABLE (listpos int IDENTITY(1, 1) NOT NULL,
                          ID int NULL, StringId varchar(22) NULL, TypeHierarchy  int NOT NULL, FreeHierItemId int NULL, ToParentFreeHierPath nvarchar(1000)) AS
   BEGIN
      DECLARE @pos      int,
              @textpos  int,
			  @StringID	varchar(22),		
			  @ID		int,	
			  @ISNUMERIC int,
			  @FreeHierItemId    varchar(5),
			  @TypeHierarchy varchar(5),
              @chunklen smallint,
			  @firstPos int,
			  @secondPos int,
			  
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

			SET @firstPos = charindex(',', @str);
			SET @StringID =  substring(@str, 1, @firstPos-1)
			SET @ISNUMERIC = ISNUMERIC(@StringID)

			if (@ISNUMERIC = 1) SET @ID = convert(int, @StringID);
			else SET @ID = null

			set @secondPos = charindex(',', @str, @firstPos+1);

			SET @TypeHierarchy = substring(@str, @firstPos+1, @secondPos - @firstPos - 1);

			SET @FreeHierItemId = substring(@tmpstr, @secondPos+1, @pos - @secondPos - 1);
			
            INSERT @tbl 
			(ID
			, StringId
			, TypeHierarchy
			, FreeHierItemId
			, ToParentFreeHierPath) 
			VALUES
			(@ID 
			, case when @ID is null then @StringID else null end
			, @TypeHierarchy
			, case when ISNUMERIC(@FreeHierItemId) = 1 then @FreeHierItemId else null end
			, @tmpstr)

            SET @tmpstr = ltrim(substring(@tmpstr, @pos + 1, len(@tmpstr)))
            SET @pos = charindex(';', @tmpstr)
         END

         SET @leftover = @tmpstr
      END

   --   IF ltrim(rtrim(@leftover)) <> '' begin

			--SET @StringID =  substring(@leftover, 1, charindex(',', @leftover)-3);
			--SET @ISNUMERIC = ISNUMERIC(@StringID)
			--if (@ISNUMERIC = 1) SET @ID = convert(int, @StringID);
			--else SET @ID = null

			--INSERT @tbl (ID
			--, StringId
			--, TypeHierarchy
			--, FreeHierItemId) 
			--VALUES(@ID
			--, case when @ID is null then @StringID else null end
			--, substring(@leftover, charindex(',', @leftover)+1,1)
			--, substring(@leftover, charindex(',', @leftover)+3,1))

 	 --end

    RETURN
   END

GO

grant select on usf2_Utils_iter_freeHierPath_to_table to [UserCalcService]
GO

-- ======================================================================================
-- Автор:
--
--		Малышев Игорь
--
-- Дата создания:
--
--		декабрь, 2019
--
-- Описание:
--
--		Строим строковое представление из ToParentFreeHierPath таблицы Dict_FreeHierarchyIncludedObjectChildren
--
-- ======================================================================================
create FUNCTION [dbo].[usf2_FreeHierarchy_GetStringPathFromNumeric]
(	
	@treeID int, --Идентификатор дерева
	@ToParentFreeHierPath varchar(896) --Цифровое представление
)
RETURNS varchar(max) --
AS
begin

return (select STUFF((select '\' + f.StringName -- родитель
		FROM [dbo].usf2_Utils_iter_freeHierPath_to_table(@ToParentFreeHierPath) u 
		join [dbo].[Dict_FreeHierarchyIncludedObjectChildren] f on f.ToParentFreeHierPath = u.ToParentFreeHierPath
		where f.FreeHierTree_ID = @treeID and listpos >= 1
		ORDER BY [listpos] desc
		FOR XML PATH('')), 1,1, ''))

end
GO
grant EXECUTE on usf2_FreeHierarchy_GetStringPathFromNumeric to [UserCalcService]
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
--		Поиск объекта
--
-- ======================================================================================
create proc [dbo].[usp2_Info_FindHierObject]
(
	@searchText nvarchar(1000),	--Текст для поиска объекта
	@User_ID varchar(22), --Индентификатор пользователя
	@treeID int, --Идентификатор дерева на котором ищем
	@searchParentText nvarchar(1000) = null,	--Текст для поиска родителя
	@paramName nvarchar(255) = 'name', -- Параметр по которому ищем
	@topFind int = 300, -- Предельное количество искомых объектов
	@findUspdAndE422InTree bit = 0, --Искать УСПД и E422
	@typeHierarchy tinyint = null
	
)
as
begin
	set nocount on
	set quoted_identifier, ansi_nulls, ansi_warnings, arithabort, concat_null_yields_null, ansi_padding on
	set numeric_roundabort off
	set transaction isolation level read uncommitted

	--Типы объектов по которым ищем
	create table #types
	(
		TypeHierarchy tinyint
	)

	if (@typeHierarchy is not null) begin

		--Нужно искать строго определенного типа
		insert into #types values (@typeHierarchy)

	end else begin
		--Набираем типы для поиска
		insert into #types values (0)
		insert into #types values (1)
		insert into #types values (2)
		insert into #types values (3)
	
		if (@findUspdAndE422InTree = 1) begin
			insert into #types values (32)
			insert into #types values (40)
		end

		if (@treeID = -101) begin
			--Стандартное дерево

			--Ищем формулы 
			insert into #types values (11)
			--константы
			insert into #types values (41)
			--балансы
			insert into #types values (45)

			--ТИ
			insert into #types values (4)

		end else if (@treeID > 0) begin --Здесь добавляем все типы объектов, которые можно искать по деревьям свободной иерархии
			--Ищем узлы свободной иерархии 
			insert into #types values (28)
			--константы
			--insert into #types values (41)
			--формулы 
			--insert into #types values (11)
			--ТИ
			insert into #types values (4)
		end

		--Все что относится к сечениям
		if (@treeID = -102 OR @treeID = -104 OR @treeID = -106 OR @treeID > 0) begin
		 --Дерево сечений
		 --Дерево фак. мощности
		 --Дерево юр. лиц
		 insert into #types values (8) -- ТП
		 insert into #types values (5) -- Сечения
		 insert into #types values (19) -- юр. лица, возможно нужно добавить в vw_HierarchyObjects
		 insert into #types values (20) -- договора юр. лиц, возможно нужно добавить в vw_HierarchyObjects
	
		end 
	end

	declare @searchTextLike nvarchar(1000), @searchParentTextLike nvarchar(1000)

	set @searchTextLike = '%' + replace(replace(replace(rtrim(ltrim(@searchText)),' ','<>'),'><',''),'<>','%') + '%';
	set @searchParentTextLike = '%' + replace(replace(replace(rtrim(ltrim(@searchParentText)),' ','<>'),'><',''),'<>','%') + '%';

	--Все объекты теперь кэшируются в Dict_FreeHierarchyIncludedObjectChildren

	if (@paramName = 'MeterSerialNumber') begin 

--------Эта часть везде одинаковая-----------------------------
		select top(@topFind) ISNULL(cast(h.ID as varchar(22)), h.StringId) as ID, cast(h.TypeHierarchy as tinyint) as TypeHierarchy, h.StringName, '' as ObjectTypeName
		, h.MeterSerialNumber, cast(h.ParentID as varchar(22)) as ParentID, cast(h.ParentTypeHierarchy as tinyint) as ParentTypeHierarchy
		, h.FreeHierItemType
		, h.ParentName, NULL as FreeHierItemId, NULL as FreeHierTree_ID
		, PATINDEX(@searchTextLike, h.MeterSerialNumber) as [charindex], ToParentFreeHierPath as [ForBuildPath] 
		--Здесь строим строку-путь к руту из названий 
		,dbo.usf2_FreeHierarchy_GetStringPathFromNumeric(@treeID, ToParentFreeHierPath) as ToRootPath
-------------------------------------------------------------
		from Dict_FreeHierarchyIncludedObjectChildren h 
		where h.FreeHierTree_ID = @treeID and TypeHierarchy in (select TypeHierarchy from #types) 
		--and [dbo].[usf2_UserHasRight](@User_ID, '6D95CECF-327A-408b-96E7-AF8EF27C0F64', h.ID, h.ObjectTypeName, null, @treeID, null) = 1 -- Проверяем право на прсмотр SeeDbObjects
		and 
		(
			@searchTextLike is null
			or
			h.MeterSerialNumber like @searchTextLike -- Ищем по номеру
		)
		and 
		(
			--Фильтр по названию родителя
			@searchParentTextLike is null or (h.ParentName like @searchParentTextLike)
		)
		--order by LEN(h.StringName), charindex(@searchText, h.StringName)
		--order by h.StringName

		order by [charindex], LEN(h.StringName), StringName


	--Поиск ТИ по Код ТИ АТС (Info_Ti.TIATSCode), Номер PIK (примечание ТИ, Info_Balance_FreeHierarchy_Comment.Comment), Место установки прибора учета (hard_meters.InstallationPlace) 
	end if (@paramName = 'TIATSCode') begin
		--Код ТИ АТС
		--TODO возможно, желательное добавить индекс по TIATSCode в Info_TI
		select top(@topFind) ISNULL(cast(h.ID as varchar(22)), h.StringId) as ID, cast(h.TypeHierarchy as tinyint) as TypeHierarchy, h.StringName, '' as ObjectTypeName
		, ti.TIATSCode as MeterSerialNumber, cast(h.ParentID as varchar(22)) as ParentID, cast(h.ParentTypeHierarchy as tinyint) as ParentTypeHierarchy
		, h.FreeHierItemType
		, h.ParentName, NULL as FreeHierItemId, NULL as FreeHierTree_ID
		,PATINDEX(@searchTextLike, h.StringName) as [charindex]
		, ToParentFreeHierPath as [ForBuildPath] 
		--Здесь строим строку-путь к руту из названий 
		,dbo.usf2_FreeHierarchy_GetStringPathFromNumeric(@treeID, ToParentFreeHierPath) as ToRootPath
		from info_ti ti
		join Dict_FreeHierarchyIncludedObjectChildren h 
		on h.FreeHierTree_ID = @treeID and ID = TI_ID and TypeHierarchy = 4
		where TIATSCode like @searchTextLike
		order by LEN(h.StringName), StringName--, [charindex], [len]

	end
	else if (@paramName = 'Pik') begin
		--Номер PIK
		--TODO возможно, желательное добавить индекс по Comment в Info_Balance_FreeHierarchy_Comment
		select top(@topFind) ISNULL(cast(h.ID as varchar(22)), h.StringId) as ID, cast(h.TypeHierarchy as tinyint) as TypeHierarchy, h.StringName, '' as ObjectTypeName
		, c.Comment as MeterSerialNumber, cast(h.ParentID as varchar(22)) as ParentID, cast(h.ParentTypeHierarchy as tinyint) as ParentTypeHierarchy
		, h.FreeHierItemType
		, h.ParentName, NULL as FreeHierItemId, NULL as FreeHierTree_ID
		,PATINDEX(@searchTextLike, h.StringName) as [charindex]
		, ToParentFreeHierPath as [ForBuildPath] 
		--Здесь строим строку-путь к руту из названий 
		,dbo.usf2_FreeHierarchy_GetStringPathFromNumeric(@treeID, ToParentFreeHierPath) as ToRootPath
		from Info_Balance_FreeHierarchy_Comment c
		join [dbo].[Info_Balance_FreeHierarchy_Objects] o on o.BalanceFreeHierarchyObject_UN = c.BalanceFreeHierarchyObject_UN
		cross apply [dbo].usf2_FreeHierarchyObjectInfo(o.FreeHierItem_ID, o.HierLev1_ID, o.HierLev2_ID, o.HierLev3_ID, o.PS_ID, o.TI_ID) i
		join Dict_FreeHierarchyIncludedObjectChildren h 
		on h.FreeHierTree_ID = @treeID and h.ID = i.ID and h.TypeHierarchy = i.TypeHierarchy
		where Comment like @searchTextLike
		order by LEN(h.StringName), StringName--, [charindex], [len]

	end else if (@paramName = 'InstallationPlace') begin
		--Место установки прибора учета
		--TODO возможно, желательное добавить индекс по InstallationPlace в Hard_Meters
		select top(@topFind) ISNULL(cast(h.ID as varchar(22)), h.StringId) as ID, cast(h.TypeHierarchy as tinyint) as TypeHierarchy, h.StringName, '' as ObjectTypeName
		, m.InstallationPlace as MeterSerialNumber, cast(h.ParentID as varchar(22)) as ParentID, cast(h.ParentTypeHierarchy as tinyint) as ParentTypeHierarchy
		, h.FreeHierItemType
		, h.ParentName, NULL as FreeHierItemId, NULL as FreeHierTree_ID
		,PATINDEX(@searchTextLike, h.StringName) as [charindex]
		, ToParentFreeHierPath as [ForBuildPath] 
		--Здесь строим строку-путь к руту из названий 
		,dbo.usf2_FreeHierarchy_GetStringPathFromNumeric(@treeID, ToParentFreeHierPath) as ToRootPath
		from Hard_Meters m
		cross apply 
		(
			select top 1 TI_ID from Info_Meters_TO_TI mti 
			where mti.METER_ID = m.Meter_ID
			order by StartDateTime
		) ti
		join Dict_FreeHierarchyIncludedObjectChildren h 
		on h.FreeHierTree_ID = @treeID and h.ID = ti.TI_ID and h.TypeHierarchy = 4
		where m.InstallationPlace like @searchTextLike
		order by LEN(h.StringName), StringName--, [charindex], [len]

	end else begin
		--Обычный поиск по названию
		select top(@topFind) ISNULL(cast(h.ID as varchar(22)), h.StringId) as ID, cast(h.TypeHierarchy as tinyint) as TypeHierarchy, h.StringName, '' as ObjectTypeName
		, h.MeterSerialNumber, cast(h.ParentID as varchar(22)) as ParentID, cast(h.ParentTypeHierarchy as tinyint) as ParentTypeHierarchy
		, h.FreeHierItemType
		, h.ParentName, NULL as FreeHierItemId, NULL as FreeHierTree_ID
		,PATINDEX(@searchTextLike, h.StringName) as [charindex], ToParentFreeHierPath as [ForBuildPath] 
		--Здесь строим строку-путь к руту из названий 
		,dbo.usf2_FreeHierarchy_GetStringPathFromNumeric(@treeID, ToParentFreeHierPath) as ToRootPath
		from Dict_FreeHierarchyIncludedObjectChildren h 
		where h.FreeHierTree_ID = @treeID and TypeHierarchy in (select TypeHierarchy from #types) 
		--and [dbo].[usf2_UserHasRight](@User_ID, '6D95CECF-327A-408b-96E7-AF8EF27C0F64', h.ID, h.ObjectTypeName, null, @treeID, null) = 1 -- Проверяем право на прсмотр SeeDbObjects
		and 
		(
			@searchTextLike is null
			or
			h.StringName like @searchTextLike -- Ищем по названию
		)
		and 
		(
			--Фильтр по названию родителя
			@searchParentTextLike is null or (h.ParentName like @searchParentTextLike)
		)

		order by [charindex], LEN(h.StringName), StringName

	end;
	

	drop table #types
end

go
   grant EXECUTE on usp2_Info_FindHierObject to [UserCalcService]
go
