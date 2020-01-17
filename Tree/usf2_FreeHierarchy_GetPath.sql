set ANSI_NULLS ON
set QUOTED_IDENTIFIER ON
go
if exists (select 1
          from sysobjects
          where  id = object_id('usf2_FreeHierarchy_GetPath')
          and type in ('IF', 'FN', 'TF'))
   drop function usf2_FreeHierarchy_GetPath
go

-- ======================================================================================
-- Автор:
--
--		Малышев Игорь
--
-- Дата создания:
--
--		Май, 2019
--
-- Описание:
--
--		Возвращаем идентификаторы родителей до рута
--
-- ======================================================================================
create FUNCTION [dbo].[usf2_FreeHierarchy_GetPath]
(	
	@id varchar(22), --Идентификатор объекта
	@typeHierarchy tinyint, --Тип  объекта
	@FreeHierItem_ID int = null, --Идентификатор из дерева свободной иерархии (если нужно)
	@treeID int = null --Идентификатор дерева на котором ищем (если нужно)
)
RETURNS varchar(max) --Родителей упаковываем по порядку в строку и возвращаем в виде Parent_ID + ',' + Parent_Type + ',' + Parent_FreeHierItem_ID + ';' и т.д.
AS
begin

if (@id is null and @FreeHierItem_ID is null) return '';

if (@FreeHierItem_ID is null and @typeHierarchy = 28 and @treeID is not null) set @FreeHierItem_ID = @id

declare @fullPath nvarchar(1000)
if (@id is not null and @typeHierarchy is not null) begin
	set @fullPath = @id + ',' + cast(@typeHierarchy as varchar(2)) + ',' + case when @FreeHierItem_ID is null then '' else cast(@FreeHierItem_ID as varchar(22)) end  + ';';
end

declare @parendId varchar(22), @parentHierarchy tinyint, @parentFreeHierItem_ID int, @parentFreeHierItemType tinyint, @freeHierItemType tinyint;

---------Ищем по стандартному дереву------------------------------------------------------
if (@treeID is null OR @treeID < 0 OR @FreeHierItem_ID is NULL) begin

	set @parentFreeHierItem_ID = @FreeHierItem_ID;

	if (@typeHierarchy = 0) begin 
		----Объекты уровня 1
		return @fullPath; --Уже рут
	end else if (@typeHierarchy = 1) begin 
		----Объекты уровня 2
		set @parendId = (select top 1 HierLev1_ID from Dict_HierLev2 where HierLev2_ID = @id)
		set @parentHierarchy = 0;
		set @parentFreeHierItemType = 1;
		set @freeHierItemType = 2;
	end else if (@typeHierarchy = 2) begin 
		----Объекты уровня 3
		set @parendId = (select top 1 HierLev2_ID from Dict_HierLev3 where HierLev3_ID = @id)
		set @parentHierarchy = 1;
		set @parentFreeHierItemType = 2;
		set @freeHierItemType = 3;
	end else if (@typeHierarchy = 3) begin 
		----Объекты уровня 4
		set @parendId = (select top 1 HierLev3_ID from Dict_PS where PS_ID = @id)
		set @parentHierarchy = 2;
		set @parentFreeHierItemType = 3;
		set @freeHierItemType = 4;
	end else if (@typeHierarchy = 4) begin 
		----Объекты ТИ
		set @parendId = (select top 1 PS_ID from Info_TI where TI_ID = @id)
		set @parentHierarchy = 3;
		set @parentFreeHierItemType = 4;
		set @freeHierItemType = 5;
	end else if (@typeHierarchy = 8) begin 
		----Объекты ТП
		set @parendId = (select top 1 Section_ID from [dbo].[Info_Section_Description2] where TP_ID = @id)
		set @parentHierarchy = 5;
		set @parentFreeHierItemType = 7;
		set @freeHierItemType = 8;
	end else if (@typeHierarchy = 5) begin 
		----Объекты Сечения
		select top 1 @parendId = o.ID, @parentHierarchy=o.TypeHierarchy from [dbo].[Info_Section_List] sl with (nolock)
			cross apply [dbo].usf2_FreeHierarchyObjectInfo(NULL, sl.HierLev1_ID, sl.HierLev2_ID, sl.HierLev3_ID, sl.PS_ID, NULL) o
			where sl.Section_ID = @id
	end else if (@typeHierarchy = 11) begin 
		----Формулы
		select top 1 @parendId = o.ID, @parentHierarchy=o.TypeHierarchy from [dbo].[Info_Formula_List] fl with (nolock)
			cross apply [dbo].usf2_FreeHierarchyObjectInfo(fl.FreeHierItem_ID, fl.HierLev1_ID, fl.HierLev2_ID, fl.HierLev3_ID, fl.PS_ID, fl.TI_ID) o
			where fl.Formula_UN = @id
	end else if (@typeHierarchy = 32) begin 
		----Объекты УСПД
		set @parendId = (select top 1 ps.PS_ID from [dbo].[Hard_USPD] uspd with (nolock)
			join [dbo].Hard_USPDCommChannels_Links l on l.USPD_ID=uspd.USPD_ID
			join [dbo].Hard_CommChannels c on c.CommChannel_ID=l.CommChannel_ID
			join [dbo].Dict_PS ps on ps.PS_ID = c.PS_ID where uspd.USPD_ID = @id)
		set @parentHierarchy = 3;
		set @parentFreeHierItemType = 4;
	end else if (@typeHierarchy = 40) begin 
		----Объекты E422
		set @parendId = (select top 1 ps.PS_ID from [dbo].[Hard_E422] uspd with (nolock)
			join [dbo].Hard_E422CommChannels_Links l on l.E422_ID=uspd.E422_ID
			join [dbo].Hard_CommChannels c on c.CommChannel_ID=l.CommChannel_ID
			join [dbo].Dict_PS ps on ps.PS_ID = c.PS_ID where uspd.E422_ID = @id)
		set @parentHierarchy = 3;
		set @parentFreeHierItemType = 4;
	end else if (@typeHierarchy = 45) begin 
		----Балансы
		select top 1 @parendId = o.ID, @parentHierarchy=o.TypeHierarchy from [dbo].[Info_Balance_FreeHierarchy_List] b with (nolock)
			join [dbo].[Info_Balance_FreeHierarchy_Objects] fl on fl.BalanceFreeHierarchyObject_UN = b.BalanceFreeHierarchyObject_UN
			cross apply [dbo].usf2_FreeHierarchyObjectInfo(fl.FreeHierItem_ID, fl.HierLev1_ID, fl.HierLev2_ID, fl.HierLev3_ID, fl.PS_ID, fl.TI_ID) o
			where b.BalanceFreeHierarchy_UN = @id
	end

	if (@treeID > 0) begin

	--Если есть дерево, но нет идентификатора, то объект передан как IncludeObjectChildren от дочернего
	--Ищем FreeHierItem_ID родителя
		set @parentFreeHierItem_ID= (select top 1 f.FreeHierItem_ID  
			from dbo.vw_FreeHierarchyObjects f
			where f.FreeHierTree_ID = @treeID
				and f.ID = @parendId
				and f.FreeHierItemType = @parentFreeHierItemType
				and f.IncludeObjectChildren = 1
					
		)

		if (@parentFreeHierItem_ID is null) begin
		--Прямой родитель не найден в дереве свободной иеррархии, ищем собственный идентификатор через описание дерева

			declare @parentHierID hierarchyid

			select top 1 @FreeHierItem_ID = f.FreeHierItem_ID,
			@parentHierID = f.ParentHierID --Идентификатор родителя
							from dbo.vw_FreeHierarchyObjects f
							where f.FreeHierTree_ID = @treeID 
							and f.ID = @id 
							and f.FreeHierItemType = @freeHierItemType

			if (@FreeHierItem_ID is not null) begin

				--Нужно перестроить путь
				set @fullPath = @id + ',' + cast(@typeHierarchy as varchar(2)) + ',' + case when @FreeHierItem_ID is null then '' else cast(@FreeHierItem_ID as varchar(22)) end  + ';';

				--Берем информацию о родителе
				select top 1 @parentFreeHierItem_ID = f.FreeHierItem_ID,
				@parentHierarchy = f.TypeHierarchy,
				@parendId = f.ID
				from dbo.vw_FreeHierarchyObjects f
				where f.FreeHierTree_ID = @treeID
					and f.HierID = @parentHierID
			end else begin
			-- Объект не из этого дерева
				return null;
			end
		end 
		
	end else begin
		set @parentFreeHierItem_ID = null
	end
	
end else begin
---------Ищем по дереву свободной иерархии------------------------------------------------------

	if (@fullPath is null) begin
		select top 1 @fullPath = case when ID is null or ID <= 0 then StringId else ltrim(str(ID,15)) end + ',' + cast(TypeHierarchy as varchar(2)) + ',' + cast(FreeHierItem_ID as varchar(2))  + ';'
		from dbo.vw_FreeHierarchyObjects f
		where f.FreeHierTree_ID = @treeID 
		and FreeHierItem_ID = @FreeHierItem_ID
	end

	select top 1 @parentFreeHierItem_ID = f.FreeHierItem_ID, @parentHierarchy=dbo.usf2_Utils_ConvertFreeHierarchyToHierarchy(f.FreeHierItemType)
	, @parendId = f.ID  --Идентификатор
	from dbo.vw_FreeHierarchyObjects f
	where f.FreeHierTree_ID = @treeID 
	and f.HierID = (select top 1 ParentHierID from dbo.vw_FreeHierarchyObjects where FreeHierTree_ID = @treeID and FreeHierItem_ID = @FreeHierItem_ID)
	
end

-----------------------------------------------------------------------------------------
return @fullPath + dbo.usf2_FreeHierarchy_GetPath(@parendId, @parentHierarchy, @parentFreeHierItem_ID, @treeID); 

end
go
grant EXECUTE on usf2_FreeHierarchy_GetPath to [UserCalcService]
go