if exists (select 1
          from sysobjects
          where  id = object_id('usp2_FreeHierarchyTree_GetChildren')
          and type in ('P','PC'))
   drop procedure usp2_FreeHierarchyTree_GetChildren
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
--		Март, 2017
--
-- Описание:
--
--		Строим дерево
--
-- ======================================================================================
create proc [dbo].[usp2_FreeHierarchyTree_GetChildren]

	@itemIDs IntType readonly,
	@freeHierarchyChildrenExpandSQLType tinyint,
	@treeID int,
	@returnCurrent bit,
	@onlySQLGenerated bit = 0
as

begin
	set nocount on
	set quoted_identifier, ansi_nulls, ansi_warnings, arithabort, concat_null_yields_null, ansi_padding on
	set numeric_roundabort off
	set transaction isolation level read uncommitted
	declare @sqlSelectString nvarchar(max);
	declare @sqlString nvarchar(max);
		set @sqlSelectString = ' select * from (select distinct 
		t.HierID.ToString() as HierID,t.HierLevel,t.FreeHierItem_ID,t.StringName,
		t.FreeHierItemType,t.Expanded,
		Nodeicon_ID = t.FreeHierIcon_ID,
		d.HierLev1_ID,d.HierLev2_ID,d.HierLev3_ID,d.PS_ID,d.TI_ID,d.Formula_UN,d.Section_ID,d.TP_ID,d.USPD_ID,d.XMLSystem_ID,d.JuridicalPerson_ID,d.JuridicalPersonContract_ID,d.DistributingArrangement_ID,d.BusSystem_ID,d.UANode_ID,
		d.ForecastObject_UN,
		d.OurFormula_UN,ISNULL(d.IncludeObjectChildren, 0) as IncludeObjectChildren,d.SqlSelectString_ID,t.SortNumber
		' + case when @onlySQLGenerated = 1 then ',[dbo].[usf2_FreeHierarchy_GetPath](null, null, d.FreeHierItem_ID, @treeID) as ToRootPath' else '' end + '
		from Dict_FreeHierarchyTree t 
		left join Dict_FreeHierarchyTree_Description d on t.FreeHierItem_ID=d.FreeHierItem_ID 
		left join Info_TI ti on t.FreeHierItemType = 5 and ti.TI_ID = d.TI_ID and Deleted <> 1
		left join Forecast_Objects f on t.FreeHierItemType = 29 and f.ForecastObject_UN = d.ForecastObject_UN and Deleted <> 1 ';

		if (not exists(select top 1 1 from @itemIDs)) begin
			--Выборка всего дерева
			if (@freeHierarchyChildrenExpandSQLType = 1) begin
				set @sqlString = ' declare @minHielLevel int; 
					set @minHielLevel = (Select MIN(HierLevel) from Dict_FreeHierarchyTree WHERE Dict_FreeHierarchyTree.FreeHierTree_ID= @treeID )' +
                     @sqlSelectString + ' ' + ' where t.FreeHierTree_ID= @treeID AND t.HierLevel = @minHielLevel';
			end else begin
				set @sqlString = @sqlSelectString + ' where t.FreeHierTree_ID= @treeID';
			end

			if (@onlySQLGenerated = 1) begin
				-- Возвращаем только узлы по которым дочерние нужно сгенерировать только SQL запросом
				set @sqlString = @sqlString + ' and d.SqlSelectString_ID is not null'
			end
		end else begin
			--Если заданы конкретные узлы
			declare @whereSelect nvarchar(1000);
			if (@returnCurrent = 1) set @whereSelect =  ' s.HierID = t.HierID or ';
			else set @whereSelect = '';
			if (@freeHierarchyChildrenExpandSQLType = 0) begin
				set @whereSelect = @whereSelect + 't.HierID.IsDescendantOf(s.HierID)=1';
			end else begin
				set @whereSelect = @whereSelect + 't.HierID.GetAncestor(1)=s.HierID ';
			end
			 set @sqlString = 'select HierID into #ht from @itemIDs s join Dict_FreeHierarchyTree df on df.FreeHierItem_ID = s.Id;' +
                                @sqlSelectString +'join #ht s on ' + @whereSelect + ' where t.FreeHierTree_ID=@treeID';
		end
		--Контроль целостности
		set @sqlString = @sqlString + ' and (t.FreeHierItemType = 0 or
			  (t.FreeHierItemType = 1 and d.HierLev1_ID is not null) or
			  (t.FreeHierItemType = 2 and d.HierLev2_ID is not null) or
			  (t.FreeHierItemType = 3 and d.HierLev3_ID is not null) or
			  (t.FreeHierItemType = 4 and d.PS_ID is not null) or
			  (t.FreeHierItemType = 5 and ti.TI_ID is not null) or
			  (t.FreeHierItemType = 6 and d.Formula_UN is not null) or
			  (t.FreeHierItemType = 7 and d.Section_ID is not null) or
			  (t.FreeHierItemType = 8 and d.TP_ID is not null) or
			  (t.FreeHierItemType = 9 and d.USPD_ID is not null) or
			  (t.FreeHierItemType = 12 and d.XMLSystem_ID is not null) or
			  (t.FreeHierItemType = 13 and d.JuridicalPerson_ID is not null) or
			  (t.FreeHierItemType = 10 and d.JuridicalPersonContract_ID is not null) or
			  (t.FreeHierItemType = 18 and d.DistributingArrangement_ID is not null) or
			  (t.FreeHierItemType = 19 and d.BusSystem_ID is not null) or
			  (t.FreeHierItemType = 23 and d.UANode_ID is not null) or
			  (t.FreeHierItemType = 29 and f.ForecastObject_UN is not null) or
			  (t.FreeHierItemType = 14 and d.OurFormula_UN is not null) 
			  )) t order by t.HierLevel, case when t.SortNumber is null then 1 else 0 end, t.SortNumber,t.StringName';
	--select @sqlString
	EXEC sp_executesql @sqlString, N'@treeID int, @itemIDs IntType READONLY', @treeID, @itemIDs
end

go
   grant EXECUTE on usp2_FreeHierarchyTree_GetChildren to [UserCalcService]
go

   grant EXECUTE on usp2_FreeHierarchyTree_GetChildren to [UserDeclarator]
go








 