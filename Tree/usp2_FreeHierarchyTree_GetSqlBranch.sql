if exists (select 1
          from sysobjects
          where  id = object_id('usp2_FreeHierarchyTree_GetSqlBranch')
          and type in ('P','PC'))
   drop procedure usp2_FreeHierarchyTree_GetSqlBranch
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
--		Апрель, 2018
--
-- Описание:
--
--		Возвращаем ветку указанного родительского объекта по выборке из SQL
--
-- ======================================================================================
Create proc [dbo].[usp2_FreeHierarchyTree_GetSqlBranch]
	@userId varchar(22),
	@parentId int,
	@treeID int,
	@HierLev1_ID tinyint = null, 
		@HierLev2_ID int = null, 
		@HierLev3_ID int = null, 
		@PS_ID int = null, 
		@TI_ID int = null, 
		@Formula_UN varchar(22) = null, 
		@Section_ID int = null,  
		@TP_ID int = null, 
		@USPD_ID int = null, 
		@XMLSystem_ID int = null, 
		@JuridicalPerson_ID int = null, 
		@JuridicalPersonContract_ID int = null, 
		@DistributingArrangement_ID int = null, 
		@BusSystem_ID int = null, 
		@UANode_ID bigint = null, 
		@OurFormula_UN varchar(22) = null, 
		@ForecastObject_UN varchar(22) = null,
		@FreeHierItem_ID int = null
as

begin
	set nocount on
	set quoted_identifier, ansi_nulls, ansi_warnings, arithabort, concat_null_yields_null, ansi_padding on
	set numeric_roundabort off
	set transaction isolation level read uncommitted

	--@userId varchar(22),
	--@parentId int,
	---@treeID int

	declare @sqlSelectString nvarchar(max);
	
	select top 1 @sqlSelectString = s.sqlSelectString
	from Dict_FreeHierarchyTree t
	join Dict_FreeHierarchyTree_Description d on d.FreeHierItem_ID = t.FreeHierItem_ID
	join [dbo].[Dict_FreeHierarchySQL] s on s.SqlSelectString_ID = d.SqlSelectString_ID
	where t.FreeHierTree_ID = @treeID and t.FreeHierItem_ID = @parentId;
	

	declare @sqlParams nvarchar(max)
	set @sqlParams = N'@USER_ID varchar(128),
	@HierLev1_ID tinyint, 
	@HierLev2_ID int, 
	@HierLev3_ID int, 
	@PS_ID int, 
	@TI_ID int, 
	@Formula_UN varchar(22), 
	@Section_ID int,  
	@TP_ID int, 
	@USPD_ID int, 
	@XMLSystem_ID int, 
	@JuridicalPerson_ID int, 
	@JuridicalPersonContract_ID int, 
	@DistributingArrangement_ID int, 
	@BusSystem_ID int, 
	@UANode_ID bigint, 
	@OurFormula_UN varchar(22), 
	@ForecastObject_UN varchar(22),
	@FreeHierItem_ID int';

	EXEC sp_executesql @sqlSelectString, @sqlParams, @userId, @HierLev1_ID, 
	@HierLev2_ID, 
	@HierLev3_ID, 
	@PS_ID, 
	@TI_ID, 
	@Formula_UN, 
	@Section_ID,  
	@TP_ID, 
	@USPD_ID, 
	@XMLSystem_ID, 
	@JuridicalPerson_ID, 
	@JuridicalPersonContract_ID, 
	@DistributingArrangement_ID, 
	@BusSystem_ID, 
	@UANode_ID, 
	@OurFormula_UN, 
	@ForecastObject_UN,
	@FreeHierItem_ID;


end
go
   grant EXECUTE on usp2_FreeHierarchyTree_GetSqlBranch to [UserCalcService]
go
