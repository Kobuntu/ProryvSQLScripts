if exists (select 1
          from sysobjects
          where  id = object_id('usf2_FreeHierarchyTreeDescriptionTypeAndId')
          and type in ('IF', 'FN', 'TF'))
   drop function usf2_FreeHierarchyTreeDescriptionTypeAndId
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
--		Возвращыем тип и идентификатор поля из дерева иерархии исходя из самого объекта
--
-- ======================================================================================
CREATE FUNCTION [dbo].[usf2_FreeHierarchyTreeDescriptionTypeAndId]
(
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
	@FreeHierItem_ID int
)
returns @objectTable TABLE
(
 Object_ID varchar(22),
 ObjectTypeName varchar(255)
)
AS
BEGIN
	if (@PS_ID is not null) insert into @objectTable values (@PS_ID, 'Dict_PS_')
	else if (@HierLev3_ID is not null) insert into @objectTable values (@HierLev3_ID, 'Dict_HierLev3')
	else if (@HierLev2_ID is not null) insert into @objectTable values (@HierLev2_ID, 'Dict_HierLev2_')
	else if (@HierLev1_ID is not null) insert into @objectTable values (@HierLev1_ID, 'Dict_HierLev1_')
	else if (@Section_ID is not null) insert into @objectTable values (@Section_ID, 'Info_Section_List')
	else if (@UANode_ID is not null) insert into @objectTable values (@UANode_ID, 'UANode')
	else if (@JuridicalPersonContract_ID is not null) insert into @objectTable values (@JuridicalPersonContract_ID, 'Dict_JuridicalPersons_Contracts')
	else if (@JuridicalPerson_ID is not null) insert into @objectTable values (@JuridicalPerson_ID, 'Dict_JuridicalPersons')
	else if (@XMLSystem_ID is not null) insert into @objectTable values (@XMLSystem_ID, 'Expl_XML_System_List')
	else if (@BusSystem_ID is not null) insert into @objectTable values (@BusSystem_ID, 'Dict_BusSystem')
	else if (@ForecastObject_UN is not null) insert into @objectTable values (@ForecastObject_UN, 'Forecast_Objects')
	else if (@TI_ID is not null) insert into @objectTable values (@TI_ID, 'Info_TI')
	else if (@TP_ID is not null) insert into @objectTable values (@TP_ID, 'Info_TP2')
	else if (@DistributingArrangement_ID is not null) insert into @objectTable values (@DistributingArrangement_ID, 'Dict_DistributingArrangement')
	else if (@Formula_UN is not null) insert into @objectTable values (@Formula_UN, 'Info_Formula_List')
	else if (@OurFormula_UN is not null) insert into @objectTable values (@OurFormula_UN, 'Info_TP2_OurSide_Formula_List')
	else if (@USPD_ID is not null) insert into @objectTable values (@USPD_ID, 'Hard_USPD')
	else insert into @objectTable values (@FreeHierItem_ID, 'Dict_FreeHierarchyTree')
	RETURN
END
go
grant select on usf2_FreeHierarchyTreeDescriptionTypeAndId to [UserCalcService]
go