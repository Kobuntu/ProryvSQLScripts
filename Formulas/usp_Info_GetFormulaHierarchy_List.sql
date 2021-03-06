if exists (select 1
          from sysobjects
          where  id = object_id('usp2_Info_GetFormulaHierarchy_List')
          and type in ('P','PC'))
   drop procedure usp2_Info_GetFormulaHierarchy_List
go
/****** Object:  StoredProcedure [dbo].[usp2_Info_GetFormulaHierarchy_List]    Script Date: 09/25/2008 12:47:23 ******/
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
--		Сентябрь, 2008
--
-- Описание:
--
--		Берем список формул для соответствующего объекта
--
-- ======================================================================================

create proc [dbo].[usp2_Info_GetFormulaHierarchy_List]

	-- Выбираем все формулы для данного подуровня
	@ID int,
	@Type tinyint --  0-для МЭС, 1- ПМС, 2- МСК, 3- ПС, 4- ТИ

as
begin
if @Type=0 
	select Formula_UN,FormulaName, @ID as ParentId, @Type as ParentType
	from Info_Formula_List 
	where (@ID = HierLev1_ID)
	and  (TI_ID is Null) and  (HierLev2_ID is Null) and  (HierLev3_ID is Null) and (PS_ID is Null)
	order by SortNumber
else if @Type=1 
	select Formula_UN,FormulaName, @ID as ParentId, @Type as ParentType
	from Info_Formula_List 
	where (@ID = HierLev2_ID)
	and  (TI_ID is Null) and  (HierLev3_ID is Null) and (PS_ID is Null)
	order by SortNumber
else if @Type=2 
	select Formula_UN,FormulaName, @ID as ParentId, @Type as ParentType 
	from Info_Formula_List 
	where (@ID = HierLev3_ID)
	and  (TI_ID is Null) and (PS_ID is Null)
	order by SortNumber
else if @Type=3 
	select Formula_UN,FormulaName, @ID as ParentId, @Type as ParentType 
	from Info_Formula_List 
	where (@ID = PS_ID)
	and  TI_ID is Null
	order by SortNumber
else if @Type=4 
	select Formula_UN,FormulaName, @ID as ParentId, @Type as ParentType 
	from Info_Formula_List 
	where (@ID = TI_ID)
	order by SortNumber
	
end
go
   grant EXECUTE on usp2_Info_GetFormulaHierarchy_List to [UserCalcService]
go
