if exists (select 1
          from sysobjects
          where  id = object_id('usp2_Info_GetSectionActHierarchy_List')
          and type in ('P','PC'))
   drop procedure usp2_Info_GetSectionActHierarchy_List
go
/****** Object:  StoredProcedure [dbo].[usp2_Info_GetSectionActHierarchy_List]    Script Date: 09/30/2008 21:33:59 ******/
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
--		Береб список формул для соответствующего объекта
--
-- ======================================================================================

create proc [dbo].[usp2_Info_GetSectionActHierarchy_List]

	-- Выбираем все формулы для данного подуровня
	@ID int,
	@Type tinyint --  0-для МЭС, 1- ПМС, 2- МСК, 3- ПС, 4- ТИ

as
begin
if @Type=0 
	select Section_ID,SectionName
	from dbo.Info_Section_List
	where (@ID = HierLev1_ID)
	and  (HierLev2_ID is Null) and  (HierLev3_ID is Null) and (PS_ID is Null)
if @Type=1 
	select Section_ID,SectionName 
	from dbo.Info_Section_List 
	where (@ID = HierLev2_ID)
	and  (HierLev3_ID is Null) and (PS_ID is Null)
if @Type=2 
	select Section_ID,SectionName 
	from dbo.Info_Section_List
	where (@ID = HierLev3_ID)
	and  (PS_ID is Null)
if @Type=3 
	select Section_ID,SectionName 
	from dbo.Info_Section_List
	where (@ID = PS_ID)
end
go
   grant EXECUTE on usp2_Info_GetSectionActHierarchy_List to [UserCalcService]
go
