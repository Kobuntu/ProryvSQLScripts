if exists (select 1
          from sysobjects
          where  id = object_id('usp2_Info_GetBalanseHierarchy_List_ver2')
          and type in ('P','PC'))
   drop procedure usp2_Info_GetBalanseHierarchy_List_ver2
go
/****** Object:  StoredProcedure [dbo].[usp2_Info_GetBalanseHierarchy_List_ver2]    Script Date: 09/25/2008 12:46:51 ******/
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
--		Берем список балансов для соответствующего объекта
--
-- ======================================================================================

create  proc [dbo].[usp2_Info_GetBalanseHierarchy_List_ver2]

	-- Выбираем все формулы для данного подуровня
	-- Пока работает только для ПС и МСК
	@ID int,
	@Type tinyint --  2- МСК, 3- ПС версия 2

as
begin
if @Type=3 
	select BalancePS_UN as Balance_UN,BalancePSName  as Balance_Name,ForAutoUse 
	from dbo.Info_Balance_PS_List_2
	where (@ID = PS_ID)
	and  TI_ID is Null
end
go
   grant exec on usp2_Info_GetBalanseHierarchy_List_ver2 to [UserCalcService]
go