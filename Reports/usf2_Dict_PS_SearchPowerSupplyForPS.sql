set ANSI_NULLS ON
set QUOTED_IDENTIFIER ON
go
if exists (select 1
          from sysobjects
          where  id = object_id('usf2_Dict_PS_SearchPowerSupplyForPS')
          and type in ('IF', 'FN', 'TF'))
   drop function usf2_Dict_PS_SearchPowerSupplyForPS
go

-- ======================================================================================
-- Автор:
--
--		Малышев Игорь
--
-- Дата создания:
--
--		Декабрь, 2010
--
-- Описание:
--
--		Ищем идентификатор конечной питающей ТП для дома
--
-- ======================================================================================
create FUNCTION [dbo].[usf2_Dict_PS_SearchPowerSupplyForPS] (
		@PS_ID   int --Идентификатор дома
)

 RETURNS int 
 AS BEGIN

declare @PowerSupplyPS_ID int;

--Строим цепочку питающих подстанций
with SearchPowerSupplyForPS(PowerSupplyPS_ID, PS_ID, rowNumber) as
(
	select PowerSupplyPS_ID, PS_ID, 0 as rowNumber from dbo.Dict_PS_PowerSupply_PS_List 
	where PS_ID = @ps_id
	union all
	select t.PowerSupplyPS_ID, t.PS_ID, rowNumber + 1 from dbo.Dict_PS_PowerSupply_PS_List t
	join SearchPowerSupplyForPS r on t.PS_ID = r.PowerSupplyPS_ID
	where rowNumber < 100 --ограниечение
) 

--Берем последнюю питающую в цепочке
select @PowerSupplyPS_ID = PowerSupplyPS_ID from SearchPowerSupplyForPS where rowNumber = (select MAX(rowNumber) from SearchPowerSupplyForPS);

RETURN @PowerSupplyPS_ID
END;
go
grant EXECUTE on usf2_Dict_PS_SearchPowerSupplyForPS to [UserCalcService]
go