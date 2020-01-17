if exists (select 1
          from sysobjects
          where  id = object_id('usp2_Info_FindAbonentbyNameORCode')
          and type in ('P','PC'))
   drop procedure usp2_Info_FindAbonentbyNameORCode
go
/****** Object:  StoredProcedure [dbo].[usp2_InfoFormulaSelect]    Script Date: 10/02/2008 22:40:23 ******/
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
--		Март, 2011
--
-- Описание:
--
--		Поиск по абоненту
--
-- ======================================================================================
create proc [dbo].[usp2_Info_FindAbonentbyNameORCode]
(
	@searchText nvarchar(1000),	--Текст для поиска
	@nowDateTime DateTime,
	@parrentsList nvarchar(4000) = null --Родители по которым фильтруем
)
as
begin

set @searchText = '"' + @searchText + '*"';

select top 100 al.*, ti.TI_ID, ti.PS_ID, dt.Tariff_ID 
from dbo.InfoBit_Abonents_List al
join dbo.InfoBit_Abonents_To_TI at on at.BitAbonent_ID = al.BitAbonent_ID and @nowDateTime between at.StartDateTime and at.FinishDateTime
join dbo.Info_TI ti on ti.TI_ID = at.TI_ID
left join dbo.DictTariffs_ToTI dt on dt.TI_ID = at.TI_ID and @nowDateTime between dt.StartDateTime and dt.FinishDateTime
where (CONTAINS((al.BitAbonentSurname, al.BitAbonentCode), @searchText)) 
and (@parrentsList is null OR LEN(@parrentsList) = 0 OR (ti.PS_ID in (select * from dbo.usf2_Utils_Split(@parrentsList,','))))
order by al.BitAbonentSurname, al.BitAbonentName;
	
end
go
   grant EXECUTE on usp2_Info_FindAbonentbyNameORCode to [UserCalcService]
go