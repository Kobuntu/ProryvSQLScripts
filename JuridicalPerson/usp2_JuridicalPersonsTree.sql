if exists (select 1
          from sysobjects
          where  id = object_id('usp2_JuridicalPersonsTree')
          and type in ('P','PC'))
   drop procedure usp2_JuridicalPersonsTree
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
--		Октябрь, 2013
--
-- Описание:
--
--		Формирование дерева юр. лиц
--
-- ======================================================================================
create proc [dbo].[usp2_JuridicalPersonsTree]
 @juridicalPersonID int = 0
as

begin
set nocount on
set quoted_identifier, ansi_nulls, ansi_warnings, arithabort, concat_null_yields_null, ansi_padding on
set numeric_roundabort off
set transaction isolation level read uncommitted

if (@juridicalPersonID = 0) select * from Info_Section_To_JuridicalContract;

select * from Dict_JuridicalPersons_To_HierLevels
where @juridicalPersonID = 0 OR (@juridicalPersonID > 0 and JuridicalPerson_ID = @juridicalPersonID)

select * from Dict_JuridicalPersons
where @juridicalPersonID = 0 OR (@juridicalPersonID > 0 and JuridicalPerson_ID = @juridicalPersonID)


select * from Dict_JuridicalPersons_Contracts
where PowerSupplyingIntermediary_ID is not null 
and (@juridicalPersonID = 0 OR (@juridicalPersonID > 0 and JuridicalPerson_ID = @juridicalPersonID))
 
end
go
   grant EXECUTE on usp2_JuridicalPersonsTree to [UserCalcService]
go