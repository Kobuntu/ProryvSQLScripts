if exists (select 1
          from sysobjects
          where  id = object_id('usf2_Rep_FactPowerVoltageLevels')
          and type in ('IF', 'FN', 'TF'))
   drop function usf2_Rep_FactPowerVoltageLevels
go


SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

---- ======================================================================================
---- Автор:
----
----		Малышев Игорь
----
---- Дата создания:
----
----		Январь, 2016
----
---- Описание:
----
----		Все уровни напряжения, по ТП
----
---- ======================================================================================
create function [dbo].[usf2_Rep_FactPowerVoltageLevels]
(
	@list varchar(max),		--Список идентификаторов+тип объектов
	@DateStart DateTime,	--Начальная дата, 
	@DateEnd DateTime	-- конечная дата
)
RETURNS TABLE
AS
RETURN
(
	select TP_ID, VoltageLevel from
(
select distinct sd.TP_ID
	from 
	(
		select distinct TInumber as ID, CHnumber as TypeHierarchy, 
		ISNULL(
		case CHnumber
		when 5 then TInumber
		when 20 then (
			select top 1 Section_ID from Info_Section_To_JuridicalContract where JuridicalPersonContract_ID = TInumber 
		)
		when 19 then (
			select top 1 Section_ID from Dict_JuridicalPersons_Contracts jpc
			join Info_Section_To_JuridicalContract sjc on sjc.JuridicalPersonContract_ID = jpc.JuridicalPersonContract_ID
			where jpc.JuridicalPerson_ID = TInumber
		)
		when 8 then (
			select top 1 Section_ID from Info_Section_Description2 sd 
			where sd.TP_ID = TInumber
		)
		when 16 then (
			select top 1 Section_ID from Info_TP2 tp 
			join Info_Section_Description2 sd on sd.TP_ID = tp.TP_ID
			where tp.DirectConsumer_ID = TInumber
		)
		END
	, -1) as Section_ID

	from [dbo].[usf2_Utils_iter_intlist_to_table](@list)
	) id
	join Info_Section_Description2 sd on (id.TypeHierarchy = 8 and sd.TP_ID = id.ID) or (id.TypeHierarchy <> 8 and sd.Section_ID = id.Section_ID)
) tp
cross apply
(
	select top 1 VoltageLevel from [dbo].[Info_TP_VoltageLevel] v
	where v.TP_ID = tp.TP_ID and (@DateEnd is null or StartDateTime < @DateEnd) and (FinishDateTime is null or FinishDateTime > @DateStart)
	order by StartDateTime desc
) v
);
go
grant select on usf2_Rep_FactPowerVoltageLevels to [UserCalcService]
go
