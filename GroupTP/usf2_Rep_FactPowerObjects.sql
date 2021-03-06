if exists (select 1
          from sysobjects
          where  id = object_id('usf2_Rep_FactPowerObjects')
          and type in ('IF', 'FN', 'TF'))
   drop function usf2_Rep_FactPowerObjects
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
----		Декабрь, 2016
----
---- Описание:
----
----		Данные для формирования отчетов с факт. мощностью
----
---- ======================================================================================
create function [dbo].[usf2_Rep_FactPowerObjects]
(
	@list varchar(max), --Список идентификаторов+тип объектов
	@DateStart DateTime, --Начальная дата, конечная дата
	@DateEnd DateTime--,
	--@OnlyStartAndStopDate bit = 0 -- Только начальную и конечную даты
)
RETURNS TABLE
AS
RETURN
(
	select * from 
	(
	select id.ID, id.TypeHierarchy, SecondaryID as Section_ID, SectionName,
	(select top 1 ContractNumber from Info_Section_To_JuridicalContract sjc 
	join Dict_JuridicalPersons_Contracts jc on sjc.JuridicalPersonContract_ID = jc.JuridicalPersonContract_ID where sl.Section_ID = sjc.Section_ID) as ContractNumber,
	case when sl.HierLev2_ID is not null then (select top 1 StringName from Dict_HierLev2 where HierLev2_ID = sl.HierLev2_ID)
	 when sl.HierLev3_ID is not null then (select top 1 StringName from Dict_HierLev2 where HierLev2_ID = (select HierLev2_ID from Dict_HierLev3 where HierLev3_ID = sl.HierLev3_ID)) end as POName,
	 pc.StringName as PriceCategoryName,
	 (
		case TypeHierarchy
			--Сечения
			when 5 then (select top 1 SectionName from Info_Section_List where Section_ID = id.ID) 
			--ТП
			when 8 then (select top 1 StringName from Info_TP2 where TP_ID = id.ID) 
			--Прямые потребители
			when 16 then (select top 1 StringName from Dict_DirectConsumer where DirectConsumer_ID = id.ID) 
			--Юр.лицо
			when 19 then (select top 1 StringName from Dict_JuridicalPersons where JuridicalPerson_ID = id.ID) 
			--Контракты юр.лиц
			when 20 then (select top 1 StringName from Dict_JuridicalPersons_Contracts where JuridicalPersonContract_ID = id.ID) 
			--Остальные объекты добавлять по мере необходимости
		end
	 ) as [Name],
	 (
		case TypeHierarchy
			--ТП
			when 8 then (select top 1 StringName from Dict_DirectConsumer where DirectConsumer_ID = (select top 1 DirectConsumer_ID from Info_TP2 where TP_ID = id.ID)) 
			--Прямые потребители
			when 16 then (select top 1 StringName from Dict_DirectConsumer where DirectConsumer_ID = id.ID) 
			else ''
		end
	 ) as DirectConsumerName
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
	, -1) as SecondaryID

	from [dbo].[usf2_Utils_iter_intlist_to_table](@list)
	) id
	left join Info_Section_List sl on sl.Section_ID = SecondaryID
	outer apply 
	(
	  select top 1 pc.PriceCategory_ID, StringName from  Dict_PriceCategory_To_Section pcs 
	  join Dict_PriceCategory pc on pc.PriceCategory_ID = pcs.PriceCategory_ID where pcs.Section_ID = sl.Section_ID
	 and StartMonthYear <= @DateStart and ISNULL(FinishMonthYear, '21000101') >= @DateStart order by StartMonthYear desc 
	) pc
	) a
);
go
grant select on usf2_Rep_FactPowerObjects to [UserCalcService]
go
