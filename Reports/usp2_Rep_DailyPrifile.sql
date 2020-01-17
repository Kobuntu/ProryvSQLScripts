if exists (select 1
          from sysobjects
          where  id = object_id('usp2_Rep_DailyPrifile')
          and type in ('P','PC'))
   drop procedure usp2_Rep_DailyPrifile
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
----		Сентябрь, 2012
----
---- Описание:
----
----		Суточный отчет (профиль) по счетчикам (1.9)
----
---- ======================================================================================
create proc [dbo].[usp2_Rep_DailyPrifile]

	@PSArray varchar(4000),
	@DTStart DateTime,
	@DTEnd DateTime
as

set nocount on
set quoted_identifier, ansi_nulls, ansi_warnings, arithabort, concat_null_yields_null, ansi_padding on
set numeric_roundabort off
set transaction isolation level read uncommitted

declare
@TIArray tinyint;

select usf.*, isnull(tarif.TariffTypeAddStringName + ' ' + zones.StringName, 'Одноставочный') as TariffName, -- Тариф
case when AbsentChannelsMask is not null and (AbsentChannelsMask & 1) = 1 
	then case when ISNULL(AIATSCode,1) = 1 then ISNULL(ChannelType2, 2) else ISNULL(ChannelType1, 1) end
	else case when ISNULL(AIATSCode,1) = 1 then ISNULL(ChannelType1, 1) else ISNULL(ChannelType2, 2) end
end as ChannelType
into #tableTI
from usf2_Rep_Info_TI(@PSArray, null) usf
join Info_TI ti on ti.TI_ID = usf.TI_ID --Добираем недостающую информацию
left join dbo.DictTariffs_ToTI tti on tti.TI_ID = usf.TI_ID and tti.StartDateTime <= @DTEnd and (tti.FinishDateTime is null OR tti.FinishDateTime >= @DTStart)
left join dbo.DictTariffs_Tariffs tarif on tarif.Tariff_ID = tti.Tariff_ID
left join dbo.DictTariffs_Zones zones on zones.Tariff_ID = tti.Tariff_ID and zones.StartDateTime <= @DTEnd and (zones.FinishDateTime is null OR zones.FinishDateTime >= @DTStart)


--Информация по точкам
select * from #tableTI order by PSName desc, TIName desc;
drop table #tableTI;

go
  grant EXECUTE on usp2_Rep_DailyPrifile to [UserCalcService]
go