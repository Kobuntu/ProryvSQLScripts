if exists (select 1
          from sysobjects
          where  id = object_id('usp2_DictTariffInfoForPS')
          and type in ('P','PC'))
   drop procedure usp2_DictTariffInfoForPS
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
--		Июнь, 2011
--
-- Описание:
--
--		Читаем точки вместе с тарифными каналами для выбранной ПС
--
-- ======================================================================================
create proc [dbo].[usp2_DictTariffInfoForPS]

	@PS_ID int,
	@Phase int,
	@DateStart datetime,
	@DateEnd datetime,
	@CustomerKindList varchar(4000)

as
begin

set nocount on
set quoted_identifier, ansi_nulls, ansi_warnings, arithabort, concat_null_yields_null, ansi_padding on
set numeric_roundabort off
set transaction isolation level read uncommitted

create table #CustomerKindFilter(
id int
);

declare
@IsNullableCustomerKindEnabled bit,
@IsAllSelect bit;

set @IsAllSelect = 1;

--Выбираем точки у которых не задан CustomerKind
set @IsNullableCustomerKindEnabled = 1;

if (@CustomerKindList is not null AND @CustomerKindList <> '') BEGIN

	--Действует фильтр
	set @IsAllSelect = 0;
	insert into #CustomerKindFilter 
	select * from dbo.usf2_Utils_Split(@CustomerKindList, ',');
	
	--Смотрим выбирать ли точки у которых не задан CustomerKind
	if not Exists(select * from #CustomerKindFilter where id = 255)
		set @IsNullableCustomerKindEnabled = 0;
END;

select * 
into #tmp
from 
(
	select z.*, COUNT(TariffZone_ID) OVER(PARTITION BY Tariff_ID) AS 'Count' from DictTariffs_Zones z
	where [StartDateTime] <= @DateEnd AND ((NOT ([FinishDateTime] IS NOT NULL)) OR (@DateStart <= [FinishDateTime]))
) a where a.[Count] > 1 --Отсеиваем одноставочные тарифы

SELECT distinct [t0].[TI_ID], [t2].[ChannelType1], [t2].[ChannelType2], [t0].TIName, [t0].ps_id, PhaseNumber, [t0].SortNumber
FROM [dbo].[Info_TI] AS [t0]
left JOIN [dbo].[DictTariffs_ToTI] AS [t1] ON [t0].[TI_ID] = [t1].[TI_ID]
AND ([t1].[StartDateTime] <= @DateEnd) AND ((NOT ([t1].[FinishDateTime] IS NOT NULL)) OR (@DateStart <= [t1].[FinishDateTime]))
left JOIN [dbo].[#tmp] AS [t2] ON [t1].[Tariff_ID] = [t2].[Tariff_ID]
WHERE [t0].[PS_ID] = @PS_ID 
--Фильтр по фазам
AND (@Phase = -1 OR (@Phase = 0 AND PhaseNumber is not null) OR (@Phase is not null AND PhaseNumber = @Phase) OR (@Phase is null AND PhaseNumber is NULL)) 
--Чтобы небыло признака удаленной
AND ([t0].[Deleted] <> 1) 
--Фильтр по типу потребителя
AND (@IsAllSelect=1 OR ((@IsNullableCustomerKindEnabled = 1 AND [t0].CustomerKind is null) OR [t0].CustomerKind in (select id from #CustomerKindFilter)))
order by [t0].[SortNumber], [t0].[TI_ID]

drop table #tmp

end
go
   grant EXECUTE on usp2_DictTariffInfoForPS to [UserCalcService]
go