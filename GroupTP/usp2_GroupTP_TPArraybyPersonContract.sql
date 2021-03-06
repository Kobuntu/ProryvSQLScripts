if exists (select 1
          from sysobjects
          where  id = object_id('usp2_GroupTP_TPArraybyPersonContract')
          and type in ('P','PC'))
   drop procedure usp2_GroupTP_TPArraybyPersonContract
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
--		Декабрь, 2012
--
-- Описание:
--
--		Выбираем список ТП по контрактам
--
-- ======================================================================================

create proc [dbo].[usp2_GroupTP_TPArraybyPersonContract]
	@HierLev2_ID int = null, --Уровни по которым фильтруем
	@StartMonthYear DateTime = null, -- Месяц, год первого отчетного периода
	@FinishMonthYear DateTime = null, -- Месяц, год последнего отчетного периода
	@PowerSupplyingIntermediary_ID int = null, 
	@User_ID varchar(22)
as

set nocount on
set quoted_identifier, ansi_nulls, ansi_warnings, arithabort, concat_null_yields_null, ansi_padding on
set numeric_roundabort off
set transaction isolation level read uncommitted

if (@PowerSupplyingIntermediary_ID < 0) set @PowerSupplyingIntermediary_ID = null;
if (@HierLev2_ID < 0) set @HierLev2_ID = null;

--Отчетный период, по умолчанию ставим последний месяц
if (@StartMonthYear is null) set @StartMonthYear = GETDATE();
if (@FinishMonthYear is null) set @FinishMonthYear = @StartMonthYear;

declare @dtStart DateTime, @dtEnd DateTime;

--Округляем до 1 числа, 00 часов и минут
set @dtStart = floor(cast(DATEADD(day, - Day(@StartMonthYear) + 1, @StartMonthYear) as float));
set @dtEnd = floor(cast(DATEADD(day, - Day(@FinishMonthYear) + 1, @FinishMonthYear) as float));
set @dtEnd = DATEADD(second, -1, DATEADD(month, 1, @dtEnd));


declare @hierLev1Name nvarchar(255), @powerSupplyingIntermediaryName nvarchar(256);

if (@HierLev2_ID is not null) begin
	set @hierLev1Name = (select StringName from Dict_HierLev2 where HierLev2_ID = @HierLev2_ID);
end else if (@PowerSupplyingIntermediary_ID is not null) begin
	set	@hierLev1Name = (select StringName from Dict_HierLev1 
		where HierLev1_Id = (select HierLev1_ID from Dict_JuridicalPersons_To_HierLevels 
			where JuridicalPerson_ID = @PowerSupplyingIntermediary_ID))
	if (@hierLev1Name is null)
		set	@hierLev1Name = (select StringName from Dict_HierLev1 where HierLev1_Id = (select HierLev1_Id from Dict_HierLev2
			where HierLev2_Id = (select HierLev2_ID from Dict_JuridicalPersons_To_HierLevels 
				where JuridicalPerson_ID = @PowerSupplyingIntermediary_ID)))
	if (@hierLev1Name is null)
		set	@hierLev1Name = (select StringName from Dict_HierLev1 
			where HierLev1_ID = (select HierLev1_ID from Dict_HierLev2
				where HierLev2_Id = (select HierLev2_Id from Dict_HierLev3
					where HierLev3_Id = (select HierLev3_ID from Dict_JuridicalPersons_To_HierLevels 
						where JuridicalPerson_ID = @PowerSupplyingIntermediary_ID))))
end;

if (@PowerSupplyingIntermediary_ID is not null) begin
	set @powerSupplyingIntermediaryName = (select StringName from Dict_JuridicalPersons where JuridicalPerson_ID = @PowerSupplyingIntermediary_ID);
end;

--Названия родителей, для отчетов
select @hierLev1Name as hierLev1Name, @powerSupplyingIntermediaryName as powerSupplyingIntermediaryName

	--Объекты доступные данному пользователю
	select cast(o.Object_ID as int) as Hier2_id
	into #HierIdByRight
	from [dbo].[Expl_User_UserGroup] ug
	join [dbo].[Expl_UserGroup_Right] ur on ur.UserGroup_ID = ug.UserGroup_ID
	join [dbo].[Expl_Users_DBObjects] o on o.ID = ur.DBObject_ID
	where ug.User_ID = @User_ID and ug.Deleted <> 1 and ur.Deleted <> 1 and o.Deleted <> 1
	and ObjectTypeName = 'Dict_HierLev2_' and RIGHT_ID = '6D95CECF-327A-408b-96E7-AF8EF27C0F64' --Признак SeeDbObjects

declare @HaveRightRows bit;

if (exists(select top 1 1 from #HierIdByRight)) set @HaveRightRows = 1;
else set @HaveRightRows = 0;

--Все ТП, затем будем раскидывать по уровням напряжений
	select distinct sjc.JuridicalPersonContract_ID, dj.StringName as ContractName, djc.ContractNumber as ContractNumber
	from Info_Section_To_JuridicalContract sjc
	join [dbo].[Dict_JuridicalPersons_Contracts] djc on djc.JuridicalPersonContract_ID = sjc.JuridicalPersonContract_ID
	join [dbo].[Dict_JuridicalPersons] dj on djc.JuridicalPerson_ID = dj.JuridicalPerson_ID
	join Info_Section_List sl on sl.Section_ID = sjc.Section_ID
	join Dict_HierLev2 h2 on 
		(sl.HierLev2_ID is not null and h2.HierLev2_ID = sl.HierLev2_ID) 
		or (sl.HierLev3_ID is not null and h2.HierLev2_ID = (select HierLev2_ID from Dict_HierLev3 where HierLev3_ID = sl.HierLev3_ID)) 
		or (sl.PS_ID is not null and h2.HierLev2_ID = (select HierLev2_ID from Dict_HierLev3 where HierLev3_ID = (select HierLev3_ID from Dict_PS where PS_ID = sl.PS_ID)))
	where ((@HierLev2_ID is null and (@HaveRightRows = 0 or (@HaveRightRows = 1 and h2.HierLev2_ID in (select Hier2_id from #HierIdByRight))))
	or (@HierLev2_ID is not null AND h2.HierLev2_ID = @HierLev2_ID)) 
	--Ограничение на действующие договора
	and @dtEnd >= djc.SignDate AND @dtStart <= ISNULL(djc.FinishDate, '21000101')
	--Ограничение АЭСК
	and (@PowerSupplyingIntermediary_ID is null or (@PowerSupplyingIntermediary_ID is not null and djc.PowerSupplyingIntermediary_ID = @PowerSupplyingIntermediary_ID))
	order by djc.ContractNumber, dj.StringName
go
   grant EXECUTE on usp2_GroupTP_TPArraybyPersonContract to [UserCalcService]
go
