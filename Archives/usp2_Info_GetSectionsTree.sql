if exists (select 1
          from sysobjects
          where  id = object_id('usp2_Info_GetSectionsTree')
          and type in ('P','PC'))
   drop procedure usp2_Info_GetSectionsTree
go

SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO


create proc [dbo].[usp2_Info_GetSectionsTree]
@Section_id int = null
as
begin
	set nocount on
	set quoted_identifier, ansi_nulls, ansi_warnings, arithabort, concat_null_yields_null, ansi_padding on
	set numeric_roundabort off
	set transaction isolation level read uncommitted

	--Точки поставок
	select distinct tp.TP_ID,tp.StringName,TPMode,IsMoneyOurSide,EvalModeOurSide,EvalModeContr,ISNULL(sd.Section_ID, 0) as Section_ID, ExcludeFromXMLExport
		,VoltageLevel,IsMoneyOurSideMode2,DirectConsumer_ID, TPATSCode, p.MaximumPower
		,ISNULL(sd.StartDateTime, '20010101') as StartDateTime, sd.FinishDateTime
		,ISNULL(sd.IsTransit, 0) as IsTransit, Voltage, contract.JuridicalPersonContract_ID 
    from dbo.Info_TP2 tp 
    outer apply
	(
		select top (1) * from Info_Section_Description2 
		where TP_ID = tp.TP_ID 
		order by StartDateTime desc
	) sd
    left join Info_Section_To_JuridicalContract sjc on sjc.Section_ID = sd.Section_ID  
    left join dbo.Dict_JuridicalPersons_Contracts contract on contract.JuridicalPersonContract_ID = sjc.JuridicalPersonContract_ID 
    outer apply
    (
		select top (1) * from [dbo].[Info_TP2_Power] 
		where TP_ID = tp.TP_ID
		order by StartDateTime desc
    ) p
	outer apply
    (
		select top (1) * from [dbo].[Info_TP_VoltageLevel] 
		where TP_ID = tp.TP_ID
		order by StartDateTime desc
    ) v
	where (@Section_id is null and sd.Section_ID is null) or @Section_id = 0 or (@Section_id is not null and sd.Section_ID = @Section_id)
	order by ISNULL(sd.Section_ID, 0), tp.StringName desc
		
	if (@Section_id is not null) begin
		--Сечения
		select sl.*, c.JuridicalPerson_ID, c.JuridicalPersonContract_ID 
		from Info_Section_List sl 
		left join Info_Section_To_JuridicalContract sjc on sjc.Section_ID = sl.Section_ID 
		left join Dict_JuridicalPersons_Contracts c on c.JuridicalPersonContract_ID = sjc.JuridicalPersonContract_ID
		where @Section_id = 0 or (@Section_id<>0 and sl.Section_ID = @Section_id)
		order by sl.SectionName
	end;
end
go
   grant EXECUTE on usp2_Info_GetSectionsTree to [UserCalcService]
go
