if exists (select 1
          from sysobjects
          where  id = object_id('usp2_TpRange_ReadArray')
          and type in ('P','PC'))
 drop procedure usp2_TpRange_ReadArray
go

-- ======================================================================================
-- Автор:
--
--		Малышев Игорь
--
-- Дата создания:
--
--		Ноябрь, 2014
--
-- Описание:
--
--		Читаем схемы учета для сечения
--
-- ======================================================================================
create proc [dbo].[usp2_TpRange_ReadArray]
	@Section_ID int
as
begin

	declare @dtNow DateTime;
	set @dtNow = GETDATE();

	set nocount on
	set quoted_identifier, ansi_nulls, ansi_warnings, arithabort, concat_null_yields_null, ansi_padding on
	set numeric_roundabort off
	set transaction isolation level read uncommitted

	SELECT tr.[TpRangeScheme_ID]
      ,tr.[Section_ID]
      ,[ApplyDateTime]
      ,[UserName]
	  ,tr.[User_ID]
      ,[SchemeName]
      ,[SchemeDescription]
	  ,cast(ISNULL(IsSchemeActive, 0) as bit) IsSchemeActive --Данная схема действует в текущем моменте времени
	into #tmp
	FROM [dbo].[Info_TpRangeScheme] tr
	JOIN Expl_Users u on u.[User_ID] = tr.[User_ID]
	cross apply
	(
	select Min(IsSchemeActive) as IsSchemeActive
	from
		(select case when m.IsTpActive = sd.IsActive then 
				--Здесь проверяем действует ли тарифный уровень напряжения для этой точке, который указан в схеме
				case when sd.VoltageLevel is null then 1
				else case when sd.VoltageLevel = (
					select top 1 VoltageLevel from Info_TP_VoltageLevel
					where TP_ID = sd.TP_ID 
					and StartDateTime <= GetDate() and ISNULL(FinishDateTime, '21000101') >= GetDate()
				) then 1 else 0 end end
				else 0 end as IsSchemeActive,
				sd.TP_ID
				from Info_TpRangeScheme_To_Tp sd
				cross apply 
				(
					select top 1 Section_Id, TP_ID, 
					case when StartDateTime <= GetDate() and ISNULL(FinishDateTime, '21000101') >= GetDate() then 1 else 0 end IsTpActive
					from Info_Section_Description2
					where Section_ID = @Section_ID and TP_ID = sd.TP_ID 
					order by StartDateTime desc
				) m
				where TpRangeScheme_ID = tr.TpRangeScheme_ID
		) n
	) a
	where tr.Section_ID = @Section_ID

	select * from #tmp

	select TpRangeScheme_ID, TP_ID, IsActive, VoltageLevel from [dbo].[Info_TpRangeScheme_To_Tp]
	where TpRangeScheme_ID in (select TpRangeScheme_ID from #tmp)

	drop table #tmp

end
go
   grant EXECUTE on usp2_TpRange_ReadArray to [UserCalcService]
go