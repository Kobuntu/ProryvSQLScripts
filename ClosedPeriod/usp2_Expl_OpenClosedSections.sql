--Процедура устарела и не используется
if exists (select 1
          from sysobjects
          where  id = object_id('usp2_Expl_OpenClosedSections')
          and type in ('P','PC'))
 drop procedure usp2_Expl_OpenClosedSections
go

-- ======================================================================================
-- Автор:
--
--		Малышев Игорь
--
-- Дата создания:
--
--		Май, 2013
--
-- Описание:
--
--		Читаем закрытия на отчетный период по списку сечений
--
-- ======================================================================================
create proc [dbo].[usp2_Expl_OpenClosedSections]
(	
	@User_ID varchar(22),
	@ClosedIds varchar(max) --Идентификаторы закрытий, разделенные запятой
)
AS
BEGIN 
	set nocount on
	set quoted_identifier, ansi_nulls, ansi_warnings, arithabort, concat_null_yields_null, ansi_padding on
	set numeric_roundabort off
	set transaction isolation level read uncommitted

	select distinct Item into #array from dbo.usf2_Utils_SplitString(@ClosedIds, ',')
	
	BEGIN TRY  BEGIN TRANSACTION
		
		declare @dt DateTime;
		set @dt = Dateadd(MILLISECOND, -2600, GETDATE());

		--Запись в журнал об отмене закрытия периода
		insert into Expl_User_Journal (ApplicationType, CommentString, CUS_ID, EventDateTime, EventString, User_ID, ObjectID, ObjectName)
		select 1, sl.SectionName, 0, DateAdd(MILLISECOND, cs.Section_ID * 4, @dt),
		'Отмена закрытия ' +  DATENAME(month, DATEADD(month, cl.[Month]-1, CAST('2014-01-01' AS datetime))) + ' ' + ltrim(str([Year], 4)) + 'г.',
		@User_ID, ltrim(str(cs.Section_ID,5)), 'Expl_ClosedPeriod_List'
		from Expl_ClosedPeriod_List cl
		join Expl_ClosedPeriod_To_Section cs on cs.ClosedPeriod_ID = cl.ClosedPeriod_ID
		join Info_Section_List sl on sl.Section_ID = cs.Section_ID
		where cl.ClosedPeriod_ID in (select Item from #array)
		
		delete from Expl_DataSource_PriorityList_Closed
		where ClosedPeriod_ID in (select Item from #array)
	
		delete from Expl_ClosedPeriod_Current_DeltaTP
		where ClosedPeriod_ID in (select Item from #array)

		delete from [dbo].[Expl_ClosedPeriod_List]
		where ClosedPeriod_ID in (select Item from #array)

	COMMIT	
	END TRY
	BEGIN CATCH
		--Ошибка, откатываем все изменения
		IF @@TRANCOUNT > 0 ROLLBACK 

		DECLARE @ErrMsg nvarchar(4000), @ErrSeverity int
		set @ErrMsg = ERROR_MESSAGE()
		set @ErrSeverity = 16 
		SELECT @ErrMsg 
		RAISERROR(@ErrMsg, @ErrSeverity, 1)
	END CATCH

	drop table #array
END
go
   grant EXECUTE on usp2_Expl_OpenClosedSections to [UserCalcService]
go