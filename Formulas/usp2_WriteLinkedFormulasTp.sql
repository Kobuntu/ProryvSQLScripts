if exists (select 1
          from sysobjects
          where  id = object_id('usp2_WriteLinkedFormulasTp')
          and type in ('P','PC'))
   drop procedure usp2_WriteLinkedFormulasTp
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
--		Март, 2015
--
-- Описание:
--
--		Обновление таблиц Info_TP_LinkedFormulas_List и Info_TP_LinkedFormulas_OurSide_Description
-- ======================================================================================
create proc [dbo].[usp2_WriteLinkedFormulasTp]
	@tp_id int, --ТП
	@channelType tinyint, --Канал
	@dtStart DateTime,
	@dtOldStart DateTime = null,
	@dtEnd DateTime = null,
	@LinkedFormula_UN varchar(22),
	@LinkType tinyint,
	@ApplyDateTime DateTime,
	@User_ID varchar(22),
	@formulasTp varchar(max)
as
begin

set nocount on
set quoted_identifier, ansi_nulls, ansi_warnings, arithabort, concat_null_yields_null, ansi_padding on
set numeric_roundabort off

BEGIN TRY  BEGIN TRANSACTION

--Удаление старой записи
if (@dtOldStart is not null) begin
	delete from Info_TP_LinkedFormulas_List where TP_ID = @tp_id and StartDateTime = @dtOldStart;
end;

--Поиск пересечения дат с новой записью
if (exists(select top 1 1 from Info_TP_LinkedFormulas_List where TP_ID = @tp_id and ChannelType = @channelType
	and StartDateTime <= ISNULL(@dtEnd, '21000101') and ISNULL(FinishDateTime,'21000101') >= @dtStart)) begin
	RAISERROR('Пересечение диапазона времени с существующей записью!', 16, 1);
end

--Добавляем новые записи
insert into Info_TP_LinkedFormulas_List  ([TP_ID], ChannelType
           ,[StartDateTime]
           ,[FinishDateTime]
           ,[LinkedFormula_UN]
           ,[LinkType]
           ,[ApplyDateTime]
           ,[User_ID])
values (@tp_id,@channelType,@dtStart, @dtEnd, @LinkedFormula_UN, @LinkType, @ApplyDateTime, @User_ID)

if (@formulasTp is not null and LEN(@formulasTp) > 0) begin
	insert into Info_TP_LinkedFormulas_OurSide_Description (LinkedFormula_UN, Formula_UN)
	select @LinkedFormula_UN, Item
	from dbo.usf2_Utils_SplitString(@formulasTp, ';')
end;

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
end
go
   grant EXECUTE on usp2_WriteLinkedFormulasTp to [UserCalcService]
go