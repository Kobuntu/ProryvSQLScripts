if exists (select 1
          from sysobjects
          where  id = object_id('usp2_Hard_TI_List_CA')
          and type in ('P','PC'))
   drop procedure usp2_Hard_TI_List_CA
go


-- ======================================================================================
-- Автор:
--
--		Малышев Игорь
--
-- Дата создания:
--
--		Сентябрь, 2008
--
-- Описание:
--
--		Выбираем время действия обходного выключателя
--
-- ======================================================================================


create proc [dbo].[usp2_Hard_TI_List_CA]
	@ContrTI_ID int = null,--Идентификатор ОВ
	@DTStart datetime,
	@DTEnd datetime
as
--Выбираем 
--1)Идентификатор самого ОВ
--2)Позиция ОВ
--3)Идентификато точки которую  которую он замещает
--4)Дата начала замещени
--5)Дата конца замещения
declare
	@_OV_ContrTI_ID int,
	@_ContrTI_ID int,
	@_DTStart datetime,
	@_DTEnd datetime,
	@Need_OV_ContrTI_ID int,
	@Need_ContrTI_ID int,
	@NeedDTStart datetime,
	@NeedDTEnd datetime

create table #TmpResult(Need_OV_ContrTI_ID int,Position int, Need_ContrTI_ID int, NeedDTStart DateTime, NeedDTEnd DateTime);

declare t_ov cursor LOCAL STATIC FORWARD_ONLY READ_ONLY LOCAL for 
	select ContrTI_ID,OV_ContrTI_ID, case 
				when StartDateTime < @DTStart then @DTStart 
				else StartDateTime 
	 end as StartDateTime,
		   case 
				when FinishDateTime > DateAdd(n,30,@DTEnd) then DateAdd(n,30,@DTEnd)
				else FinishDateTime 
	 end as FinishDateTime from dbo.ArchComm_Contr_OV_Switches WITH (NOLOCK)
where ContrTI_ID = @ContrTI_ID and (StartDateTime <=  @DTEnd and FinishDateTime > @DTStart )
order by ContrTI_ID,OV_ContrTI_ID, StartDateTime,FinishDateTime	

	open t_ov;
	FETCH NEXT FROM t_ov into @_ContrTI_ID,@_OV_ContrTI_ID,@_DTStart,@_DTEnd
	--Предыдущее значение идентификатора ОВ, окончание времени его замещения
	set @Need_OV_ContrTI_ID = @_OV_ContrTI_ID
	set	@Need_ContrTI_ID = @_ContrTI_ID
	set @NeedDTStart = @_DTStart
	set @NeedDTEnd = @_DTEnd
	WHILE @@FETCH_STATUS = 0
	BEGIN
	--следующий обходной или существует провал времение на том же ОВ с предыдущим значением
	if ((@Need_OV_ContrTI_ID <> @_OV_ContrTI_ID)or(@_DTStart>@NeedDTEnd)) begin 
		insert into #TmpResult (Need_OV_ContrTI_ID,Position, Need_ContrTI_ID, NeedDTStart, NeedDTEnd)
		select @Need_OV_ContrTI_ID,0,	@Need_ContrTI_ID,@NeedDTStart,@NeedDTEnd
		set @Need_OV_ContrTI_ID = @_OV_ContrTI_ID
		set	@Need_ContrTI_ID = @_ContrTI_ID
		set @NeedDTStart = @_DTStart
		set @NeedDTEnd = @_DTEnd
	end else  begin -- Тот же ОВ и время действия является продолжением времени с предыдущей записи
		set @NeedDTEnd = @_DTEnd
	end
	FETCH NEXT FROM t_ov into @_ContrTI_ID,@_OV_ContrTI_ID,@_DTStart,@_DTEnd
	END;
	CLOSE t_ov
	DEALLOCATE t_ov
	--Последний оставшийся
	insert into #TmpResult (Need_OV_ContrTI_ID,Position, Need_ContrTI_ID, NeedDTStart, NeedDTEnd)
	select @Need_OV_ContrTI_ID,0,@Need_ContrTI_ID,@NeedDTStart,@NeedDTEnd

	select Need_ContrTI_ID,Position,Need_OV_ContrTI_ID, NeedDTStart, NeedDTEnd from #TmpResult

drop table #TmpResult

go
   grant EXECUTE on usp2_Hard_TI_List_CA to [UserCalcService]
go
