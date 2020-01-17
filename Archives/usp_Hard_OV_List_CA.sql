if exists (select 1
          from sysobjects
          where  id = object_id('usp2_Hard_OV_List_CA')
          and type in ('P','PC'))
   drop procedure usp2_Hard_OV_List_CA
go

-- ======================================================================================
-- Автор:
--
--		Малышев Игорь
--
-- Дата создания:
--
--		Апрель, 2009
--
-- Описание:
--
--		Выбираем список обходных выключателей и время замещения для датчик TI_ID
--		Эта процедура для контрагентов
--
-- ======================================================================================

create proc [dbo].[usp2_Hard_OV_List_CA]
	@ContrTI_ID int = null,--Точка которую замещает обходной выключатель
	@DTStart datetime,
	@DTEnd datetime
as

declare
	@_OV_ContrTI_ID int,
	@_ContrTI_ID int,
	@_DTStart datetime,
	@_DTEnd datetime,
	@Need_OV_ContrTI_ID int,
	@Need_ContrTI_ID int,
	@NeedDTStart datetime,
	@NeedDTEnd datetime,
	@TPCoefOurSide float,
    @AIATSCode int,
	@AOATSCode int,
	@RIATSCode int,
	@ROATSCode int,
	@NTPCoefOurSide float,
    @NAIATSCode int,
	@NAOATSCode int,
	@NRIATSCode int,
	@NROATSCode int



declare t_ov cursor LOCAL STATIC FORWARD_ONLY READ_ONLY LOCAL for 
	select OV_ContrTI_ID, ArchComm_Contr_OV_Switches.ContrTI_ID, case 
				when StartDateTime < @DTStart then @DTStart 
				else StartDateTime 
	end as StartDateTime,
		   case 
				when FinishDateTime > DateAdd(n,30,@DTEnd) then DateAdd(n,30,@DTEnd)
				else FinishDateTime 
	end as FinishDateTime,
	TPCoefContr, AIATSCode,AOATSCode,RIATSCode,ROATSCode
	from dbo.ArchComm_Contr_OV_Switches WITH (NOLOCK) 

	join Info_Contr_TI on Info_Contr_TI.ContrTI_ID = ArchComm_Contr_OV_Switches.ContrTI_ID

	where OV_ContrTI_ID = @ContrTI_ID and (StartDateTime <=  @DTEnd and FinishDateTime > @DTStart )
	order by OV_ContrTI_ID, ContrTI_ID, StartDateTime,FinishDateTime	

	open t_ov;
	FETCH NEXT FROM t_ov into @_OV_ContrTI_ID,@_ContrTI_ID,@_DTStart,@_DTEnd,@TPCoefOurSide, @AIATSCode, @AOATSCode, @RIATSCode, @ROATSCode 
	--Предыдущее значение идентификатора ОВ, окончание времени его замещения
	set @Need_OV_ContrTI_ID = @_OV_ContrTI_ID
	set	@Need_ContrTI_ID = @_ContrTI_ID
	set @NeedDTStart = @_DTStart
	set @NeedDTEnd = @_DTEnd
	select @NTPCoefOurSide = @TPCoefOurSide, @NAIATSCode=@AIATSCode, @NAOATSCode=@AOATSCode, @NRIATSCode=@RIATSCode, @NROATSCode=@ROATSCode

	WHILE @@FETCH_STATUS = 0
	BEGIN
	if ((@Need_OV_ContrTI_ID <> @_OV_ContrTI_ID and  @Need_ContrTI_ID <> @_ContrTI_ID)or(@_DTStart>@NeedDTEnd)) begin --следующий обходной или существует провал времение на том же ОВ с предыдущим значением
		select @Need_OV_ContrTI_ID,	@Need_ContrTI_ID,@NeedDTStart,@NeedDTEnd,@NTPCoefOurSide, @NAIATSCode, @NAOATSCode, @NRIATSCode, @NROATSCode, cast(0 as bit)
		set @Need_OV_ContrTI_ID = @_OV_ContrTI_ID
		set	@Need_ContrTI_ID = @_ContrTI_ID
		set @NeedDTStart = @_DTStart
		set @NeedDTEnd = @_DTEnd
		select @NTPCoefOurSide = @TPCoefOurSide, @NAIATSCode=@AIATSCode, @NAOATSCode=@AOATSCode, @NRIATSCode=@RIATSCode, @NROATSCode=@ROATSCode

	end else  begin -- Тот же ОВ и время действия является продолжением времени с предыдущей записи
		set @NeedDTEnd = @_DTEnd
	end

	FETCH NEXT FROM t_ov into @_OV_ContrTI_ID,@_ContrTI_ID,@_DTStart,@_DTEnd,@TPCoefOurSide, @AIATSCode, @AOATSCode, @RIATSCode, @ROATSCode 
	END;
	CLOSE t_ov
	DEALLOCATE t_ov
	--Последний оставшийся
	select @Need_OV_ContrTI_ID,	@Need_ContrTI_ID,@NeedDTStart,@NeedDTEnd,@NTPCoefOurSide, @NAIATSCode, @NAOATSCode, @NRIATSCode, @NROATSCode, cast(0 as bit)
	
go
   grant EXECUTE on usp2_Hard_OV_List_CA to [UserCalcService]
go