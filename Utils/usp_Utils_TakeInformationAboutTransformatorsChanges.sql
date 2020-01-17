if exists (select 1
          from sysobjects
          where  id = object_id('usp2_Utils_TakeInformationAboutTransformatorsChanges')
          and type in ('P','PC'))
   drop procedure usp2_Utils_TakeInformationAboutTransformatorsChanges
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
--		Январь, 2010
--
-- Описание:
--
--		Возвращаем информацию о сменах трансформаторов на промежутке времени по списку точек
--		возвращаются только точки для которых была смена (или несколько смен) на указанном промежутке времени
--
-- ======================================================================================
create proc [dbo].[usp2_Utils_TakeInformationAboutTransformatorsChanges]
(	
	@TI_Array varchar(4000), --список ТИ
	@datestart datetime,
	@dateend datetime
)
AS
BEGIN 
	select it.TI_ID, it.StartDateTime, ISNULL(it.FinishDateTime, '21000101') as FinishDateTime, it.COEFU, it.COEFI, ti.TIName as TiName
	from Info_Transformators it
	join Info_TI ti on ti.TI_ID = it.TI_ID
	inner join 
	(
		select ti_id from Info_Transformators
		where 
		StartDateTime <= @dateend
		and (FinishDateTime is null or  FinishDateTime>= @DateStart)
		and ti_id in (select TInumber from usf2_Utils_iter_intlist_to_table(@TI_Array))
		group by TI_ID
		having Count(TI_ID)>1
	) sit
	on 
	StartDateTime <= @dateend
	and (FinishDateTime is null or  FinishDateTime>= @DateStart)
	and it.TI_ID = sit.ti_id
	order by it.TI_ID, it.StartDateTime, it.FinishDateTime
END

go
   grant EXECUTE on usp2_Utils_TakeInformationAboutTransformatorsChanges to [UserCalcService]
go