if exists (select 1
          from sysobjects
          where  id = object_id('usp2_Hard_TI_List')
          and type in ('P','PC'))
   drop procedure usp2_Hard_TI_List
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


create proc [dbo].[usp2_Hard_TI_List]
	@TI_ID int = null,
	@DTStart datetime,
	@DTEnd datetime
as
--Выбираем 
--1)Идентификатор самого ОВ
--2)Позиция ОВ
--3)Идентификато точки которую  которую он замещает
select ovl.OV_ID,aw.OVPosition_ID,hopl.TI_ID,aw.StartDateTime,aw.FinishDateTime  from 
Hard_OV_List ovl WITH (NOLOCK) 
left join ArchComm_OV_Switches aw
on ovl.OV_ID = aw.OV_ID and @DTStart <= FinishDateTime and @DTEnd >= StartDateTime
left join dbo.Hard_OV_Positions_List hopl
on hopl.OV_ID = ovl.OV_ID and aw.OVPosition_ID = hopl.OVPosition_ID
where ovl.TI_ID = @TI_ID

go
   grant EXECUTE on usp2_Hard_TI_List to [UserCalcService]
go
