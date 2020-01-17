if exists (select 1
          from sysobjects
          where  id = object_id('usp2_Info_FormulaSelectFSK')
          and type in ('P','PC'))
   drop procedure usp2_Info_FormulaSelectFSK
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
--		Декабрь, 2008
--
-- Описание:
--
--		Выбираем параметры формулы, также параметры вложенных формул и уровни вложенности этих формул для ФСК
--
-- ======================================================================================

create proc [dbo].[usp2_Info_FormulaSelectFSK]
	@formulaId varchar(22),
	@DTStart DateTime = '20100101',
	@DTEnd DateTime = null,
	@ClosedPeriod_ID uniqueidentifier = null
as
begin
set nocount on
set quoted_identifier, ansi_nulls, ansi_warnings, arithabort, concat_null_yields_null, ansi_padding on
set numeric_roundabort off
set transaction isolation level read uncommitted
declare 
@sqlCommand nvarchar(4000),
@sqlTablePrefix nvarchar(100),
@sqlWherePrefix nvarchar(200),
@sqlJoinPrefix nvarchar(200),
@parmDefinition NVARCHAR(1000);
if @ClosedPeriod_ID is not null begin 

	--Закрытые данные	
	--Данные по основной формуле
	select 0 as InnerLevel,fd.Formula_UN,fd.StringNumber,fd.OperBefore,fd.UsedFormula_UN,fd.TI_ID,fd.ChannelType as ChannelType, 
	fd.TP_ID,fd.Section_ID, fd.ContrTI_ID,  fd.OperAfter, fl.FormulaName, fl.ForAutoUse as ForAutoUse, 
	cast((case when ListPU_UN is null then 0 else 1 end) as bit) as IsIntegral, 
	fl.StartDateTime, fl.FinishDateTime, fl.FormulaType_ID, @ClosedPeriod_ID as ClosedPeriod_ID,
	@formulaId as Mainformula_UN, FormulaConstant_UN, NULL as UnitDigit
	from 
	(
		select 0 as InnerLevel,Formula_UN,StringNumber,OperBefore,UsedFormula_UN,TI_ID,ChannelType,TP_ID,Section_ID,ContrTI_ID,OperAfter,ListPU_UN,
		@closedPeriod_ID as closedPeriod_ID,FormulaConstant_UN 
		from Info_TP2_OurSide_Formula_Description_Closed fl where Formula_UN=@formulaId and fl.ClosedPeriod_ID = @ClosedPeriod_ID
	) fd
	join dbo.Info_TP2_OurSide_Formula_List_Closed fl on fd.Formula_UN = fl.Formula_UN
		and ISNULL(FinishDateTime, '21000101') > @DTStart and ISNULL(@DTEnd, '21000101') >= StartDateTime and fd.ClosedPeriod_ID = fl.ClosedPeriod_ID
	union all
	--Данные по вложенным формулам
	select f.InnerLevel,t.Formula_UN,StringNumber,OperBefore,UsedFormula_UN,TI_ID,t.ChannelType as ChannelType, t.TP_ID,t.Section_ID, t.ContrTI_ID,OperAfter,t.FormulaName, 
	t.ForAutoUse as ForAutoUse, cast((case when ListPU_UN is null then 0 else 1 end) as bit) as IsIntegral,
	t.StartDateTime, t.FinishDateTime, FormulaType_ID, @ClosedPeriod_ID as ClosedPeriod_ID,@formulaId, FormulaConstant_UN, NULL as UnitDigit
	from usf2_Info_GetFormulasNeededForMainFormulaFSK(@formulaId,0, @closedPeriod_ID) f
	left join 
	(
		select fl.Formula_UN,ISNULL(StringNumber, 0) as StringNumber,OperBefore,UsedFormula_UN,TI_ID,fd.ChannelType,OperAfter,fd.TP_ID,Section_ID, ContrTI_ID,fl.FormulaName, 
		ForAutoUse, ListPU_UN, fl.StartDateTime, fl.FinishDateTime, FormulaType_ID, @ClosedPeriod_ID as ClosedPeriod_ID, FormulaConstant_UN
		from Info_TP2_OurSide_Formula_Description_Closed fd
		right join dbo.Info_TP2_OurSide_Formula_List_Closed fl	on fd.Formula_UN = fl.Formula_UN
		and fd.ClosedPeriod_ID = fl.ClosedPeriod_ID
		where fl.Formula_UN in (select formulaId from usf2_Info_GetFormulasNeededForMainFormulaFSK(@formulaId,0, @closedPeriod_ID)) and fl.ClosedPeriod_ID = @ClosedPeriod_ID
	) t
	on t.Formula_UN = f.formulaId
	order by InnerLevel, Formula_UN, StringNumber

end else begin

	--Открытые данные
	--Данные по основной формуле
	select 0 as InnerLevel,fd.Formula_UN,fd.StringNumber,fd.OperBefore,fd.UsedFormula_UN,fd.TI_ID,fd.ChannelType as ChannelType, 
	fd.TP_ID,fd.Section_ID, fd.ContrTI_ID,  fd.OperAfter, fl.FormulaName, fl.ForAutoUse as ForAutoUse, 
	cast((case when ListPU_UN is null then 0 else 1 end) as bit) as IsIntegral, 
	fl.StartDateTime, fl.FinishDateTime, fl.FormulaType_ID, @ClosedPeriod_ID as ClosedPeriod_ID,
	@formulaId as Mainformula_UN, FormulaConstant_UN, fl.UnitDigit 
	from 
	(
		select 0 as InnerLevel,Formula_UN,StringNumber,OperBefore,UsedFormula_UN,TI_ID,ChannelType,TP_ID,Section_ID,ContrTI_ID,OperAfter,ListPU_UN,
		@closedPeriod_ID as closedPeriod_ID,FormulaConstant_UN 
		from Info_TP2_OurSide_Formula_Description fl where Formula_UN=@formulaId
	) fd
	join dbo.Info_TP2_OurSide_Formula_List fl on fd.Formula_UN = fl.Formula_UN
		and ISNULL(FinishDateTime, '21000101') > @DTStart and ISNULL(@DTEnd, '21000101') >= StartDateTime
	union all
	--Данные по вложенным формулам
	select f.InnerLevel,t.Formula_UN,StringNumber,OperBefore,UsedFormula_UN,TI_ID,t.ChannelType as ChannelType, t.TP_ID,t.Section_ID, t.ContrTI_ID,OperAfter,t.FormulaName, 
	t.ForAutoUse as ForAutoUse, cast((case when ListPU_UN is null then 0 else 1 end) as bit) as IsIntegral,
	t.StartDateTime, t.FinishDateTime, FormulaType_ID, @ClosedPeriod_ID as ClosedPeriod_ID,@formulaId, FormulaConstant_UN, t.UnitDigit
	from usf2_Info_GetFormulasNeededForMainFormulaFSK(@formulaId,0, @closedPeriod_ID) f
	left join 
	(
		select fl.Formula_UN,ISNULL(StringNumber, 0) as StringNumber,OperBefore,UsedFormula_UN,TI_ID,fd.ChannelType,OperAfter,fd.TP_ID,Section_ID, ContrTI_ID,fl.FormulaName, 
		ForAutoUse, ListPU_UN, fl.StartDateTime, fl.FinishDateTime, FormulaType_ID, @ClosedPeriod_ID as ClosedPeriod_ID, FormulaConstant_UN, fl.UnitDigit
		from Info_TP2_OurSide_Formula_Description fd
		right join dbo.Info_TP2_OurSide_Formula_List fl	on fd.Formula_UN = fl.Formula_UN
		where fl.Formula_UN in (select formulaId from usf2_Info_GetFormulasNeededForMainFormulaFSK(@formulaId,0, @closedPeriod_ID)) 
	) t
	on t.Formula_UN = f.formulaId
	order by InnerLevel, Formula_UN, StringNumber

end
end
go
   grant EXECUTE on usp2_Info_FormulaSelectFSK to [UserCalcService]
go