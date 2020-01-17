if exists (select 1
          from sysobjects
          where  id = object_id('usp2_Info_FormulaSelectContr')
          and type in ('P','PC'))
   drop procedure usp2_Info_FormulaSelectContr
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
--		Декабрь, 2008
--
-- Описание:
--
--		Выбираем параметры формулы, также параметры вложенных формул и уровни вложенности этих формул для контрагентов
--
-- ======================================================================================
create proc [dbo].[usp2_Info_FormulaSelectContr]
	@formulaId varchar(22),
	@DTStart DateTime = '20060101',
	@DTEnd DateTime = null
as
begin

set nocount on
set quoted_identifier, ansi_nulls, ansi_warnings, arithabort, concat_null_yields_null, ansi_padding on
set numeric_roundabort off
set transaction isolation level read uncommitted

--Берем параметры самой формулы
select  0 as InnerLevel,fd.Formula_UN,fd.StringNumber,fd.OperBefore,fd.UsedFormula_UN,fd.TI_ID,fd.ChannelType as ChannelType, fd.TP_ID, fd.Section_ID, fd.ContrTI_ID,  fd.OperAfter, fl.FormulaName, fl.ForAutoUse as ForAutoUse
, cast((case when ListPU_UN is null then 0 else 1 end) as bit) as IsIntegral
, fl.StartDateTime, fl.FinishDateTime, fl.FormulaType_ID, cast(null as uniqueidentifier) as ClosedPeriod_ID,@formulaId as MainFormula_UN, fd.FormulaConstant_UN, fl.UnitDigit 
from 
(
	select 0 as InnerLevel,Formula_UN,StringNumber,OperBefore,UsedFormula_UN,TI_ID,ChannelType, TP_ID,Section_ID, ContrTI_ID,OperAfter, ListPU_UN, FormulaConstant_UN from 
	Info_TP2_Contr_Formula_Description where Formula_UN=@formulaId
) fd
join dbo.Info_TP2_Contr_Formula_List fl on fd.Formula_UN = fl.Formula_UN 
and (FinishDateTime is null or (FinishDateTime is not null and FinishDateTime >= @DTStart))
and (@DTEnd is null or (@DTEnd is not null and @DTEnd >= StartDateTime))
union
--И параметры вложенной формулы
select f.InnerLevel,t.Formula_UN,StringNumber,OperBefore,UsedFormula_UN,TI_ID,t.ChannelType as ChannelType, t.TP_ID,t.Section_ID, t.ContrTI_ID,OperAfter,t.FormulaName, t.ForAutoUse as ForAutoUse
, cast((case when ListPU_UN is null then 0 else 1 end) as bit) as IsIntegral 
, t.StartDateTime, t.FinishDateTime, t.FormulaType_ID, cast(null as uniqueidentifier) as ClosedPeriod_ID,@formulaId,FormulaConstant_UN, t.UnitDigit
from 
usf2_Info_GetFormulasNeededForMainFormulaContr(@formulaId,0) f
left join 
(
	select fl.Formula_UN,ISNULL(StringNumber, 0) as StringNumber,OperBefore,UsedFormula_UN,TI_ID,fd.ChannelType,OperAfter,fd.TP_ID,Section_ID, ContrTI_ID,fl.FormulaName, ForAutoUse, ListPU_UN 
	, StartDateTime, FinishDateTime, fl.FormulaType_ID,FormulaConstant_UN,UANode_ID, fl.UnitDigit
	from 
	Info_TP2_Contr_Formula_Description fd
	right join dbo.Info_TP2_Contr_Formula_List fl on fd.Formula_UN = fl.Formula_UN 
		and (FinishDateTime is null or (FinishDateTime is not null and FinishDateTime >= @DTStart))
		and (@DTEnd is null or (@DTEnd is not null and @DTEnd >= StartDateTime))
	where fl.Formula_UN in (select formulaId from usf2_Info_GetFormulasNeededForMainFormulaContr(@formulaId,0))
) t
on t.Formula_UN = f.formulaId
order by InnerLevel, Formula_UN, StringNumber
end
go
   grant EXECUTE on usp2_Info_FormulaSelectContr to [UserCalcService]
go