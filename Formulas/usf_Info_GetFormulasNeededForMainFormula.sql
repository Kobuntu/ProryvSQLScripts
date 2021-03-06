if exists (select 1
          from sysobjects
          where  id = object_id('usf2_Info_GetFormulasNeededForMainFormula')
          and type in ('IF', 'FN', 'TF'))
   drop function usf2_Info_GetFormulasNeededForMainFormula
go

/****** Object:  UserDefinedFunction [dbo].[usf2_Info_GetFormulasNeededForMainFormula]    Script Date: 09/25/2008 12:50:13 ******/
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
--		Сентябрь, 2008
--
-- Описание:
--
--		Создает список идентификаторов формул которые необходимы для расчета указанной формулы
--
-- ======================================================================================

create FUNCTION [dbo].[usf2_Info_GetFormulasNeededForMainFormula]
(	
	-- Add the parameters for the function here
	@formulaId varchar(22),
	@Innerparam int
	
)
RETURNS @ret TABLE
(
	formulaId varchar(22),
	InnerLevel int,
    FormulaType_ID tinyint,
	StringNumber int,
	OperBefore varchar(255),
	UsedFormula_UN [dbo].[ABS_NUMBER_TYPE_2],
	TI_ID [dbo].[TI_ID_TYPE],
	ContrTI_ID [dbo].[CONTRTI_ID_TYPE],
	ChannelType [dbo].[TI_CHANNEL_TYPE],
	OperAfter varchar(255),
	FormulaName varchar(255), 
	Section_ID [dbo].[SECTION_ID_TYPE],
	TP_ID [dbo].[TP_ID_TYPE],
	IsIntegral bit, 
	FormulaClassification_ID tinyint,
	PS_ID int,
	HierLev3_ID int,
	HierLev2_ID int,
	HierLev1_ID int,
	FormulaConstant_UN varchar(22),
	Formula_TP_OurSide_UN varchar(22),
    UANode_ID bigint

)
AS
begin
	--Уровень вложенности
	set @Innerparam = @Innerparam +1;

	with FormulasParams (formulaId, Innerparam, FormulaType_ID, StringNumber, OperBefore, UsedFormula_UN, TI_ID, ContrTI_ID, ChannelType, 
	OperAfter, FormulaName, Section_ID, TP_ID, IsIntegral, FormulaClassification_ID, PS_ID,HierLev3_ID,HierLev2_ID, HierLev1_ID, FormulaConstant_UN,
	Formula_TP_OurSide_UN,UANode_ID) AS 
	(
		select distinct uifd.UsedFormula_UN as formulaId, @Innerparam, FormulaType_ID,StringNumber,OperBefore,uifd2.UsedFormula_UN
		, uifd2.TI_ID,ContrTI_ID,ChannelType,OperAfter,FormulaName, uifd2.Section_ID, uifd2.TP_ID, (case when uifd2.ListPU_UN is null then 0 else 1 end) as IsIntegral 
		, uifl.FormulaClassification_ID, uifl.PS_ID,uifl.HierLev3_ID,uifl.HierLev2_ID, uifl.HierLev1_ID, uifd2.FormulaConstant_UN, uifd2.Formula_TP_OurSide_UN,uifd2.UANode_ID
		from	
			(
				select UsedFormula_UN from Info_Formula_Description 
				where Formula_UN = @formulaId and not UsedFormula_UN is null
			) uifd
		inner join 
			Info_Formula_List uifl on  uifl.Formula_UN = uifd.UsedFormula_UN
		left join 
			Info_Formula_Description uifd2 on uifl.Formula_UN = uifd2.Formula_UN

		-----Рекурсия
		union all
		select FormulasParams.UsedFormula_UN as formulaId, @Innerparam + 1, uifl.FormulaType_ID,uifd2.StringNumber,uifd2.OperBefore,uifd2.UsedFormula_UN
		, uifd2.TI_ID,uifd2.ContrTI_ID,uifd2.ChannelType,uifd2.OperAfter,uifl.FormulaName,uifd2.Section_ID, uifd2.TP_ID, (case when uifd2.ListPU_UN is null then 0 else 1 end) as IsIntegral 
		, uifl.FormulaClassification_ID, uifl.PS_ID,uifl.HierLev3_ID,uifl.HierLev2_ID, uifl.HierLev1_ID, uifd2.FormulaConstant_UN, uifd2.Formula_TP_OurSide_UN,uifd2.UANode_ID
		from FormulasParams
		inner join
			Info_Formula_List uifl on  uifl.Formula_UN = FormulasParams.UsedFormula_UN
		inner join 
			Info_Formula_Description uifd2 on uifl.Formula_UN = uifd2.Formula_UN
	)

	--Результат
	insert into @ret
	select * from FormulasParams o;

	--Формулы ТП
	with FormulasTpParams (formulaId, Innerparam, FormulaType_ID, StringNumber, OperBefore, UsedFormula_UN, TI_ID, ContrTI_ID, ChannelType, 
	OperAfter, FormulaName, Section_ID, TP_ID, IsIntegral, FormulaClassification_ID, PS_ID,HierLev3_ID,HierLev2_ID, HierLev1_ID, FormulaConstant_UN,
	Formula_TP_OurSide_UN,UANode_ID) AS 
	(
		select distinct uifd.Formula_TP_OurSide_UN as formulaId, @Innerparam, FormulaType_ID,StringNumber,OperBefore,uifd2.UsedFormula_UN
		, uifd2.TI_ID,ContrTI_ID,uifl.ChannelType, OperAfter, FormulaName, uifd2.Section_ID, uifd2.TP_ID, (case when uifd2.ListPU_UN is null then 0 else 1 end) as IsIntegral 
		, cast(null as tinyint) as FormulaClassification_ID, cast(null as int) as PS_ID, cast(null as int) as HierLev3_ID, cast(null as int) as HierLev2_ID, 
		cast(null as tinyint) as HierLev1_ID, uifd2.FormulaConstant_UN, uifd2.UsedFormula_UN as Formula_TP_OurSide_UN,
		uifd2.UANode_ID AS UANode_ID
		from	
			(
				select Formula_TP_OurSide_UN from Info_Formula_Description 
				where Formula_UN = @formulaId and not Formula_TP_OurSide_UN is null
			) uifd
		inner join 
			Info_TP2_OurSide_Formula_List uifl on  uifl.Formula_UN = uifd.Formula_TP_OurSide_UN
		left join 
			Info_TP2_OurSide_Formula_Description uifd2 on uifl.Formula_UN = uifd2.Formula_UN

		-----Рекурсия
		union all
		select FormulasTpParams.UsedFormula_UN as formulaId, @Innerparam + 1, uifl.FormulaType_ID,uifd2.StringNumber,uifd2.OperBefore,uifd2.UsedFormula_UN
		, uifd2.TI_ID,uifd2.ContrTI_ID,uifd2.ChannelType,uifd2.OperAfter,uifl.FormulaName,uifd2.Section_ID, uifd2.TP_ID, (case when uifd2.ListPU_UN is null then 0 else 1 end) as IsIntegral 
		, cast(null as tinyint) as FormulaClassification_ID, cast(null as int) as PS_ID, cast(null as int) as HierLev3_ID, cast(null as int) as HierLev2_ID, 
		cast(null as tinyint) as HierLev1_ID, uifd2.FormulaConstant_UN, uifd2.UsedFormula_UN as Formula_TP_OurSide_UN
		,uifd2.UANode_ID AS UANode_ID
		from FormulasTpParams
		inner join
			Info_TP2_OurSide_Formula_List uifl on  uifl.Formula_UN = FormulasTpParams.UsedFormula_UN
		inner join 
			Info_TP2_OurSide_Formula_Description uifd2 on uifl.Formula_UN = uifd2.Formula_UN
	)

	insert into @ret
	select * from FormulasTpParams o;

	return
end
go
   grant select on usf2_Info_GetFormulasNeededForMainFormula to [UserCalcService]
go