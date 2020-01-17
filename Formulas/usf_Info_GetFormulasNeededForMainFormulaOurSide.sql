if exists (select 1
          from sysobjects
          where  id = object_id('usf2_Info_GetFormulasNeededForMainFormulaOurSide')
          and type in ('IF', 'FN', 'TF'))
   drop function usf2_Info_GetFormulasNeededForMainFormulaOurSide
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
--		Декабрь, 2009
--
-- Описание:
--
--		Создает список идентификаторов формул которые необходимы для расчета указанной формулы для ТП КА
--
-- ======================================================================================

create FUNCTION [dbo].[usf2_Info_GetFormulasNeededForMainFormulaOurSide]
(	
	-- Add the parameters for the function here
	@formulaId varchar(22),
	@Innerparam int,
	@ClosedPeriod_ID uniqueidentifier = null --Закрытый период (если null читаем из открытого периода) 
)
RETURNS @ret TABLE
(
	formulaId varchar(22),
	MainTP_ID [dbo].[TP_ID_TYPE],
	MainChannelType [dbo].[TI_CHANNEL_TYPE],
	InnerLevel int,
	ForAutoUse tinyint,
    FormulaType_ID tinyint,
	StringNumber int,
	OperBefore varchar(255),
	UsedFormula_UN [dbo].[ABS_NUMBER_TYPE_2],
	TI_ID [dbo].[TI_ID_TYPE],
	ContrTI_ID [dbo].[CONTRTI_ID_TYPE],
	Section_ID [dbo].[SECTION_ID_TYPE],
	ChannelType [dbo].[TI_CHANNEL_TYPE],
	OperAfter varchar(255),
	FormulaName varchar(255)
)
AS
begin
	--Уровень вложенности
	set @Innerparam = @Innerparam +1;

	if (@ClosedPeriod_ID is null) begin 
		with FormulasParams (formulaId, MainTP_ID, MainChannelType, Innerparam, ForAutoUse, FormulaType_ID, StringNumber, OperBefore, UsedFormula_UN, TI_ID, ContrTI_ID, Section_ID, ChannelType, OperAfter, FormulaName) AS 
		(
			select distinct uifd.UsedFormula_UN as formulaId, uifl.TP_ID, uifl.ChannelType, @Innerparam, ForAutoUse, FormulaType_ID,StringNumber,OperBefore,uifd2.UsedFormula_UN,uifd2.TI_ID,ContrTI_ID,Section_ID, uifd2.ChannelType,OperAfter,FormulaName 
			from	
				(
					select UsedFormula_UN from dbo.Info_TP2_OurSide_Formula_Description 
					where Formula_UN = @formulaId and not UsedFormula_UN is null
				) uifd
			inner join 
				dbo.Info_TP2_OurSide_Formula_List uifl on  uifl.Formula_UN = uifd.UsedFormula_UN
			left join 
				dbo.Info_TP2_OurSide_Formula_Description uifd2 on uifl.Formula_UN = uifd2.Formula_UN

			-----Рекурсия
			union all
			select FormulasParams.UsedFormula_UN as formulaId,uifl.TP_ID, uifl.ChannelType, @Innerparam + 1,uifl.ForAutoUse, uifl.FormulaType_ID,uifd2.StringNumber,uifd2.OperBefore,uifd2.UsedFormula_UN,uifd2.TI_ID,uifd2.ContrTI_ID,uifd2.Section_ID, uifd2.ChannelType,uifd2.OperAfter,uifl.FormulaName 
			from FormulasParams
			inner join
				dbo.Info_TP2_OurSide_Formula_List uifl on  uifl.Formula_UN = FormulasParams.UsedFormula_UN
			inner join 
				dbo.Info_TP2_OurSide_Formula_Description uifd2 on uifl.Formula_UN = uifd2.Formula_UN
		)
		--Результат
		insert into @ret
		select * from FormulasParams o
	end else begin
		with FormulasParams (formulaId, MainTP_ID, MainChannelType, Innerparam, ForAutoUse, FormulaType_ID, StringNumber, OperBefore, UsedFormula_UN, TI_ID, ContrTI_ID, Section_ID, ChannelType, OperAfter, FormulaName) AS 
		(
			select distinct uifd.UsedFormula_UN as formulaId, uifl.TP_ID, uifl.ChannelType, @Innerparam, ForAutoUse, FormulaType_ID,StringNumber,OperBefore,uifd2.UsedFormula_UN,uifd2.TI_ID,ContrTI_ID,Section_ID, uifd2.ChannelType,OperAfter,FormulaName 
			from	
				(
					select UsedFormula_UN from dbo.Info_TP2_OurSide_Formula_Description_Closed 
					where Formula_UN = @formulaId and not UsedFormula_UN is null
				) uifd
			inner join 
				dbo.Info_TP2_OurSide_Formula_List_Closed uifl on  uifl.Formula_UN = uifd.UsedFormula_UN
			left join 
				dbo.Info_TP2_OurSide_Formula_Description_Closed uifd2 on uifl.Formula_UN = uifd2.Formula_UN

			-----Рекурсия
			union all
			select FormulasParams.UsedFormula_UN as formulaId,uifl.TP_ID, uifl.ChannelType, @Innerparam + 1,uifl.ForAutoUse, uifl.FormulaType_ID,uifd2.StringNumber,uifd2.OperBefore,uifd2.UsedFormula_UN,uifd2.TI_ID,uifd2.ContrTI_ID,uifd2.Section_ID, uifd2.ChannelType,uifd2.OperAfter,uifl.FormulaName 
			from FormulasParams
			inner join
				dbo.Info_TP2_OurSide_Formula_List_Closed uifl on  uifl.Formula_UN = FormulasParams.UsedFormula_UN
			inner join 
				dbo.Info_TP2_OurSide_Formula_Description_Closed uifd2 on uifl.Formula_UN = uifd2.Formula_UN
		)
		--Результат
		insert into @ret
		select * from FormulasParams o
	end;
	return
end
go
   grant select on usf2_Info_GetFormulasNeededForMainFormulaFSK to [UserCalcService]
go