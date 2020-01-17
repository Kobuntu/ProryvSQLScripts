if exists (select 1
          from sysobjects
          where  id = object_id('usf2_Info_GetFormulasNeededForMainFormulaContr')
          and type in ('IF', 'FN', 'TF'))
   drop function usf2_Info_GetFormulasNeededForMainFormulaContr
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
--		Январь, 2009
--
-- Описание:
--
--		Создает список идентификаторов формул которые необходимы для расчета указанной формулы для ТП КА
--
-- ======================================================================================

create FUNCTION [dbo].[usf2_Info_GetFormulasNeededForMainFormulaContr]
(	
	-- Add the parameters for the function here
	@formulaId varchar(22),
	@Innerparam int
	
)
RETURNS @ret TABLE
(
	formulaId varchar(22),
	InnerLevel int
)
AS
begin
	--Уровень вложенности
	set @Innerparam = @Innerparam +1
	insert into @ret
		select distinct Info_TP2_Contr_Formula_Description.UsedFormula_UN, @Innerparam
		from	dbo.Info_TP2_Contr_Formula_Description
		join Info_TP2_Contr_Formula_Description uifd on  uifd.Formula_UN = Info_TP2_Contr_Formula_Description.UsedFormula_UN
		join Info_TP2_Contr_Formula_List uifl on  uifl.Formula_UN = Info_TP2_Contr_Formula_Description.UsedFormula_UN
		where	Info_TP2_Contr_Formula_Description.Formula_UN = @formulaId 
		and not Info_TP2_Contr_Formula_Description.UsedFormula_UN is null
			
	declare innerFormulasCursor cursor LOCAL STATIC FORWARD_ONLY READ_ONLY for	select formulaId,InnerLevel	from @ret order by InnerLevel
	declare @innerFormulaId varchar(22)
	set @innerFormulaId = null
	open innerFormulasCursor
	fetch next from innerFormulasCursor into @innerFormulaId, @Innerparam
	WHILE @@fetch_status = 0
	BEGIN
		if (@innerFormulaId is null)
		begin
			close innerFormulasCursor
			deallocate innerFormulasCursor
			return
		end

		insert into @ret 
			select distinct * from usf2_Info_GetFormulasNeededForMainFormulaContr(@innerFormulaId, @Innerparam)
	fetch next from innerFormulasCursor into @innerFormulaId,@Innerparam
	END
	close innerFormulasCursor
	deallocate innerFormulasCursor
	return
end
go
   grant select on usf2_Info_GetFormulasNeededForMainFormulaContr to [UserCalcService]
go