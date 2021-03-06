if exists (select 1
          from sysobjects
          where  id = object_id('usp2_Info_FormulasSelectByFormulaType')
          and type in ('P','PC'))
   drop procedure usp2_Info_FormulasSelectByFormulaType
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
--		Сентябрь, 2009
--
-- Описание:
--
--		Выбираем параметры формул, также параметры вложенных формул и уровни вложенности этих формул
--		Функция оптимизирована работать с группой формул
--
-- ======================================================================================

create proc [dbo].[usp2_Info_FormulasSelectByFormulaType]
	@FormulaIDs varchar(max)
as
begin
declare 
@FormulaID varchar(22),
@FormulaType int

	declare t cursor LOCAL STATIC FORWARD_ONLY READ_ONLY for select STRnumber, CHnumber from usf2_Utils_iter_strintlist_to_table(@FormulaIDs)
	open t;
	FETCH NEXT FROM t into @FormulaID,@FormulaType

	WHILE @@FETCH_STATUS = 0
	BEGIN
		select top 1 Formula_UN, @FormulaType as FormulaType, FormulaType_ID, HighLimit, LowerLimit, MeasureUnit_UN
		from Info_Formula_List where Formula_UN = @FormulaID
		if (exists(select * from Info_Formula_List where Formula_UN = @FormulaID)) begin
			exec dbo.usp2_Info_FormulaSelect @FormulaID
		end
	FETCH NEXT FROM t into @FormulaID,@FormulaType
	END;

	CLOSE t
	DEALLOCATE t
end
go
   grant EXECUTE on usp2_Info_FormulasSelectByFormulaType to [UserCalcService]
go