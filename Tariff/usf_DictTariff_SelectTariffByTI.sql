if exists (select 1
          from sysobjects
          where  id = object_id('usf2_DictTariff_SelectTariffByTI')
          and type in ('IF', 'FN', 'TF'))
   drop function usf2_DictTariff_SelectTariffByTI
go

-- ======================================================================================
-- Автор:
--
--		Малышев Игорь
--
-- Дата создания:
--
--		Апрель 2011
--
-- Описание:
--
--		Выбираем идентификатор тарифа в зависимости от типа точки
--
-- ======================================================================================

CREATE FUNCTION [dbo].[usf2_DictTariff_SelectTariffByTI]
(
	@ID varchar(22),
	@DateStart datetime,
	@DateEnd datetime,
	@TypeHierarchy int -- 1 - Эта точка является контрагентом, 2 - Эта точка является формулой
)
returns @tbl TABLE 
(
	ID varchar(22), 
	TypeHierarchy int, 
	Tariff_id int, 
	StartDateTime DateTime, 
	FinishDateTime DateTime null
)
as
BEGIN

	if (@TypeHierarchy = 11) begin --Это формула
	
		insert into @tbl (ID, TypeHierarchy, Tariff_ID, StartDateTime, FinishDateTime)  
				select @ID, @TypeHierarchy, Tariff_ID, StartDateTime, FinishDateTime from dbo.DictTariffs_ToFormula
				where Formula_UN = @ID 
				and (StartDateTime <= @DateEnd and (FinishDateTime is null OR @DateStart <= FinishDateTime));
	
	end else begin
	
		declare @TI_ID int;
		
		if (ISNUMERIC(@ID)=1)
			set @TI_ID = cast(@ID as int);
		
		if (@TypeHierarchy = 4) begin --Обычная точка
			insert into @tbl (ID, TypeHierarchy, Tariff_ID, StartDateTime, FinishDateTime)  
			select @ID, @TypeHierarchy, Tariff_ID, StartDateTime, FinishDateTime from dbo.DictTariffs_ToTI
			where TI_ID = @TI_ID 
			and (StartDateTime <= @DateEnd and (FinishDateTime is null OR @DateStart <= FinishDateTime));
		end else begin --Точка КА
			insert into @tbl (ID, TypeHierarchy, Tariff_ID, StartDateTime, FinishDateTime) 
			select @ID, @TypeHierarchy, Tariff_ID, StartDateTime, FinishDateTime from dbo.DictTariffs_ToContrTI
			where ContrTI_ID = @TI_ID 
			and (StartDateTime <= @DateEnd and (FinishDateTime is null OR @DateStart <= FinishDateTime));
		end
	end
RETURN
END

   go
   grant select on usf2_DictTariff_SelectTariffByTI to [UserCalcService]
go