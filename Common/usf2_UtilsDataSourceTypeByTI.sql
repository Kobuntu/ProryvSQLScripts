if exists (select 1
          from sysobjects
          where  id = object_id('usf2_UtilsDataSourceTypeByTI')
          and type in ('IF', 'FN', 'TF'))
   drop function usf2_UtilsDataSourceTypeByTI
go

-- ======================================================================================
-- Автор:
--
--		Малышев Игорь
--
-- Дата создания:
--
--		Апрель, 2013
--
-- Описание:
--
--		Получаем список дней, источников и их типов 
--	
-- ======================================================================================
create FUNCTION [dbo].[usf2_UtilsDataSourceTypeByTI] 
(
		@isValidateOtherDataSource bit,
		@TP_ID int, -- По этой ТП
		@TI_ID int, -- ТИ
		@EventDate DateTime, --Дата, время
		@DataSource_ID int --Источник
)
      RETURNS @tbl TABLE 
	  (
		[DataSourceType] tinyint, --Тип источника
		[IsPrimary] bit NOT NULL, --Это самый приоритетный источник
		[IsManualySetPrimary] bit NOT NULL, -- Этот источник явно указан как главный
		[Priority] int 
	 )
AS
BEGIN
declare 
	@DataSourceType tinyint, --Тип источника
	@IsPrimary bit, --Это самый приоритетный источник
	@IsManualySetPrimary bit, -- Этот источник явно указан как главный
	@Priority int, --Приоритет источника, чем больше тем выше
	@Month int,
	@Year int,
	@ds int

		set @Month = Month(@EventDate);
		set @Year = Year(@EventDate);
		set @IsManualySetPrimary = 0;

			if (@TP_ID is not null) begin
				--Необходимо выбрать источник для расчета по связке с сечением и ТП
				set @ds = (select DataSource_ID from Expl_DataSource_To_TI_TP where TI_ID = @TI_ID and TP_ID = @TP_ID and [Year] = @Year and [Month] = @Month);
				if (@ds is not null) begin
					if (@DataSource_ID = @ds) begin
						set @IsManualySetPrimary = 1;
						set @IsPrimary = 1;
					end else begin
						set @IsManualySetPrimary = 0;
						set @IsPrimary = 0;
					end;
				end;
			end

			--Чтение приоритета источника
			set @Priority = (select [Priority] from Expl_DataSource_PriorityList where [Month] = @month and [Year] = @year and DataSource_ID = @DataSource_ID);

			if (@IsPrimary is null) begin
				--Не удалось установить приоритетность источника по связке с сечение или в этом небыло необходимости, выбираем из общей таблицы
				if (@Priority = (select Max([Priority]) from Expl_DataSource_PriorityList where [Year] = @year and [Month] = @month)) begin
					set	@IsPrimary = 1;
				end else begin 
					set	@IsPrimary = 0;
				end;
			end

			set @DataSourceType = (select [DataSourceType] from [dbo].[Expl_DataSource_List] where DataSource_ID = @DataSource_ID)

		--Вставляем самый приоритетный источник
		insert @tbl values (@DataSourceType, @IsPrimary, @IsManualySetPrimary, @Priority);
		
RETURN
END
go
grant select on usf2_UtilsDataSourceTypeByTI to [UserCalcService]
go
grant select on usf2_UtilsDataSourceTypeByTI to [UserMaster61968Service]
go