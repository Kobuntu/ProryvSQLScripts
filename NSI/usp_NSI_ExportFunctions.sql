set ANSI_NULLS ON
set QUOTED_IDENTIFIER ON
go
if exists (select 1
          from sysobjects
          where  id = object_id('usf2_NSI_Export_PSwithSections')
          and type in ('IF', 'FN', 'TF'))
   drop function usf2_NSI_Export_PSwithSections
go

-- ======================================================================================
-- Автор:
--
--		Боровиков Сергей
--
-- Дата создания:
--
--		Август, 2011
--
-- Описание:
--
--		Выгрузка из НСИ (№1)
--
-- ======================================================================================


CREATE FUNCTION [dbo].[usf2_NSI_Export_PSwithSections]()
RETURNS @table TABLE (МЭС varchar(255),
                      Сети varchar(255),
					  Код_сечения_АТС varchar(255),
					  Наименование_перетока_АТС varchar(255),
					  Тип_сечения_АТС varchar(255),
					  Регистрационный_номер_ПСИ varchar(255) null,
					  Дата_регистрации_ПСИ datetime null,
					  Дата_вступления_в_силу_ПСИ datetime null,
					  Срок_окончания_действия_ПСИ datetime null,
					  Наименование_ПС varchar(255),
					  Балансовая_принадлежность_ПС int,
					  Уровень_напряжения_ПС float,
					  PS_ID int,
					  Кол_во_расчетных_ТИ_на_ПС int,
					  Кол_во_контрольных_ТИ_на_ПС int,
					  Кол_во_ТП_на_ПС int)
AS BEGIN	    
	INSERT INTO @table
	SELECT HL1.StringName,
		HL3.StringName,
		S.ATSSectionCode+'-'+S.ATSSubjORECOde,
		S.SectionName,
		S.SectionType,
		PSI.RegistrationNumber,
		PSI.RegistrationDateTime,
		PSI.StartDateTime,
		PSI.FinishDateTime,
		PS.StringName,
		PS.PSProperty,
		PS.PSVoltage,
		PS.PS_ID,		
		(SELECT COUNT(TI.TI_ID) FROM dbo.Info_Section_Description2 SD
			JOIN dbo.Info_TP2 TP ON TP.TP_ID = SD.TP_ID
			JOIN dbo.Info_TI TI ON TI.TP_ID = TP.TP_ID
			WHERE TP.IsMoneyOurSide = 1 AND TI.AccountType = 0 AND Section_ID = S.Section_ID AND TI.PS_ID = u.PS_ID AND TP.IsMoneyOurSideMode2 = 1) as cnt2,
		(SELECT COUNT(TI.TI_ID) FROM dbo.Info_Section_Description2 SD
			JOIN dbo.Info_TP2 TP ON TP.TP_ID = SD.TP_ID
			JOIN dbo.Info_TI TI ON TI.TP_ID = TP.TP_ID
			WHERE TP.IsMoneyOurSide = 1 AND TI.AccountType = 1 AND Section_ID = S.Section_ID AND TI.PS_ID = u.PS_ID AND TP.IsMoneyOurSideMode2 = 1) as cnt3,
		(SELECT COUNT(TP.TP_ID) FROM dbo.Info_TP2 TP
			JOIN dbo.Info_TI TI	ON TI.TP_ID = TP.TP_ID
			WHERE TI.PS_ID = PS.PS_ID) as cnt4
from
( 
	--Точки связанные напрямую 
	select distinct ps_id, Section_id from info_ti ti 
	join dbo.Info_Section_Description2 isd on ti.tp_id = isd.tp_id  
	union 
	--Связанные через формулу 
	select distinct ti.ps_id, isd.Section_id from dbo.Info_TP2_OurSide_Formula_List fl 
	cross apply usf2_Info_GetFormulasNeededForMainFormulaOurSide(fl.Formula_UN, 100, null) usf
	join dbo.Info_Section_Description2 isd on usf.MainTP_ID = isd.TP_ID 
	join dbo.Info_TI ti on usf.ti_id = ti.ti_id 
	where usf.TI_ID is not null
) u
	join dbo.Info_Section_List S on u.Section_ID = S.Section_ID 
	left join dbo.Info_Section_PSI PSI on S.Section_ID = PSI.Section_ID
	join Dict_PS ps on ps.PS_ID = u.PS_ID 
	JOIN dbo.Dict_HierLev3 HL3 ON HL3.HierLev3_ID = PS.HierLev3_ID 
	JOIN dbo.Dict_HierLev2 HL2 ON HL2.HierLev2_ID = HL3.HierLev2_ID 
	JOIN dbo.Dict_HierLev1 HL1 ON HL1.HierLev1_ID = HL2.HierLev1_ID
	
UNION

		SELECT HL1.StringName,
		NULL,
		S.ATSSectionCode+'-'+S.ATSSubjORECOde,
		S.SectionName,
		S.SectionType,
		PSI.RegistrationNumber,
		PSI.RegistrationDateTime,
		PSI.StartDateTime,
		PSI.FinishDateTime,
		CPS.StringName,
		CPS.PSProperty,
		CPS.PSVoltage,
		PS.PS_ID,		
		(SELECT COUNT(CTI.ContrTI_ID) FROM dbo.Info_Section_Description2 SD
			JOIN dbo.Info_TP2 TP ON TP.TP_ID = SD.TP_ID
			JOIN dbo.Info_Contr_TI CTI ON CTI.TP_ID2 = TP.TP_ID
			WHERE CTI.AccountType = 0 AND Section_ID = S.Section_ID AND CTI.Contr_PS_ID = CPS.Contr_PS_ID) as cnt2,
		(SELECT COUNT(CTI.ContrTI_ID) FROM dbo.Info_Section_Description2 SD
			JOIN dbo.Info_TP2 TP ON TP.TP_ID = SD.TP_ID
			JOIN dbo.Info_Contr_TI CTI ON CTI.TP_ID2 = TP.TP_ID
			WHERE CTI.AccountType = 1 AND Section_ID = S.Section_ID AND CTI.Contr_PS_ID = CPS.Contr_PS_ID) as cnt3,
		(SELECT COUNT(TP.TP_ID) FROM dbo.Info_TP2 TP
			JOIN dbo.Info_Contr_TI CTI ON CTI.TP_ID2 = TP.TP_ID
			WHERE CTI.Contr_PS_ID = CPS.Contr_PS_ID) as cnt4
from dbo.Info_Section_List S
join dbo.Info_Section_Description2 SD2 ON SD2.Section_ID = S.Section_ID
join dbo.Info_TP2 TP ON SD2.TP_ID = TP.TP_ID
join dbo.Info_Contr_TI CTI ON CTI.TP_ID2 = TP.TP_ID
join dbo.Info_TI TI ON TP.TP_ID = TI.TP_ID
join dbo.Dict_PS PS ON TI.PS_ID = PS.PS_ID
join dbo.Dict_Contr_PS CPS ON CTI.Contr_PS_ID = CPS.Contr_PS_ID
join dbo.Dict_HierLev1 HL1 ON CPS.HierLev1_ID = HL1.HierLev1_ID
left join dbo.Info_Section_PSI PSI on S.Section_ID = PSI.Section_ID
RETURN
END
GO

grant select on usf2_NSI_Export_PSwithSections to [UserCalcService]
go

if exists (select 1
          from sysobjects
          where  id = object_id('usf2_NSI_Export_TPCountInPS')
          and type in ('IF', 'FN', 'TF'))
   drop function usf2_NSI_Export_TPCountInPS
go

-- ======================================================================================
-- Автор:
--
--		Боровиков Сергей
--
-- Дата создания:
--
--		Август, 2011
--
-- Описание:
--
--		Выгрузка из НСИ (№2)
--
-- ======================================================================================


CREATE FUNCTION [dbo].[usf2_NSI_Export_TPCountInPS] ()
RETURNS @table TABLE (МЭС varchar(255),
					  Код_сечения_АТС varchar(255),
					  Наименование_перетока_АТС varchar(255),
					  Регистрационный_номер varchar(255) null,
					  Дата_регистрации datetime null,
					  Дата_вступления_в_силу datetime null,
					  Срок_действия datetime null,
					  Подстанция varchar(255),
					  Количество_ТП int,
					  PS_ID int)
AS BEGIN	    
		INSERT INTO @table
		SELECT HL1.StringName,
			   S.ATSSectionCode+'-'+S.ATSSubjORECOde,
			   S.SectionName,
			   PSI.RegistrationNumber,
			   PSI.RegistrationDateTime,
			   PSI.StartDateTime,
			   PSI.FinishDateTime,
			   PS.StringName,
			   COUNT(TP.TP_ID) as cnt,
			   PS.PS_ID
			   
		   FROM ( 
	--Точки связанные напрямую 
	select distinct ps_id, Section_id from info_ti ti 
	join dbo.Info_Section_Description2 isd on ti.tp_id = isd.tp_id  
	union 
	--Связанные через формулу 
	select distinct ti.ps_id, isd.Section_id from dbo.Info_TP2_OurSide_Formula_List fl 
	cross apply usf2_Info_GetFormulasNeededForMainFormulaOurSide(fl.Formula_UN, 100, null) usf
	join dbo.Info_Section_Description2 isd on usf.MainTP_ID = isd.TP_ID 
	join dbo.Info_TI ti on usf.ti_id = ti.ti_id 
	where usf.TI_ID is not null
) u
    join dbo.Info_Section_List S on u.Section_ID = S.Section_ID 
	left join dbo.Info_Section_PSI PSI on S.Section_ID = PSI.Section_ID
	join Dict_PS ps on ps.PS_ID = u.PS_ID 
	JOIN dbo.Dict_HierLev3 HL3 ON HL3.HierLev3_ID = PS.HierLev3_ID 
	JOIN dbo.Dict_HierLev2 HL2 ON HL2.HierLev2_ID = HL3.HierLev2_ID 
	JOIN dbo.Dict_HierLev1 HL1 ON HL1.HierLev1_ID = HL2.HierLev1_ID
	join dbo.Info_Section_Description2 SD ON SD.Section_ID = S.Section_ID
	join dbo.Info_TP2 TP ON TP.TP_ID = SD.TP_ID
	join dbo.Info_TI TI ON TI.TP_ID = TP.TP_ID AND TI.PS_ID = PS.PS_ID
	WHERE TP.IsMoneyOurSideMode2 = 1 AND TP.IsMoneyOurSide = 1
	GROUP BY PS.PS_ID, HL1.StringName, S.SectionName, PSI.RegistrationNumber,
		PSI.RegistrationDateTime, PSI.StartDateTime, PSI.FinishDateTime,
		PS.StringName, S.ATSSectionCode, S.ATSSubjORECOde
	RETURN
	END
GO

grant select on usf2_NSI_Export_TPCountInPS to [UserCalcService]
go

if exists (select 1
          from sysobjects
          where  id = object_id('usf2_NSI_Export_SectionDVKR')
          and type in ('IF', 'FN', 'TF'))
   drop function usf2_NSI_Export_SectionDVKR
go

-- ======================================================================================
-- Автор:
--
--		Боровиков Сергей
--
-- Дата создания:
--
--		Август, 2011
--
-- Описание:
--
--		Выгрузка из НСИ (№3)
--
-- ======================================================================================

CREATE FUNCTION [dbo].[usf2_NSI_Export_SectionDVKR] ()	
RETURNS @table TABLE (МЭС varchar(255),
                      МСК varchar(255),
					  Сечение_АТС varchar(255),
					  Переток_откуда varchar(255),
					  Переток_куда varchar(255),
					  Тип_сечения tinyint,
					  Наименование_перетока varchar(255),
					  Регистрационный_номер_ПСИ varchar(255) null,
					  Дата_регистрации_ПСИ datetime null,
					  Дата datetime null,
					  Дата_окончания_действия_ПСИ datetime null,
					  Смежный_субъект_ОРЭМ varchar(255),
					  Section_ID int,
					  Кол_во_ТП_в_сечении int,
					  Ценовая_зона_ОРЭМ varchar(255))
AS BEGIN
		INSERT INTO @table 
		SELECT HL1.StringName,
		       HL3.StringName,
			   S.ATSSectionCode+'-'+S.ATSSubjORECOde,
			   S.ATSSectionCode,
			   S.ATSSubjORECOde,
			   S.SectionType,
			   S.SectionName,
			   PSI.RegistrationNumber,
			   PSI.RegistrationDateTime,
			   PSI.StartDateTime,
			   PSI.FinishDateTime,
			   ORE.SubjOREName,
			   S.Section_ID,
			   (SELECT COUNT(SD2.TP_ID) from dbo.Info_Section_Description2 SD2
			    JOIN dbo.Info_TP2 TP on SD2.TP_ID = TP.TP_ID
			    WHERE SD2.Section_ID = S.Section_ID) as cnt,
			   PZ.StringName
		FROM dbo.Info_Section_List S
		LEFT JOIN dbo.Dict_HierLev3 HL3 ON HL3.HierLev3_ID = S.HierLev3_ID
		JOIN dbo.Dict_HierLev1 HL1 ON HL1.HierLev1_ID = S.HierLev1_ID
		LEFT JOIN dbo.Dict_SubjORE ORE ON ORE.SubjORE_ID = S.SubjORE_ID
		left join dbo.Info_Section_PSI PSI on S.Section_ID = PSI.Section_ID
		left join dbo.Info_ObjectTerritory_Section OTS on S.Section_ID = OTS.Section_ID
		left join dbo.Info_ObjectTerritory_PriceZone OTPZ on OTS.ObjectTerritory_ID = OTPZ.ObjectTerritory_ID
		left join dbo.Dict_PriceZone PZ on OTPZ.PriceZone_ID = PZ.PriceZone_ID
	RETURN
	END
GO

grant select on usf2_NSI_Export_SectionDVKR to [UserCalcService]
go


if exists (select 1
          from sysobjects
          where  id = object_id('usf2_NSI_Export_SectionTICount')
          and type in ('IF', 'FN', 'TF'))
   drop function usf2_NSI_Export_SectionTICount
go

-- ======================================================================================
-- Автор:
--
--		Боровиков Сергей
--
-- Дата создания:
--
--		Август, 2011
--
-- Описание:
--
--		Выгрузка из НСИ (№4)
--
-- ======================================================================================

CREATE FUNCTION [dbo].[usf2_NSI_Export_SectionTICount] ()	
RETURNS @table TABLE (МЭС varchar(255),
                      Наименование_сетей varchar(255),
					  Код_ОАО_АТС_сечения varchar(255),
					  Наименование_перетока_АТС varchar(255),
					  Тип_сечения tinyint,
					  Регистрационный_номер_ПСИ varchar(255) null,
					  Дата_регистрации_ПСИ datetime null,
					  Дата_вступления_в_силу_ПСИ datetime null,
					  Срок_действия_ПСИ datetime null,
					  Section_ID int,
					  Кол_во_расчетных_ТИ_на_ПС_ФСК int,
					  Кол_во_расчетных_ТИ_на_ПС_контрагента int,
					  Кол_во_контрольных_ТИ_на_ПС_ФСК int,
					  Кол_во_контрольных_ТИ_на_ПС_контрагента int)
AS BEGIN
		INSERT INTO @table 
		SELECT HL1.StringName,
		       HL3.StringName,
			   S.ATSSectionCode+'-'+S.ATSSubjORECOde,
			   S.SectionName,
			   S.SectionType,
			   PSI.RegistrationNumber,
			   PSI.RegistrationDateTime,
			   PSI.StartDateTime,
			   PSI.FinishDateTime,			   
			   S.Section_ID,
			   (SELECT COUNT(TI.TP_ID) FROM dbo.Info_Section_Description2 SD
					JOIN dbo.Info_TP2 TP ON TP.TP_ID = SD.TP_ID
					JOIN dbo.Info_TI TI ON TI.TP_ID = TP.TP_ID
					WHERE TP.IsMoneyOurSide = 1 AND Section_ID = S.Section_ID AND TI.AccountType = 0) as cnt2,			
			   (SELECT COUNT(TI.TP_ID) FROM dbo.Info_Section_Description2 SD
					JOIN dbo.Info_TP2 TP ON TP.TP_ID = SD.TP_ID
					JOIN dbo.Info_TI TI ON TI.TP_ID = TP.TP_ID
					WHERE TP.IsMoneyOurSide = 0 AND Section_ID = S.Section_ID AND TI.AccountType = 0) as cnt3,
			   (SELECT COUNT(TI.TP_ID) FROM dbo.Info_Section_Description2 SD
					JOIN dbo.Info_TP2 TP ON TP.TP_ID = SD.TP_ID
					JOIN dbo.Info_TI TI ON TI.TP_ID = TP.TP_ID
					WHERE TP.IsMoneyOurSide = 1 AND Section_ID = S.Section_ID AND TI.AccountType = 1) as cnt4,			
			   (SELECT COUNT(TI.TP_ID) FROM dbo.Info_Section_Description2 SD
					JOIN dbo.Info_TP2 TP ON TP.TP_ID = SD.TP_ID
					JOIN dbo.Info_TI TI ON TI.TP_ID = TP.TP_ID
					WHERE TP.IsMoneyOurSide = 0 AND Section_ID = S.Section_ID AND TI.AccountType = 1) as cnt5
		FROM dbo.Info_Section_List S
		JOIN dbo.Dict_HierLev3 HL3 ON HL3.HierLev3_ID = S.HierLev3_ID
		JOIN dbo.Dict_HierLev1 HL1 ON HL1.HierLev1_ID = S.HierLev1_ID
		left join dbo.Info_Section_PSI PSI on S.Section_ID = PSI.Section_ID
	RETURN
	END
GO

grant select on usf2_NSI_Export_SectionTICount to [UserCalcService]
go

if exists (select 1
          from sysobjects
          where  id = object_id('usf2_NSI_Export_SectionTICountByPS')
          and type in ('IF', 'FN', 'TF'))
   drop function usf2_NSI_Export_SectionTICountByPS
go

-- ======================================================================================
-- Автор:
--
--		Боровиков Сергей
--
-- Дата создания:
--
--		Август, 2011
--
-- Описание:
--
--		Выгрузка из НСИ (№5)
--
-- ======================================================================================


CREATE FUNCTION [dbo].[usf2_NSI_Export_SectionTICountByPS] ()
RETURNS @table TABLE (МЭС varchar(255),
                      Наименование_сетей varchar(255),
					  Код_НП_АТС varchar(255),
					  Наименование_перетока varchar(255),
					  Тип_сечения varchar(255),
					  Регистрационный_номер varchar(255) null,
					  Дата_регистрации datetime null,
					  Дата_вступления_в_силу datetime null,
					  Срок_действия datetime null,
					  Наименование_ПС varchar(255),
					  ПС_Вольтаж float,
					  Кол_во_ТИ int,
					  Тип__учета varchar(255),
					  PS_ID int,
					  Кол_во_расчетных_ТИ_на_ПС int,
					  Кол_во_контрольных_ТИ_на_ПС int)
AS BEGIN	    

		INSERT INTO @table
		SELECT HL1.StringName,
			   HL3.StringName,
			   S.ATSSectionCode+'-'+S.ATSSubjORECOde,
			   S.SectionName,
			   S.SectionType,
			   PSI.RegistrationNumber,
			   PSI.RegistrationDateTime,
		       PSI.StartDateTime,
		       PSI.FinishDateTime,
			   PS.StringName, 
			   PS.PSVoltage,
			   (SELECT COUNT(TI.TP_ID) FROM dbo.Info_Section_Description2 SD
					JOIN dbo.Info_TP2 TP ON TP.TP_ID = SD.TP_ID
					JOIN dbo.Info_TI TI ON TI.TP_ID = TP.TP_ID
					WHERE TP.IsMoneyOurSide = 1 AND Section_ID = S.Section_ID AND TI.PS_ID = u.PS_ID) as cnt,
			   'Расчетный',
			   PS.PS_ID,
			   (SELECT COUNT(TI.TP_ID) FROM dbo.Info_Section_Description2 SD
					JOIN dbo.Info_TP2 TP ON TP.TP_ID = SD.TP_ID
					JOIN dbo.Info_TI TI ON TI.TP_ID = TP.TP_ID
					WHERE TP.IsMoneyOurSide = 1 AND TI.AccountType = 0 AND Section_ID = S.Section_ID AND TI.PS_ID = u.PS_ID AND TP.IsMoneyOurSideMode2 = 1) as cnt2,
		       (SELECT COUNT(TI.TP_ID) FROM dbo.Info_Section_Description2 SD
					JOIN dbo.Info_TP2 TP ON TP.TP_ID = SD.TP_ID
					JOIN dbo.Info_TI TI ON TI.TP_ID = TP.TP_ID
					WHERE TP.IsMoneyOurSide = 1 AND TI.AccountType = 1 AND Section_ID = S.Section_ID AND TI.PS_ID = u.PS_ID AND TP.IsMoneyOurSideMode2 = 1) as cnt3
		   FROM (
	--Точки связанные напрямую 
	select distinct ps_id, Section_id from info_ti ti 
	join dbo.Info_Section_Description2 isd on ti.tp_id = isd.tp_id  
	union 
	--Связанные через формулу 
	select distinct ti.ps_id, isd.Section_id from dbo.Info_TP2_OurSide_Formula_List fl 
	cross apply usf2_Info_GetFormulasNeededForMainFormulaOurSide(fl.Formula_UN, 100, null) usf
	join dbo.Info_Section_Description2 isd on usf.MainTP_ID = isd.TP_ID 
	join dbo.Info_TI ti on usf.ti_id = ti.ti_id 
	where usf.TI_ID is not null
) u
		join dbo.Info_Section_List S on u.Section_ID = S.Section_ID 
		left join dbo.Info_Section_PSI PSI on S.Section_ID = PSI.Section_ID
		join Dict_PS ps on ps.PS_ID = u.PS_ID 
		JOIN dbo.Dict_HierLev3 HL3 ON HL3.HierLev3_ID = PS.HierLev3_ID 
		JOIN dbo.Dict_HierLev2 HL2 ON HL2.HierLev2_ID = HL3.HierLev2_ID 
		JOIN dbo.Dict_HierLev1 HL1 ON HL1.HierLev1_ID = HL2.HierLev1_ID
	RETURN
	END
GO

grant select on usf2_NSI_Export_SectionTICountByPS to [UserCalcService]
go

if exists (select 1
          from sysobjects
          where  id = object_id('dbo.usf2_NSI_Export_Full_TI')
          and type in ('IF', 'FN', 'TF'))
   drop function dbo.usf2_NSI_Export_Full_TI
go

-- ======================================================================================
-- Автор:
--
--		Боровиков Сергей
--
-- Дата создания:
--
--		Август, 2011
--
-- Описание:
--
--		Выгрузка из НСИ (№6)
--
-- ======================================================================================

CREATE FUNCTION [dbo].[usf2_NSI_Export_Full_TI]
(
	@PS_ID int,
	@IsActual bit
)
RETURNS @table TABLE (МЭС varchar(255),
                      Наименование_сетей varchar(255),
					  Наименование_ПС varchar(255),
					  Наименование_ТИ varchar(255),
					  Тип_сбора_данных_по_ТИ varchar(255),
					  Код_ТИ_АТС varchar(255),					  
					  Напряжение_ТИ float,
					  Тип_учета varchar(255),
					  Код_ЕПП varchar(255),
					  Является_малой_ТИ bit,
					  Наименование_ТП varchar(255),
					  Наименование_Сечения varchar(255),
					  Тип_ПУ varchar(255),
					  Инферсия_ПУ varchar(255),
					  Номер_ПУ varchar(255),
					  Расчетный_коэффициент float,
					  Кт float,
					  Кн float,
					  Погрешность_ИК float,
					  Является_ОВ bit,					
					  ТИ_внесена_в_паспорт bit,					  
					  Номер_паспорта varchar(255),
					  Тип_учета_ТИ_в_паспорте varchar(255),
					  Срок_действия_паспорта datetime,
					  ТИ_является_перспективной bit,
					  Перспективный_вид_учета varchar(255),
					  Отсутствие_технической_возможности_установки_ИК_паспорта bit,
					  Необходима_установка_счетчика bit,
					  Необходима_установка_ТТ bit,
					  Необходима_установка_ТN bit,
					  Наименование_АИИС_АТС varchar(255),
					  Номер_АИИС_АТС varchar(255),
					  Номер_резервного_счетчика varchar(255),
					  Тип_резервного_счетчика varchar(255),
					  Номер_контрольного_счетчика varchar(255),
					  Тип_контрольного_счетчика varchar(255),
					  Номер_технического_счетчика varchar(255),
					  Тип_технического_счетчика varchar(255))
AS BEGIN
		INSERT INTO @table 
		SELECT HL1.StringName,
		       HL3.StringName,
			   PS.StringName,
			   TI.TIName,
			   TT.StringName,
			   TI.TIATSCode,
			   TI.Voltage,
			   CASE TI.Commercial
					WHEN 1 THEN 'Коммерческий'
					ELSE 'Технический'
			   END,
			   TI.EPP_ID,
			   TI.IsSmallTI,
			   TP.StringName,
			   S.SectionName,
			   DMT.MeterTypeName,
			   CASE ISNULL(TI.AIATSCode,1)
					WHEN 1 THEN 'Не перевернут'
					ELSE 'Перевернут'
			   END,
			   HM.MeterSerialNumber,
			   IT.COEFI * IT.COEFU,
			   IT.COEFI,
			   IT.COEFU,
			   MCE.MeasuringComplexError,
			   CASE
				    WHEN OV.OV_ID IS NULL THEN 0
			  	    ELSE 1
			   END,			   
			   PSP.IncludedInPassport,
			   PSP.PassportNumber,
			   CASE PSP.Commercial
					WHEN 1 THEN 'Коммерческий'
					ELSE 'Технический'
			   END,
			   PSP.FinishDateTime,
			   CASE
					WHEN PL.PerspectiveMeteringType IS NULL THEN 0
					ELSE 1
			   END,
			   CASE PL.PerspectiveMeteringType
					WHEN 0 THEN 'Расчетный'
					WHEN 1 THEN 'Контрольный'
					WHEN 2 THEN 'Технический'
			   END,
			   PL.CannotInstallIKPassport,
			   PL.NeedInstallMeter,
			   PL.NeedInstallTT,
			   PL.NeedInstallTN,
			   TA.ATSAISName,
			   TA.ATSAISAdditional,
			   RM.ReservMeterSerialNumber,
			   DMT_RES.MeterTypeName,
			   RM.ControlMeterSerialNumber,
			   DMT_CNT.MeterTypeName,
			   RM.TechMeterSerialNumber,
			   DMT_TEC.MeterTypeName
		FROM dbo.Info_TI TI
		LEFT JOIN dbo.Info_Meters_TO_TI MTT ON MTT.TI_ID = TI.TI_ID AND MTT.StartDateTime < GETDATE() AND MTT.FinishDateTime > GETDATE()
		LEFT JOIN dbo.Hard_Meters HM ON HM.Meter_ID = MTT.METER_ID
		LEFT JOIN dbo.Dict_Meters_Types DMT ON DMT.MeterType_ID = HM.MeterType_ID
		LEFT JOIN dbo.Info_Transformators IT ON IT.TI_ID = TI.TI_ID AND IT.StartDateTime < GETDATE() AND IT.FinishDateTime > GETDATE()
		LEFT JOIN dbo.Info_MeasuringComplexError MCE ON MCE.TI_ID = TI.TI_ID AND MCE.StartDateTime < GETDATE() AND MCE.FinishDateTime > GETDATE()
		LEFT JOIN dbo.Hard_OV_List OV ON OV.TI_ID = TI.TI_ID
		LEFT JOIN dbo.Info_TI_Passport_AIIS_KUE PSP ON PSP.TI_ID = TI.TI_ID
		LEFT JOIN dbo.Info_TI_Plan_Info PL ON PL.TI_ID = TI.TI_ID
		LEFT JOIN dbo.Dict_TI_Types TT ON TT.TIType = TI.TIType
		LEFT JOIN dbo.Dict_TI_AIS TA ON TA.ATSAIS_ID = TI.ATSAIS_ID
		LEFT JOIN dbo.Info_TI_Reserv_Meters RM ON RM.TI_ID = TI.TI_ID
		LEFT JOIN dbo.Dict_Meters_Types DMT_RES ON DMT_RES.MeterType_ID = RM.ReservMeterType_ID
		LEFT JOIN dbo.Dict_Meters_Types DMT_CNT ON DMT_CNT.MeterType_ID = RM.ControlMeterType_ID
		LEFT JOIN dbo.Dict_Meters_Types DMT_TEC ON DMT_TEC.MeterType_ID = RM.TechMeterType_ID
		LEFT JOIN dbo.Dict_PS PS ON PS.PS_ID = TI.PS_ID
		JOIN dbo.Dict_HierLev3 HL3 ON HL3.HierLev3_ID = PS.HierLev3_ID 
		JOIN dbo.Dict_HierLev2 HL2 ON HL2.HierLev2_ID = HL3.HierLev2_ID 
		JOIN dbo.Dict_HierLev1 HL1 ON HL1.HierLev1_ID = HL2.HierLev1_ID
		LEFT JOIN dbo.Info_TP2 TP ON TP.TP_ID = TI.TP_ID
		LEFT JOIN dbo.Info_Section_Description2 SD ON SD.TP_ID = TP.TP_ID
		LEFT JOIN dbo.Info_Section_List S ON SD.Section_ID = S.Section_ID
		LEFT JOIN dbo.Info_TP2_OurSide_Formula_List OFL ON OFL.TP_ID = TP.TP_ID
		WHERE PS.PS_ID = @PS_ID									
			AND (@IsActual = 0 OR (@IsActual = 1 AND OFL.Formula_UN IS NOT NULL))
	RETURN
	END
GO

grant select on dbo.usf2_NSI_Export_Full_TI to [UserCalcService]
go


if exists (select 1
          from sysobjects
          where  id = object_id('dbo.usf2_NSI_Export_Full_TP')
          and type in ('IF', 'FN', 'TF'))
   drop function dbo.usf2_NSI_Export_Full_TP
go


-- ======================================================================================
-- Автор:
--
--		Боровиков Сергей
--
-- Дата создания:
--
--		Август, 2011
--
-- Описание:
--
--		Выгрузка из НСИ (№7)
--
-- ======================================================================================

CREATE FUNCTION [dbo].[usf2_NSI_Export_Full_TP]
(
	@Section_ID int,
	@IsActual bit
)
RETURNS @table TABLE (МЭС varchar(255),
                      Наименование_сетей varchar(255),
					  Наименование_Сечения varchar(255),
					  Наименование_ТП varchar(255),
					  Напряжение_ТП int,
					  Тип_точки_поставки tinyint,					  
					  Код_ОАО_АТС varchar(255))
AS BEGIN
		INSERT INTO @table 
		SELECT HL1.StringName,
		       HL3.StringName,
			   S.SectionName,
			   TP.StringName,
			   CASE 
					WHEN TI.TI_ID IS NULL THEN CTI.Voltage
					ELSE TI.Voltage
			   END,
			   TP.TPMode,			  			   
			   TP.TPATSCode
		FROM dbo.Info_TP2 TP
		JOIN dbo.Info_Section_Description2 SD ON SD.TP_ID = TP.TP_ID
		JOIN dbo.Info_Section_List S ON SD.Section_ID = S.Section_ID
		JOIN dbo.Dict_HierLev1 HL1 ON HL1.HierLev1_ID = S.HierLev1_ID			
		LEFT JOIN dbo.Dict_HierLev3 HL3 ON HL3.HierLev3_ID = S.HierLev3_ID 	
		LEFT JOIN dbo.Info_TI TI ON TI.TP_ID = TP.TP_ID	
		LEFT JOIN dbo.Info_Contr_TI CTI ON CTI.TP_ID2 = TP.TP_ID
		LEFT JOIN dbo.Info_TP2_Contr_Formula_List CFL ON CFL.TP_ID = TP.TP_ID
		LEFT JOIN dbo.Info_TP2_OurSide_Formula_List OFL ON OFL.TP_ID = TP.TP_ID
		WHERE S.Section_ID = @Section_ID
			AND (@IsActual = 0 OR (@IsActual = 1 AND 
				(TI.TI_ID IS NOT NULL OR CTI.ContrTI_ID IS NOT NULL OR CFL.Formula_UN IS NOT NULL OR OFL.Formula_UN IS NOT NULL)))
	RETURN
	END
GO


grant select on dbo.usf2_NSI_Export_Full_TP to [UserCalcService]
go

if exists (select 1
          from sysobjects
          where  id = object_id('usf2_NSI_Export_Full_TI_and_CA')
          and type in ('IF', 'FN', 'TF'))
   drop function dbo.usf2_NSI_Export_Full_TI_and_CA
go


CREATE FUNCTION [dbo].[usf2_NSI_Export_Full_TI_and_CA]
(
	@MES_ID int,
	@IsActual bit
)
RETURNS @table TABLE (МЭС varchar(255),
                      Наименование_сетей varchar(255),
					  Наименование_ПС varchar(255),
					  Наименование_ТИ varchar(255),
					  Тип_сбора_данных_по_ТИ varchar(255),
					  Код_ТИ_АТС varchar(255),					  
					  Напряжение_ТИ float,
					  Тип_учета varchar(255),
					  Код_ЕПП varchar(255),
					  Является_малой_ТИ bit,
					  Наименование_ТП varchar(255),
					  Наименование_Сечения varchar(255),
					  Тип_ПУ varchar(255),
					  Инферсия_ПУ varchar(255),
					  Номер_ПУ varchar(255),
					  Расчетный_коэффициент float,
					  Кт float,
					  Кн float,
					  Погрешность_ИК float,
					  Является_ОВ bit,					
					  ТИ_внесена_в_паспорт bit,					  
					  Номер_паспорта varchar(255),
					  Тип_учета_ТИ_в_паспорте varchar(255),
					  Срок_действия_паспорта datetime,
					  ТИ_является_перспективной bit,
					  Перспективный_вид_учета varchar(255),
					  Отсутствие_технической_возможности_установки_ИК_паспорта bit,
					  Необходима_установка_счетчика bit,
					  Необходима_установка_ТТ bit,
					  Необходима_установка_ТN bit,
					  Наименование_АИИС_АТС varchar(255),
					  Номер_АИИС_АТС varchar(255),
					  Номер_резервного_счетчика varchar(255),
					  Тип_резервного_счетчика varchar(255),
					  Номер_контрольного_счетчика varchar(255),
					  Тип_контрольного_счетчика varchar(255),
					  Номер_технического_счетчика varchar(255),
					  Тип_технического_счетчика varchar(255))
AS BEGIN
		INSERT INTO @table 
		SELECT HL1.StringName,
		       HL3.StringName,
			   PS.StringName,
			   TI.TIName,
			   TT.StringName,
			   TI.TIATSCode,
			   TI.Voltage,
			   CASE TI.Commercial
					WHEN 1 THEN 'Коммерческий'
					ELSE 'Технический'
			   END,
			   TI.EPP_ID,
			   TI.IsSmallTI,
			   TP.StringName,
			   S.SectionName,
			   DMT.MeterTypeName,
			   CASE ISNULL(TI.AIATSCode,1)
					WHEN 1 THEN 'Не перевернут'
					ELSE 'Перевернут'
			   END,
			   HM.MeterSerialNumber,
			   IT.COEFI * IT.COEFU,
			   IT.COEFI,
			   IT.COEFU,
			   MCE.MeasuringComplexError,
			   CASE
				    WHEN OV.OV_ID IS NULL THEN 0
			  	    ELSE 1
			   END,			   
			   PSP.IncludedInPassport,
			   PSP.PassportNumber,
			   CASE PSP.Commercial
					WHEN 1 THEN 'Коммерческий'
					ELSE 'Технический'
			   END,
			   PSP.FinishDateTime,
			   CASE
					WHEN PL.PerspectiveMeteringType IS NULL THEN 0
					ELSE 1
			   END,
			   CASE PL.PerspectiveMeteringType
					WHEN 0 THEN 'Расчетный'
					WHEN 1 THEN 'Контрольный'
					WHEN 2 THEN 'Технический'
			   END,
			   PL.CannotInstallIKPassport,
			   PL.NeedInstallMeter,
			   PL.NeedInstallTT,
			   PL.NeedInstallTN,
			   TA.ATSAISName,
			   TA.ATSAISAdditional,
			   RM.ReservMeterSerialNumber,
			   DMT_RES.MeterTypeName,
			   RM.ControlMeterSerialNumber,
			   DMT_CNT.MeterTypeName,
			   RM.TechMeterSerialNumber,
			   DMT_TEC.MeterTypeName
		FROM dbo.Info_TI TI
		LEFT JOIN dbo.Info_Meters_TO_TI MTT ON MTT.TI_ID = TI.TI_ID AND MTT.StartDateTime < GETDATE() AND MTT.FinishDateTime > GETDATE()
		LEFT JOIN dbo.Hard_Meters HM ON HM.Meter_ID = MTT.METER_ID
		LEFT JOIN dbo.Dict_Meters_Types DMT ON DMT.MeterType_ID = HM.MeterType_ID
		LEFT JOIN dbo.Info_Transformators IT ON IT.TI_ID = TI.TI_ID AND IT.StartDateTime < GETDATE() AND IT.FinishDateTime > GETDATE()
		LEFT JOIN dbo.Info_MeasuringComplexError MCE ON MCE.TI_ID = TI.TI_ID AND MCE.StartDateTime < GETDATE() AND MCE.FinishDateTime > GETDATE()
		LEFT JOIN dbo.Hard_OV_List OV ON OV.TI_ID = TI.TI_ID
		LEFT JOIN dbo.Info_TI_Passport_AIIS_KUE PSP ON PSP.TI_ID = TI.TI_ID
		LEFT JOIN dbo.Info_TI_Plan_Info PL ON PL.TI_ID = TI.TI_ID
		LEFT JOIN dbo.Dict_TI_Types TT ON TT.TIType = TI.TIType
		LEFT JOIN dbo.Dict_TI_AIS TA ON TA.ATSAIS_ID = TI.ATSAIS_ID
		LEFT JOIN dbo.Info_TI_Reserv_Meters RM ON RM.TI_ID = TI.TI_ID
		LEFT JOIN dbo.Dict_Meters_Types DMT_RES ON DMT_RES.MeterType_ID = RM.ReservMeterType_ID
		LEFT JOIN dbo.Dict_Meters_Types DMT_CNT ON DMT_CNT.MeterType_ID = RM.ControlMeterType_ID
		LEFT JOIN dbo.Dict_Meters_Types DMT_TEC ON DMT_TEC.MeterType_ID = RM.TechMeterType_ID
		LEFT JOIN dbo.Dict_PS PS ON PS.PS_ID = TI.PS_ID
		JOIN dbo.Dict_HierLev3 HL3 ON HL3.HierLev3_ID = PS.HierLev3_ID 
		JOIN dbo.Dict_HierLev2 HL2 ON HL2.HierLev2_ID = HL3.HierLev2_ID 
		JOIN dbo.Dict_HierLev1 HL1 ON HL1.HierLev1_ID = HL2.HierLev1_ID
		LEFT JOIN dbo.Info_TP2 TP ON TP.TP_ID = TI.TP_ID
		LEFT JOIN dbo.Info_Section_Description2 SD ON SD.TP_ID = TP.TP_ID
		LEFT JOIN dbo.Info_Section_List S ON SD.Section_ID = S.Section_ID
		LEFT JOIN dbo.Info_TP2_OurSide_Formula_List OFL ON OFL.TP_ID = TP.TP_ID
		WHERE HL1.HierLev1_ID = @MES_ID					
			AND (@IsActual = 0 OR (@IsActual = 1 AND OFL.Formula_UN IS NOT NULL))
		
		-- CA	
		INSERT INTO @table 
		SELECT HL1.StringName,
		       NULL,
			   PS.StringName,
			   TI.TIName,
			   TT.StringName,
			   TI.TIATSCode,
			   TI.Voltage,
			   CASE TI.Commercial
					WHEN 1 THEN 'Коммерческий'
					ELSE 'Технический'
			   END,
			   NULL,
			   NULL,
			   TP.StringName,
			   S.SectionName,
			   NULL,
			   CASE ISNULL(TI.AIATSCode,1)
					WHEN 1 THEN 'Не перевернут'
					ELSE 'Перевернут'
			   END,
			   NULL,
			   NULL,
			   NULL,
			   NULL,
			   NULL,
			   NULL,			   
			   NULL,
			   NULL,
			   NULL,
			   NULL,
			   NULL,
			   NULL,
			   NULL,
			   NULL,
			   NULL,
			   NULL,
			   TA.ATSAISName,
			   TA.ATSAISAdditional,
			   NULL,
			   NULL,
			   NULL,
			   NULL,
			   NULL,
			   NULL
		FROM dbo.Info_Contr_TI TI		
		LEFT JOIN dbo.Dict_TI_Types TT ON TT.TIType = TI.TIType
		LEFT JOIN dbo.Dict_TI_AIS TA ON TA.ATSAIS_ID = TI.ATSAIS_ID
		LEFT JOIN dbo.Dict_Contr_PS PS ON PS.Contr_PS_ID = TI.Contr_PS_ID		  
		JOIN dbo.Dict_HierLev1 HL1 ON HL1.HierLev1_ID = PS.HierLev1_ID
		LEFT JOIN dbo.Info_TP2 TP ON TP.TP_ID = TI.TP_ID2
		LEFT JOIN dbo.Info_Section_Description2 SD ON SD.TP_ID = TP.TP_ID
		LEFT JOIN dbo.Info_Section_List S ON SD.Section_ID = S.Section_ID
		LEFT JOIN dbo.Info_TP2_Contr_Formula_List CFL ON CFL.TP_ID = TP.TP_ID
		WHERE HL1.HierLev1_ID = @MES_ID			
			AND (@IsActual = 0 OR (@IsActual = 1 AND CFL.Formula_UN IS NOT NULL))
	RETURN
	END
GO

grant select on dbo.usf2_NSI_Export_Full_TI_and_CA to [UserCalcService]
go
