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
-- �����:
--
--		��������� ������
--
-- ���� ��������:
--
--		������, 2011
--
-- ��������:
--
--		�������� �� ��� (�1)
--
-- ======================================================================================


CREATE FUNCTION [dbo].[usf2_NSI_Export_PSwithSections]()
RETURNS @table TABLE (��� varchar(255),
                      ���� varchar(255),
					  ���_�������_��� varchar(255),
					  ������������_��������_��� varchar(255),
					  ���_�������_��� varchar(255),
					  ���������������_�����_��� varchar(255) null,
					  ����_�����������_��� datetime null,
					  ����_����������_�_����_��� datetime null,
					  ����_���������_��������_��� datetime null,
					  ������������_�� varchar(255),
					  ����������_��������������_�� int,
					  �������_����������_�� float,
					  PS_ID int,
					  ���_��_���������_��_��_�� int,
					  ���_��_�����������_��_��_�� int,
					  ���_��_��_��_�� int)
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
	--����� ��������� �������� 
	select distinct ps_id, Section_id from info_ti ti 
	join dbo.Info_Section_Description2 isd on ti.tp_id = isd.tp_id  
	union 
	--��������� ����� ������� 
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
-- �����:
--
--		��������� ������
--
-- ���� ��������:
--
--		������, 2011
--
-- ��������:
--
--		�������� �� ��� (�2)
--
-- ======================================================================================


CREATE FUNCTION [dbo].[usf2_NSI_Export_TPCountInPS] ()
RETURNS @table TABLE (��� varchar(255),
					  ���_�������_��� varchar(255),
					  ������������_��������_��� varchar(255),
					  ���������������_����� varchar(255) null,
					  ����_����������� datetime null,
					  ����_����������_�_���� datetime null,
					  ����_�������� datetime null,
					  ���������� varchar(255),
					  ����������_�� int,
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
	--����� ��������� �������� 
	select distinct ps_id, Section_id from info_ti ti 
	join dbo.Info_Section_Description2 isd on ti.tp_id = isd.tp_id  
	union 
	--��������� ����� ������� 
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
-- �����:
--
--		��������� ������
--
-- ���� ��������:
--
--		������, 2011
--
-- ��������:
--
--		�������� �� ��� (�3)
--
-- ======================================================================================

CREATE FUNCTION [dbo].[usf2_NSI_Export_SectionDVKR] ()	
RETURNS @table TABLE (��� varchar(255),
                      ��� varchar(255),
					  �������_��� varchar(255),
					  �������_������ varchar(255),
					  �������_���� varchar(255),
					  ���_������� tinyint,
					  ������������_�������� varchar(255),
					  ���������������_�����_��� varchar(255) null,
					  ����_�����������_��� datetime null,
					  ���� datetime null,
					  ����_���������_��������_��� datetime null,
					  �������_�������_���� varchar(255),
					  Section_ID int,
					  ���_��_��_�_������� int,
					  �������_����_���� varchar(255))
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
-- �����:
--
--		��������� ������
--
-- ���� ��������:
--
--		������, 2011
--
-- ��������:
--
--		�������� �� ��� (�4)
--
-- ======================================================================================

CREATE FUNCTION [dbo].[usf2_NSI_Export_SectionTICount] ()	
RETURNS @table TABLE (��� varchar(255),
                      ������������_����� varchar(255),
					  ���_���_���_������� varchar(255),
					  ������������_��������_��� varchar(255),
					  ���_������� tinyint,
					  ���������������_�����_��� varchar(255) null,
					  ����_�����������_��� datetime null,
					  ����_����������_�_����_��� datetime null,
					  ����_��������_��� datetime null,
					  Section_ID int,
					  ���_��_���������_��_��_��_��� int,
					  ���_��_���������_��_��_��_����������� int,
					  ���_��_�����������_��_��_��_��� int,
					  ���_��_�����������_��_��_��_����������� int)
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
-- �����:
--
--		��������� ������
--
-- ���� ��������:
--
--		������, 2011
--
-- ��������:
--
--		�������� �� ��� (�5)
--
-- ======================================================================================


CREATE FUNCTION [dbo].[usf2_NSI_Export_SectionTICountByPS] ()
RETURNS @table TABLE (��� varchar(255),
                      ������������_����� varchar(255),
					  ���_��_��� varchar(255),
					  ������������_�������� varchar(255),
					  ���_������� varchar(255),
					  ���������������_����� varchar(255) null,
					  ����_����������� datetime null,
					  ����_����������_�_���� datetime null,
					  ����_�������� datetime null,
					  ������������_�� varchar(255),
					  ��_������� float,
					  ���_��_�� int,
					  ���__����� varchar(255),
					  PS_ID int,
					  ���_��_���������_��_��_�� int,
					  ���_��_�����������_��_��_�� int)
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
			   '���������',
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
	--����� ��������� �������� 
	select distinct ps_id, Section_id from info_ti ti 
	join dbo.Info_Section_Description2 isd on ti.tp_id = isd.tp_id  
	union 
	--��������� ����� ������� 
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
-- �����:
--
--		��������� ������
--
-- ���� ��������:
--
--		������, 2011
--
-- ��������:
--
--		�������� �� ��� (�6)
--
-- ======================================================================================

CREATE FUNCTION [dbo].[usf2_NSI_Export_Full_TI]
(
	@PS_ID int,
	@IsActual bit
)
RETURNS @table TABLE (��� varchar(255),
                      ������������_����� varchar(255),
					  ������������_�� varchar(255),
					  ������������_�� varchar(255),
					  ���_�����_������_��_�� varchar(255),
					  ���_��_��� varchar(255),					  
					  ����������_�� float,
					  ���_����� varchar(255),
					  ���_��� varchar(255),
					  ��������_�����_�� bit,
					  ������������_�� varchar(255),
					  ������������_������� varchar(255),
					  ���_�� varchar(255),
					  ��������_�� varchar(255),
					  �����_�� varchar(255),
					  ���������_����������� float,
					  �� float,
					  �� float,
					  �����������_�� float,
					  ��������_�� bit,					
					  ��_�������_�_������� bit,					  
					  �����_�������� varchar(255),
					  ���_�����_��_�_�������� varchar(255),
					  ����_��������_�������� datetime,
					  ��_��������_������������� bit,
					  �������������_���_����� varchar(255),
					  ����������_�����������_�����������_���������_��_�������� bit,
					  ����������_���������_�������� bit,
					  ����������_���������_�� bit,
					  ����������_���������_�N bit,
					  ������������_����_��� varchar(255),
					  �����_����_��� varchar(255),
					  �����_����������_�������� varchar(255),
					  ���_����������_�������� varchar(255),
					  �����_������������_�������� varchar(255),
					  ���_������������_�������� varchar(255),
					  �����_������������_�������� varchar(255),
					  ���_������������_�������� varchar(255))
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
					WHEN 1 THEN '������������'
					ELSE '�����������'
			   END,
			   TI.EPP_ID,
			   TI.IsSmallTI,
			   TP.StringName,
			   S.SectionName,
			   DMT.MeterTypeName,
			   CASE ISNULL(TI.AIATSCode,1)
					WHEN 1 THEN '�� ����������'
					ELSE '����������'
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
					WHEN 1 THEN '������������'
					ELSE '�����������'
			   END,
			   PSP.FinishDateTime,
			   CASE
					WHEN PL.PerspectiveMeteringType IS NULL THEN 0
					ELSE 1
			   END,
			   CASE PL.PerspectiveMeteringType
					WHEN 0 THEN '���������'
					WHEN 1 THEN '�����������'
					WHEN 2 THEN '�����������'
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
-- �����:
--
--		��������� ������
--
-- ���� ��������:
--
--		������, 2011
--
-- ��������:
--
--		�������� �� ��� (�7)
--
-- ======================================================================================

CREATE FUNCTION [dbo].[usf2_NSI_Export_Full_TP]
(
	@Section_ID int,
	@IsActual bit
)
RETURNS @table TABLE (��� varchar(255),
                      ������������_����� varchar(255),
					  ������������_������� varchar(255),
					  ������������_�� varchar(255),
					  ����������_�� int,
					  ���_�����_�������� tinyint,					  
					  ���_���_��� varchar(255))
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
RETURNS @table TABLE (��� varchar(255),
                      ������������_����� varchar(255),
					  ������������_�� varchar(255),
					  ������������_�� varchar(255),
					  ���_�����_������_��_�� varchar(255),
					  ���_��_��� varchar(255),					  
					  ����������_�� float,
					  ���_����� varchar(255),
					  ���_��� varchar(255),
					  ��������_�����_�� bit,
					  ������������_�� varchar(255),
					  ������������_������� varchar(255),
					  ���_�� varchar(255),
					  ��������_�� varchar(255),
					  �����_�� varchar(255),
					  ���������_����������� float,
					  �� float,
					  �� float,
					  �����������_�� float,
					  ��������_�� bit,					
					  ��_�������_�_������� bit,					  
					  �����_�������� varchar(255),
					  ���_�����_��_�_�������� varchar(255),
					  ����_��������_�������� datetime,
					  ��_��������_������������� bit,
					  �������������_���_����� varchar(255),
					  ����������_�����������_�����������_���������_��_�������� bit,
					  ����������_���������_�������� bit,
					  ����������_���������_�� bit,
					  ����������_���������_�N bit,
					  ������������_����_��� varchar(255),
					  �����_����_��� varchar(255),
					  �����_����������_�������� varchar(255),
					  ���_����������_�������� varchar(255),
					  �����_������������_�������� varchar(255),
					  ���_������������_�������� varchar(255),
					  �����_������������_�������� varchar(255),
					  ���_������������_�������� varchar(255))
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
					WHEN 1 THEN '������������'
					ELSE '�����������'
			   END,
			   TI.EPP_ID,
			   TI.IsSmallTI,
			   TP.StringName,
			   S.SectionName,
			   DMT.MeterTypeName,
			   CASE ISNULL(TI.AIATSCode,1)
					WHEN 1 THEN '�� ����������'
					ELSE '����������'
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
					WHEN 1 THEN '������������'
					ELSE '�����������'
			   END,
			   PSP.FinishDateTime,
			   CASE
					WHEN PL.PerspectiveMeteringType IS NULL THEN 0
					ELSE 1
			   END,
			   CASE PL.PerspectiveMeteringType
					WHEN 0 THEN '���������'
					WHEN 1 THEN '�����������'
					WHEN 2 THEN '�����������'
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
					WHEN 1 THEN '������������'
					ELSE '�����������'
			   END,
			   NULL,
			   NULL,
			   TP.StringName,
			   S.SectionName,
			   NULL,
			   CASE ISNULL(TI.AIATSCode,1)
					WHEN 1 THEN '�� ����������'
					ELSE '����������'
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
