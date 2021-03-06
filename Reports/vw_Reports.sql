if exists (select 1
          from sysobjects
          where  id = object_id('vw_Reports')
          and type in ('V'))
   drop view vw_Reports
go

-- ======================================================================================
-- Автор:
--
--		Малышев Игорь
--
-- Дата создания:
--
--		Ноябрь, 2018
--
-- Описание:
--
--		Все отчеты для всех пользователей
--
-- ======================================================================================
create VIEW [dbo].[vw_Reports]
AS
--Универсальные отчеты
SELECT r.Report_UN AS Un, r.StringName AS Name, 1 AS ReportGroup, CASE WHEN u.UserRole = 1 THEN '9223372036854775807' ELSE dbo.usf2_UserRights2ToObjectRightType(u.[User_ID], r.Report_UN, 'Info_Report_Stimul') END AS ObjectRights, 
CAST(1 AS bit) AS IsReportSupportBusinessObjects, CAST(1 AS bit) AS IsReportSupportExport, CAST(1 AS bit) AS IsReportSupportPreview,
(select UserName from Expl_Users where User_ID = r.User_ID) as UserName,
u.[User_ID],
r.CreateDateTime AS ApplyDateTime, cast(null as tinyint) as IntegratedReportType,
null as Object_ID, null as ObjectTypeName
FROM            dbo.Expl_Users AS u CROSS JOIN dbo.Info_Report_Stimul AS r
WHERE        (NOT (u.Deleted = 1))
union all
--Отчеты привязанные к объектам
select r.Report_UN as [Un], r.StringName as [Name], 0 as [ReportGroup], 
CASE WHEN u.UserRole = 1 THEN '9223372036854775807' ELSE dbo.usf2_UserRights2ToObjectRightType(u.User_ID, rights.Object_ID, rights.ObjectTypeName) END AS ObjectRights,
cast(1 as bit) as IsReportSupportBusinessObjects,
cast(1 as bit) as IsReportSupportExport,
cast(1 as bit) as IsReportSupportPreview,
(select UserName from Expl_Users where User_ID = r.User_ID) as UserName,
u.[User_ID],
r.CreateDateTime as ApplyDateTime, cast(null as tinyint) as IntegratedReportType,
rights.Object_ID, rights.ObjectTypeName
from Expl_Users u cross join Info_Report r
outer apply usf2_FreeHierarchyTreeDescriptionTypeAndId(r.HierLev1_ID, r.HierLev2_ID, r.HierLev3_ID, r.PS_ID, 
	null, null, r.Section_ID, null, null, null, r.JuridicalPerson_ID, r.JuridicalPersonContract_ID,
	null, null, null, null, null, r.FreeHierItem_ID) rights
where (not u.Deleted=1)
GO

   grant select on vw_Reports to [UserCalcService]
go