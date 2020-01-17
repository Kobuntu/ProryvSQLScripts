set ANSI_NULLS ON
set QUOTED_IDENTIFIER ON
go
if exists (select 1
          from sysobjects
          where  id = object_id('usf2_UserRights2ToObjectRightType')
          and type in ('IF', 'FN', 'TF'))
   drop function usf2_UserRights2ToObjectRightType
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
--		Получаем права пользователя в виде EnumObjectRightType
--
-- ======================================================================================
CREATE FUNCTION [dbo].[usf2_UserRights2ToObjectRightType]
(
	@userId varchar(22), --Идентификатор пользователя
	@objectId varchar(255), -- Идентификатор объекта
	@objectTypeName varchar(255) -- Тип объекта строковый (в соответствии с полем ObjectTypeName таблицы Expl_Users_DBObjects)
)
RETURNS bigint
AS
BEGIN

declare @ObjectRights int;
 set @ObjectRights = 0;
 select @ObjectRights = @ObjectRights | 
 case when Item is null OR Item = '' then 0
 else 
	case left(Item, charindex(',', Item) - 1)
	 --Должно в результате соответствовать EnumObjectRightType
	 when '6D95CECF-327A-408b-96E7-AF8EF27C0F64' then 1 --SeeDbObjects
	 when '5253DF62-DBDD-4C83-8F11-3E4A6EBEE9E4' then 2 --EditReportsForObjectClasses
	 when '5A88CCC3-08AA-454D-B150-F177A7E65EB8' then 4 --EditReports
	 when '3A379E16-330D-11DE-AAF9-EE4556D89593' then 8 --ReplaceCommercialData
	 when '4350C391-80E3-4791-931B-DFD265FBDBB7' then 16 --EditMonthApproximation
	 when '7A438340-B4C6-4d51-8712-B25A82F66656' then 32 --EditSection
	 when '4EDFBB7C-2B78-434b-B220-D0B4E2B03A33' then 64 --EditRoundaboutSwitch
	 when '2EC883F2-470F-4f7b-8BA7-041281E6A5FC' then 128 --ViewReports
	 when '65C7760D-336A-4040-9BCA-898E0FAA8EBB' then 256 --EditFormulas

	 when '4985A42F-7A98-4ED6-ABC1-B81C2A1FB0B5' then 512 --ViewDataSection
	 when '8E5E640D-928B-41CA-8370-764755219EB6' then 1024 --SendDirectMeterCommands
	 when '01646B55-65FE-4a63-B824-F0169DDB07C5' then 2048 --ViewCollectingMonitor
	 when 'CBBB7316-9875-44B8-9178-E7B6603B6C72' then 4096 --ViewDocument
	 when '4236D46D-3773-4ABC-BFFC-0082A8647B51' then 8192 --SendPowerManagmentCommands
	 when 'E4698C96-1BF3-45D7-BEF4-D2EBC5FC07F3' then 16384 --ViewDataJuridicalPerson

	 when 'DE976463-A193-453D-B42E-FDFF43236F34' then 32768 --EditDocument
	 when 'D1B5533F-E284-45a8-B86B-79011521941F' then 65536 --ViewCommercialData
	 when '48BE4C3B-2C74-4ec0-B5C4-1897220F9F6D' then 131072 --EditStructure
	 when 'DE976463-A193-453D-B42E-FDFF43236F35' then 262144 --EditNSI
	 when 'C48B2B9D-48A8-49B0-BB1E-33044F74182F' then 524288 --ManualReadRequest
	 when '537DBB80-DCE2-4387-B8C3-A6C29F41F39B' then 1048576 --ManualReadRequestUSPD

	 --Добавляем по мере необходимости
	 else 0
	end
 end
 from dbo.usf2_Utils_SplitString(dbo.usf2_UserHaveRights(@userId, @objectId, @objectTypeName), '|')

 return @ObjectRights

END
go
grant EXECUTE on usf2_UserRights2ToObjectRightType to [UserCalcService]
go