if exists (select 1
          from sysobjects
          where  id = object_id('usp2_UA_DataRead')
          and type in ('P','PC'))
   drop procedure usp2_UA_DataRead
go
if exists (select 1
          from sysobjects
          where  id = object_id('usp2_UA_Refs')
          and type in ('P','PC'))
   drop procedure usp2_UA_Refs
go
if exists (select 1
          from sysobjects
          where  id = object_id('usp2_UA_GetNodes')
          and type in ('P','PC'))
   drop procedure usp2_UA_GetNodes
go
if exists (select 1
          from sysobjects
          where  id = object_id('usf2_UA_Types')
          and type in ('IF', 'FN', 'TF'))
   drop function usf2_UA_Types
go

--Устаревшие процедуры
if exists (select 1
          from sysobjects
          where  id = object_id('usf2_UA_Refs')
          and type in ('IF', 'FN', 'TF'))
   drop function usf2_UA_Refs
go

if exists (select 1
          from sysobjects
          where  id = object_id('dbo.usp2_CheckRight_ApplicationModulesVisibility')
          and type in ('P','PC'))
   drop procedure dbo.usp2_CheckRight_ApplicationModulesVisibility
go

--Обновляем тип
--Удаляем если есть
IF  EXISTS (SELECT * FROM sys.types st JOIN sys.schemas ss ON st.schema_id = ss.schema_id WHERE st.name = N'BigintType' AND ss.name = N'dbo')
DROP TYPE [dbo].[BigintType]
-- Пересоздаем заново
CREATE TYPE [dbo].[BigintType] AS TABLE 
(
	Id bigint NOT NULL
)
go

grant EXECUTE on TYPE::BigintType to [UserCalcService]
go
grant EXECUTE on TYPE::BigintType to UserDeclarator
go
grant EXECUTE on TYPE::BigintType to UserImportService
go
grant EXECUTE on TYPE::BigintType to UserExportService
go

-- ======================================================================================
-- Автор:
--
--		Малышев Игорь
--
-- Дата создания:
--
--		Август 2014
--
-- Описание:
--
--		Построение дерева типов OPC UA
--
-- ======================================================================================
create FUNCTION [dbo].[usf2_UA_Types]
(
	@NodeIDs BigintType READONLY --Список идентификаторов узлов
)
RETURNS @ret TABLE
(
	[UATypeNode_ID] int NOT NULL --Тип
)
as
begin
		with recurs (UATypeNode_ID) as
		(
				select UATypeNode_ID
				from UA_Nodes where UANode_ID in (select Id from @NodeIDs)
				and UATypeNode_ID is not null
				union all
				select t.UATypeNode_ID
				from UA_Nodes t 
				join recurs c on c.UATypeNode_ID = t.UANode_ID
				where t.UATypeNode_ID is not null
		)

		insert into @ret 
		select UATypeNode_ID from recurs
	return
end
go
   
grant select on usf2_UA_Types to [UserCalcService]
go
grant select on usf2_UA_Types to UserDeclarator
go
grant select on usf2_UA_Types to UserImportService
go
grant select on usf2_UA_Types to UserExportService
go

-- ======================================================================================
-- Автор:
--
--		Малышев Игорь
--
-- Дата создания:
--
--		Апрель, 2014
--
-- Описание:
--
--		Список зависимых узлов
--
-- ======================================================================================
create proc [dbo].[usp2_UA_Refs]
(
	@nodeIds dbo.BigintType READONLY, --Список идентификаторов узлов
	@isForward bit, -- Раскручиваемое направление (от 1 - ищем зависимые узлы )
	@maxLevel int, -- Максимальный уровень раскрутки
	@pathDelimer nvarchar(1) = '\' -- Разделитель объектов в стоке пути
)
AS
BEGIN 

set nocount on
set quoted_identifier, ansi_nulls, ansi_warnings, arithabort, concat_null_yields_null, ansi_padding on
set numeric_roundabort off
set transaction isolation level read uncommitted

--Переменные
DECLARE
	@UAServer_ID int, --Сервер
	@FromNode_ID bigint, -- От чего идем
	@ToNode_ID bigint, -- К чему идем
	@UAIsForward bit, --Информативно был переворот или нет
	@UAReferenceType int, --
	@Lev int, --Уровень вложенности
	@UANode_ID bigint,
	@Name nvarchar(max),
	@ParentIds varchar(max)

	set @Lev = 0;
	
	--Результирующая таблица
	create table #res
	(
		[UANode_ID] bigint NOT NULL, -- Узел
		[UAServer_ID] int NOT NULL, --Сервер
		[FromNode_ID] bigint NOT NULL, -- От чего идем
		[ToNode_ID] bigint NOT NULL, -- К чему идем
		[UAIsForward] bit not null, --Информативно был переворот или нет (не влияет на FromNode_ID и ToNode_ID)
		UAReferenceType int not NULL, -- Тип ссылки
		Lev int NOT NULL, --Уровень вложенности
		[TreePath] nvarchar(max), --Путь до рута,
		[ParentIds] varchar(max) --Идентификаторы родителей от рута до объекта,
		PRIMARY KEY CLUSTERED (Lev, [FromNode_ID], [ToNode_ID], [UANode_ID], [UAIsForward], [UAReferenceType]) WITH (IGNORE_DUP_KEY = ON)
	);

	create table #dict
	(
		[UANode_ID] bigint NOT NULL, -- Узел
		[FromNode_ID] bigint NOT NULL, -- От чего идем
		[ToNode_ID] bigint NOT NULL, -- К чему идем
		[UAReferenceType] int not NULL, -- Тип ссылки
		PRIMARY KEY CLUSTERED ([UANode_ID], [FromNode_ID], [ToNode_ID], [UAReferenceType]) 
	);

	declare @inserted int;

	if (@isForward = 1) begin 
		insert into #res
		select distinct n.Id, UAServer_ID, DestinationUANode_ID, SourceUANode_ID, [UAIsForward],
		ISNULL(dbo.usf2_UA_DetectType(UAReferenceType), -1) as UAReferenceType, @Lev
		,(select top 1 ISNULL(UADisplayNameText, '') from UA_Nodes where UA_Nodes.UANode_ID = SourceUANode_ID) +@pathDelimer 
		+  (select top 1 ISNULL(UADisplayNameText, '') from UA_Nodes where UA_Nodes.UANode_ID = DestinationUANode_ID),
		ltrim(str(SourceUANode_ID)) + ',' + ltrim(str(DestinationUANode_ID))
		from @nodeIds n
		join UA_Refs c on SourceUANode_ID = n.Id and DestinationUANode_ID is not null
		where UAReferenceType <> 'i=40'
	end else begin
		insert into #res
		select distinct n.Id, UAServer_ID, DestinationUANode_ID, SourceUANode_ID, [UAIsForward],
		ISNULL(dbo.usf2_UA_DetectType(UAReferenceType), -1) as UAReferenceType, @Lev
		,(select top 1 ISNULL(UADisplayNameText, '') from UA_Nodes where UA_Nodes.UANode_ID = SourceUANode_ID) +@pathDelimer 
		+  (select top 1 ISNULL(UADisplayNameText, '') from UA_Nodes where UA_Nodes.UANode_ID = DestinationUANode_ID),
		ltrim(str(SourceUANode_ID)) + ',' + ltrim(str(DestinationUANode_ID))
		from @nodeIds n
		join UA_Refs c on DestinationUANode_ID = n.Id and SourceUANode_ID is not null
		where UAReferenceType <> 'i=40'
	end
	
	set @inserted = @@ROWCOUNT;

	while (@inserted > 0 and @Lev + 1 < @maxLevel)
	begin
		if (@isForward = 1)
			declare t cursor LOCAL STATIC FORWARD_ONLY READ_ONLY for select distinct r.UANode_ID, c.UAServer_ID,DestinationUANode_ID, SourceUANode_ID, c.[UAIsForward],
			ISNULL(dbo.usf2_UA_DetectType(c.UAReferenceType), -1) as UAReferenceType, '', ''
			from #res r
			join UA_Refs c on c.SourceUANode_ID = r.FromNode_ID and c.DestinationUANode_ID is not null
			where r.Lev = @Lev and r.FromNode_ID >= 0 and c.UAReferenceType <> 'i=40'
		else
			declare t cursor LOCAL STATIC FORWARD_ONLY READ_ONLY for select distinct r.UANode_ID,c.UAServer_ID,DestinationUANode_ID, SourceUANode_ID, c.[UAIsForward],
			ISNULL(dbo.usf2_UA_DetectType(c.UAReferenceType), -1) as UAReferenceType, ISNULL(n.UADisplayNameText, '') + @pathDelimer + r.[TreePath],
			ltrim(str(n.UANode_ID)) + ',' + r.ParentIds
			from #res r
			join UA_Refs c on c.DestinationUANode_ID = r.ToNode_ID and c.SourceUANode_ID is not null
			join UA_Nodes n on n.UANode_ID = c.SourceUANode_ID
			where r.Lev = @Lev and r.ToNode_ID >= 0 and c.UAReferenceType <> 'i=40'

		open t;
		
		set @inserted = 0;
		FETCH NEXT FROM t into @UANode_ID,@UAServer_ID,
			@FromNode_ID, @ToNode_ID, @UAIsForward,
			@UAReferenceType, @Name, @ParentIds
		WHILE @@FETCH_STATUS = 0
		BEGIN

			if (not exists(select top 1 1 from #dict where UANode_ID = @UANode_ID and FromNode_ID=@FromNode_ID and ToNode_ID=@ToNode_ID and UAReferenceType=@UAReferenceType))
			begin 
				insert into #res
				values (@UANode_ID, @UAServer_ID, @FromNode_ID, @ToNode_ID, @UAIsForward,@UAReferenceType, @Lev + 1, @Name, @ParentIds)

				set @inserted = @inserted + @@ROWCOUNT

				insert into #dict values (@UANode_ID, @FromNode_ID, @ToNode_ID, @UAReferenceType);
			end
			FETCH NEXT FROM t into @UANode_ID,@UAServer_ID,
			@FromNode_ID, @ToNode_ID, @UAIsForward,
			@UAReferenceType, @Name, @ParentIds
		
		end;
		CLOSE t
		DEALLOCATE t

		set @Lev = @Lev + 1;
	end;

	select * from #res

	drop table #dict
	drop table #res
end
go
   grant execute on usp2_UA_Refs to [UserCalcService]
go

grant execute on usp2_UA_Refs to UserDeclarator
go
grant execute on usp2_UA_Refs to UserImportService
go
grant execute on usp2_UA_Refs to UserExportService
go

-- ======================================================================================
-- Автор:
--
--		Малышев Игорь
--
-- Дата создания:
--
--		Апрель, 2014
--
-- Описание:
--
--		Информация по узлу OPC UA
--
-- ======================================================================================
create proc [dbo].[usp2_UA_GetNodes]
(	
	@NodeIDs dbo.BigintType READONLY, --Список идентификаторов узлов
	@IsForward bit, --Раскручиваемое направление (от Destination к Source или иначе )
	@MaxNodeLevel int -- Максимальный уровень раскрутки
)
AS
BEGIN 
			set nocount on
			set quoted_identifier, ansi_nulls, ansi_warnings, arithabort, concat_null_yields_null, ansi_padding on
			set numeric_roundabort off
			set transaction isolation level read uncommitted

			create table #tmp
			(
				[UANode_ID] bigint NOT NULL, -- Узел
				[UAServer_ID] int NOT NULL, --Сервер
				[FromNode_ID] bigint NOT NULL, -- От чего идем
				[ToNode_ID] bigint NOT NULL, -- К чему идем
				[UAIsForward] bit not null, --Информативно был переворот или нет
				UAReferenceType int NULL, --
				Lev int NOT NULL, --Уровень вложенности
				[TreePath] nvarchar(max), --Путь до рута
				[ParentIds] varchar(max) --Идентификаторы родителей от рута до объекта
			);

			insert into #tmp
			exec usp2_UA_Refs @NodeIDs, @IsForward, @MaxNodeLevel

			declare @allNodes dbo.BigintType;

			insert into @allNodes 
			select distinct Id from
				(
					select distinct ToNode_ID as id from #tmp
					union 
					select distinct FromNode_ID as id from #tmp
					union 
					select Id from @NodeIDs
				) a

			--Дополнительно вытаскиваем типы узлов,для отображения
			insert into @allNodes
			select distinct UATypeNode_ID from usf2_UA_Types(@allNodes)

			--Информация по узлам
			select u.UANode_ID as UANodeId, u.UAServer_ID as UAServerId, u.TIType, u.UABrowseNameName as BrowseName,
			u.UADisplayNameText as DisplayName, UANodeClass_ID, UABaseAttributeDescription, UANodeClass_ID, UATypeNode_ID, UATypeDefinition,u.UANodeID AS OpcId
			from @allNodes a
			join UA_Nodes u on u.UANode_ID = a.Id
			
			
			--Связи
			select * from #tmp order by ToNode_ID, Lev

			drop table #tmp

end
go
   grant EXECUTE on usp2_UA_GetNodes to [UserCalcService]
go
grant execute on usp2_UA_GetNodes to UserDeclarator
go
grant execute on usp2_UA_GetNodes to UserImportService
go
grant execute on usp2_UA_GetNodes to UserExportService
go

if exists (select 1
          from sysobjects
          where  id = object_id('sf_UA_GETLASTMODIFIEDVALUEDATETIME')
          and type in ('IF', 'FN', 'TF'))
   drop function sf_UA_GETLASTMODIFIEDVALUEDATETIME
go

-- ======================================================================================
-- Автор:
--
--		Карташев Александр
--
-- Дата создания:
--
--		Июнь, 2015
--
-- Описание:
--
--		Чтение последнего времени изменния состояния занчения OPC UA
--
-- ======================================================================================
create function [dbo].[sf_UA_GETLASTMODIFIEDVALUEDATETIME](@UANODE_ID bigint,@ValueBoolean bit ,
	@ValueByteString varbinary(max) ,
	@ValueDateTime DateTime ,
	@ValueFloat float ,
	@ValueGUID uniqueidentifier ,
	@ValueInt16 smallint ,
	@ValueInt32 int ,
	@ValueInt64 bigint ,
	@ValueString nvarchar(max) ,
	@ValueXMLElement XML ,
	@ValueUANode_ID bigint ,
	@ValueNodeID varchar(max) ,
	--Массив ?
	@ArrayFormat tinyint ,
	@ArrayValue varbinary(max) ,@archiveTable nvarchar(max))
returns DATETIME
as
begin



IF @archiveTable ='UA_Data_Current_Boolean_1'
BEGIN
   return (SELECT TOP 1 a.SourceTimeStamp  FROM UA_Data_Archive_Boolean_1  as a WHERE a.UANode_ID = @UANODE_ID AND a.Value = @ValueBoolean 
   and a.SourceTimeStamp>( (SELECT TOP 1 a.SourceTimeStamp  FROM UA_Data_Archive_Boolean_1  as a WHERE a.UANode_ID = @UANODE_ID AND a.Value <> @ValueBoolean ORDER BY a.SourceTimeStamp DESC))
    ORDER BY a.SourceTimeStamp ASC)
END
IF @archiveTable ='UA_Data_Current_Int16_1'
BEGIN
   return (SELECT TOP 1 a.SourceTimeStamp  FROM UA_Data_Archive_Int16_1  as a WHERE a.UANode_ID = @UANODE_ID AND a.Value = @ValueInt16 
   and a.SourceTimeStamp>( (SELECT TOP 1 a.SourceTimeStamp  FROM UA_Data_Archive_Int16_1  as a WHERE a.UANode_ID = @UANODE_ID AND a.Value <> @ValueInt16 ORDER BY a.SourceTimeStamp DESC))
    ORDER BY a.SourceTimeStamp ASC)  
END
IF @archiveTable ='UA_Data_Current_Int32_1'
BEGIN
return (SELECT TOP 1 a.SourceTimeStamp  FROM UA_Data_Archive_Int32_1  as a WHERE a.UANode_ID = @UANODE_ID AND a.Value = @ValueInt32 
   and a.SourceTimeStamp>( (SELECT TOP 1 a.SourceTimeStamp  FROM UA_Data_Archive_Int32_1  as a WHERE a.UANode_ID = @UANODE_ID AND a.Value <> @ValueInt32 ORDER BY a.SourceTimeStamp DESC))
    ORDER BY a.SourceTimeStamp ASC)  
END
IF @archiveTable ='UA_Data_Current_Int64_1'
BEGIN
return (SELECT TOP 1 a.SourceTimeStamp  FROM UA_Data_Archive_Int64_1  as a WHERE a.UANode_ID = @UANODE_ID AND a.Value = @ValueInt64 
   and a.SourceTimeStamp>( (SELECT TOP 1 a.SourceTimeStamp  FROM UA_Data_Archive_Int64_1  as a WHERE a.UANode_ID = @UANODE_ID AND a.Value <> @ValueInt64 ORDER BY a.SourceTimeStamp DESC))
    ORDER BY a.SourceTimeStamp ASC)  
END
IF @archiveTable ='UA_Data_Current_Float_1'
BEGIN
return (SELECT TOP 1 a.SourceTimeStamp  FROM UA_Data_Archive_Float_1  as a WHERE a.UANode_ID = @UANODE_ID AND a.Value = @ValueFloat 
   and a.SourceTimeStamp>( (SELECT TOP 1 a.SourceTimeStamp  FROM UA_Data_Archive_Float_1  as a WHERE a.UANode_ID = @UANODE_ID AND a.Value <> @ValueFloat ORDER BY a.SourceTimeStamp DESC))
    ORDER BY a.SourceTimeStamp ASC)  
END
IF @archiveTable ='UA_Data_Current_String_1'
BEGIN
return (SELECT TOP 1 a.SourceTimeStamp  FROM UA_Data_Archive_String_1  as a WHERE a.UANode_ID = @UANODE_ID AND a.Value = @ValueString 
   and a.SourceTimeStamp>( (SELECT TOP 1 a.SourceTimeStamp  FROM UA_Data_Archive_String_1  as a WHERE a.UANode_ID = @UANODE_ID AND a.Value <> @ValueString ORDER BY a.SourceTimeStamp DESC))
    ORDER BY a.SourceTimeStamp ASC)  
END
IF @archiveTable ='UA_Data_Current_DateTime_1'
BEGIN
return (SELECT TOP 1 a.SourceTimeStamp  FROM UA_Data_Archive_DateTime_1  as a WHERE a.UANode_ID = @UANODE_ID AND a.Value = @ValueDateTime 
   and a.SourceTimeStamp>( (SELECT TOP 1 a.SourceTimeStamp  FROM UA_Data_Archive_DateTime_1  as a WHERE a.UANode_ID = @UANODE_ID AND a.Value <> @ValueDateTime ORDER BY a.SourceTimeStamp DESC))
    ORDER BY a.SourceTimeStamp ASC)  
END
IF @archiveTable ='UA_Data_Current_GUID_1'
BEGIN
return (SELECT TOP 1 a.SourceTimeStamp  FROM UA_Data_Archive_GUID_1  as a WHERE a.UANode_ID = @UANODE_ID AND a.Value = @ValueGUID 
   and a.SourceTimeStamp>( (SELECT TOP 1 a.SourceTimeStamp  FROM UA_Data_Archive_GUID_1  as a WHERE a.UANode_ID = @UANODE_ID AND a.Value <> @ValueGUID ORDER BY a.SourceTimeStamp DESC))
    ORDER BY a.SourceTimeStamp ASC)  
END
IF @archiveTable ='UA_Data_Current_ByteString_1'
BEGIN
return (SELECT TOP 1 a.SourceTimeStamp  FROM UA_Data_Archive_ByteString_1  as a WHERE a.UANode_ID = @UANODE_ID AND a.Value = @ValueByteString 
   and a.SourceTimeStamp>( (SELECT TOP 1 a.SourceTimeStamp  FROM UA_Data_Archive_ByteString_1  as a WHERE a.UANode_ID = @UANODE_ID AND a.Value <> @ValueByteString ORDER BY a.SourceTimeStamp DESC))
    ORDER BY a.SourceTimeStamp ASC)  
END
IF @archiveTable ='UA_Data_Current_XMLElement_1'
BEGIN
return (SELECT TOP 1 a.SourceTimeStamp  FROM UA_Data_Archive_XMLElement_1  as a WHERE a.UANode_ID = @UANODE_ID AND  Convert(nvarchar(max), a.Value) = Convert(nvarchar(max),@ValueXMLElement)
   and a.SourceTimeStamp>( (SELECT TOP 1 a.SourceTimeStamp  FROM UA_Data_Archive_XMLElement_1  as a WHERE a.UANode_ID = @UANODE_ID AND  Convert(nvarchar(max), a.Value) <> Convert(nvarchar(max),@ValueXMLElement) ORDER BY a.SourceTimeStamp DESC))
    ORDER BY  a.SourceTimeStamp DESC)  
END
IF @archiveTable ='UA_Data_Current_NodeID_1'
BEGIN
return (SELECT TOP 1 a.SourceTimeStamp  FROM UA_Data_Archive_NodeID_1  as a WHERE a.UANode_ID = @UANODE_ID AND a.ValueNodeID = @ValueUANode_ID 
   and a.SourceTimeStamp>( (SELECT TOP 1 a.SourceTimeStamp  FROM UA_Data_Archive_NodeID_1  as a WHERE a.UANode_ID = @UANODE_ID AND a.ValueNodeID <> @ValueUANode_ID ORDER BY a.SourceTimeStamp DESC))
    ORDER BY a.SourceTimeStamp ASC)  
END
IF @archiveTable ='UA_Data_Current_ExpandedNodeID_1'
BEGIN
return (SELECT TOP 1 a.SourceTimeStamp  FROM UA_Data_Archive_ExpandedNodeID_1  as a WHERE a.UANode_ID = @UANODE_ID AND a.ValueNodeID = @ValueNodeID 
   and a.SourceTimeStamp>( (SELECT TOP 1 a.SourceTimeStamp  FROM UA_Data_Archive_ExpandedNodeID_1  as a WHERE a.UANode_ID = @UANODE_ID AND a.ValueNodeID <> @ValueNodeID ORDER BY a.SourceTimeStamp DESC))
    ORDER BY a.SourceTimeStamp ASC)  
END
IF @archiveTable ='UA_Data_Current_DateTime_1'
BEGIN
return (SELECT TOP 1 a.SourceTimeStamp  FROM UA_Data_Archive_DateTime_1  as a WHERE a.UANode_ID = @UANODE_ID AND a.Value = @ValueDateTime 
   and a.SourceTimeStamp>( (SELECT TOP 1 a.SourceTimeStamp  FROM UA_Data_Archive_DateTime_1  as a WHERE a.UANode_ID = @UANODE_ID AND a.Value <> @ValueDateTime ORDER BY a.SourceTimeStamp DESC))
    ORDER BY a.SourceTimeStamp ASC)  
END
IF @archiveTable ='UA_Data_Current_Boolean_2'
BEGIN
   return (SELECT TOP 1 a.SourceTimeStamp  FROM UA_Data_Archive_Boolean_2  as a WHERE a.UANode_ID = @UANODE_ID AND a.Value = @ValueBoolean 
   and a.SourceTimeStamp>( (SELECT TOP 1 a.SourceTimeStamp  FROM UA_Data_Archive_Boolean_2  as a WHERE a.UANode_ID = @UANODE_ID AND a.Value <> @ValueBoolean ORDER BY a.SourceTimeStamp DESC))
    ORDER BY a.SourceTimeStamp ASC)
END
IF @archiveTable ='UA_Data_Current_Int16_2'
BEGIN
   return (SELECT TOP 1 a.SourceTimeStamp  FROM UA_Data_Archive_Int16_2  as a WHERE a.UANode_ID = @UANODE_ID AND a.Value = @ValueInt16 
   and a.SourceTimeStamp>( (SELECT TOP 1 a.SourceTimeStamp  FROM UA_Data_Archive_Int16_2  as a WHERE a.UANode_ID = @UANODE_ID AND a.Value <> @ValueInt16 ORDER BY a.SourceTimeStamp DESC))
    ORDER BY a.SourceTimeStamp ASC)  
END
IF @archiveTable ='UA_Data_Current_Int32_2'
BEGIN
return (SELECT TOP 1 a.SourceTimeStamp  FROM UA_Data_Archive_Int32_2  as a WHERE a.UANode_ID = @UANODE_ID AND a.Value = @ValueInt32 
   and a.SourceTimeStamp>( (SELECT TOP 1 a.SourceTimeStamp  FROM UA_Data_Archive_Int32_2  as a WHERE a.UANode_ID = @UANODE_ID AND a.Value <> @ValueInt32 ORDER BY a.SourceTimeStamp DESC))
    ORDER BY a.SourceTimeStamp ASC)  
END
IF @archiveTable ='UA_Data_Current_Int64_2'
BEGIN
return (SELECT TOP 1 a.SourceTimeStamp  FROM UA_Data_Archive_Int64_2  as a WHERE a.UANode_ID = @UANODE_ID AND a.Value = @ValueInt64 
   and a.SourceTimeStamp>( (SELECT TOP 1 a.SourceTimeStamp  FROM UA_Data_Archive_Int64_2  as a WHERE a.UANode_ID = @UANODE_ID AND a.Value <> @ValueInt64 ORDER BY a.SourceTimeStamp DESC))
    ORDER BY a.SourceTimeStamp ASC)  
END
IF @archiveTable ='UA_Data_Current_Float_2'
BEGIN
return (SELECT TOP 1 a.SourceTimeStamp  FROM UA_Data_Archive_Float_2  as a WHERE a.UANode_ID = @UANODE_ID AND a.Value = @ValueFloat 
   and a.SourceTimeStamp>( (SELECT TOP 1 a.SourceTimeStamp  FROM UA_Data_Archive_Float_2  as a WHERE a.UANode_ID = @UANODE_ID AND a.Value <> @ValueFloat ORDER BY a.SourceTimeStamp DESC))
    ORDER BY a.SourceTimeStamp ASC)  
END
IF @archiveTable ='UA_Data_Current_String_2'
BEGIN
return (SELECT TOP 1 a.SourceTimeStamp  FROM UA_Data_Archive_String_2  as a WHERE a.UANode_ID = @UANODE_ID AND a.Value = @ValueString 
   and a.SourceTimeStamp>( (SELECT TOP 1 a.SourceTimeStamp  FROM UA_Data_Archive_String_2  as a WHERE a.UANode_ID = @UANODE_ID AND a.Value <> @ValueString ORDER BY a.SourceTimeStamp DESC))
    ORDER BY a.SourceTimeStamp ASC)  
END
IF @archiveTable ='UA_Data_Current_DateTime_2'
BEGIN
return (SELECT TOP 1 a.SourceTimeStamp  FROM UA_Data_Archive_DateTime_2  as a WHERE a.UANode_ID = @UANODE_ID AND a.Value = @ValueDateTime 
   and a.SourceTimeStamp>( (SELECT TOP 1 a.SourceTimeStamp  FROM UA_Data_Archive_DateTime_2  as a WHERE a.UANode_ID = @UANODE_ID AND a.Value <> @ValueDateTime ORDER BY a.SourceTimeStamp DESC))
    ORDER BY a.SourceTimeStamp ASC)  
END
IF @archiveTable ='UA_Data_Current_GUID_2'
BEGIN
return (SELECT TOP 1 a.SourceTimeStamp  FROM UA_Data_Archive_GUID_2  as a WHERE a.UANode_ID = @UANODE_ID AND a.Value = @ValueGUID 
   and a.SourceTimeStamp>( (SELECT TOP 1 a.SourceTimeStamp  FROM UA_Data_Archive_GUID_2  as a WHERE a.UANode_ID = @UANODE_ID AND a.Value <> @ValueGUID ORDER BY a.SourceTimeStamp DESC))
    ORDER BY a.SourceTimeStamp ASC)  
END
IF @archiveTable ='UA_Data_Current_ByteString_2'
BEGIN
return (SELECT TOP 1 a.SourceTimeStamp  FROM UA_Data_Archive_ByteString_2  as a WHERE a.UANode_ID = @UANODE_ID AND a.Value = @ValueByteString 
   and a.SourceTimeStamp>( (SELECT TOP 1 a.SourceTimeStamp  FROM UA_Data_Archive_ByteString_2  as a WHERE a.UANode_ID = @UANODE_ID AND a.Value <> @ValueByteString ORDER BY a.SourceTimeStamp DESC))
    ORDER BY a.SourceTimeStamp ASC)  
END
IF @archiveTable ='UA_Data_Current_XMLElement_2'
BEGIN
return (SELECT TOP 1 a.SourceTimeStamp  FROM UA_Data_Archive_XMLElement_2  as a WHERE a.UANode_ID = @UANODE_ID AND  Convert(nvarchar(max), a.Value) = Convert(nvarchar(max),@ValueXMLElement)
   and a.SourceTimeStamp>( (SELECT TOP 1 a.SourceTimeStamp  FROM UA_Data_Archive_XMLElement_2  as a WHERE a.UANode_ID = @UANODE_ID AND  Convert(nvarchar(max), a.Value) <> Convert(nvarchar(max),@ValueXMLElement) ORDER BY a.SourceTimeStamp DESC))
    ORDER BY  a.SourceTimeStamp DESC)  
END
IF @archiveTable ='UA_Data_Current_NodeID_2'
BEGIN
return (SELECT TOP 1 a.SourceTimeStamp  FROM UA_Data_Archive_NodeID_2  as a WHERE a.UANode_ID = @UANODE_ID AND a.ValueNodeID = @ValueUANode_ID 
   and a.SourceTimeStamp>( (SELECT TOP 1 a.SourceTimeStamp  FROM UA_Data_Archive_NodeID_2  as a WHERE a.UANode_ID = @UANODE_ID AND a.ValueNodeID <> @ValueUANode_ID ORDER BY a.SourceTimeStamp DESC))
    ORDER BY a.SourceTimeStamp ASC)  
END
IF @archiveTable ='UA_Data_Current_ExpandedNodeID_2'
BEGIN
return (SELECT TOP 1 a.SourceTimeStamp  FROM UA_Data_Archive_ExpandedNodeID_2  as a WHERE a.UANode_ID = @UANODE_ID AND a.ValueNodeID = @ValueNodeID 
   and a.SourceTimeStamp>( (SELECT TOP 1 a.SourceTimeStamp  FROM UA_Data_Archive_ExpandedNodeID_2  as a WHERE a.UANode_ID = @UANODE_ID AND a.ValueNodeID <> @ValueNodeID ORDER BY a.SourceTimeStamp DESC))
    ORDER BY a.SourceTimeStamp ASC)  
END
IF @archiveTable ='UA_Data_Current_DateTime_2'
BEGIN
return (SELECT TOP 1 a.SourceTimeStamp  FROM UA_Data_Archive_DateTime_2  as a WHERE a.UANode_ID = @UANODE_ID AND a.Value = @ValueDateTime 
   and a.SourceTimeStamp>( (SELECT TOP 1 a.SourceTimeStamp  FROM UA_Data_Archive_DateTime_2  as a WHERE a.UANode_ID = @UANODE_ID AND a.Value <> @ValueDateTime ORDER BY a.SourceTimeStamp DESC))
    ORDER BY a.SourceTimeStamp ASC)  
END
IF @archiveTable ='UA_Data_Current_Boolean_3'
BEGIN
   return (SELECT TOP 1 a.SourceTimeStamp  FROM UA_Data_Archive_Boolean_3  as a WHERE a.UANode_ID = @UANODE_ID AND a.Value = @ValueBoolean 
   and a.SourceTimeStamp>( (SELECT TOP 1 a.SourceTimeStamp  FROM UA_Data_Archive_Boolean_3  as a WHERE a.UANode_ID = @UANODE_ID AND a.Value <> @ValueBoolean ORDER BY a.SourceTimeStamp DESC))
    ORDER BY a.SourceTimeStamp ASC)
END
IF @archiveTable ='UA_Data_Current_Int16_3'
BEGIN
   return (SELECT TOP 1 a.SourceTimeStamp  FROM UA_Data_Archive_Int16_3  as a WHERE a.UANode_ID = @UANODE_ID AND a.Value = @ValueInt16 
   and a.SourceTimeStamp>( (SELECT TOP 1 a.SourceTimeStamp  FROM UA_Data_Archive_Int16_3  as a WHERE a.UANode_ID = @UANODE_ID AND a.Value <> @ValueInt16 ORDER BY a.SourceTimeStamp DESC))
    ORDER BY a.SourceTimeStamp ASC)  
END
IF @archiveTable ='UA_Data_Current_Int32_3'
BEGIN
return (SELECT TOP 1 a.SourceTimeStamp  FROM UA_Data_Archive_Int32_3  as a WHERE a.UANode_ID = @UANODE_ID AND a.Value = @ValueInt32 
   and a.SourceTimeStamp>( (SELECT TOP 1 a.SourceTimeStamp  FROM UA_Data_Archive_Int32_3  as a WHERE a.UANode_ID = @UANODE_ID AND a.Value <> @ValueInt32 ORDER BY a.SourceTimeStamp DESC))
    ORDER BY a.SourceTimeStamp ASC)  
END
IF @archiveTable ='UA_Data_Current_Int64_3'
BEGIN
return (SELECT TOP 1 a.SourceTimeStamp  FROM UA_Data_Archive_Int64_3  as a WHERE a.UANode_ID = @UANODE_ID AND a.Value = @ValueInt64 
   and a.SourceTimeStamp>( (SELECT TOP 1 a.SourceTimeStamp  FROM UA_Data_Archive_Int64_3  as a WHERE a.UANode_ID = @UANODE_ID AND a.Value <> @ValueInt64 ORDER BY a.SourceTimeStamp DESC))
    ORDER BY a.SourceTimeStamp ASC)  
END
IF @archiveTable ='UA_Data_Current_Float_3'
BEGIN
return (SELECT TOP 1 a.SourceTimeStamp  FROM UA_Data_Archive_Float_3  as a WHERE a.UANode_ID = @UANODE_ID AND a.Value = @ValueFloat 
   and a.SourceTimeStamp>( (SELECT TOP 1 a.SourceTimeStamp  FROM UA_Data_Archive_Float_3  as a WHERE a.UANode_ID = @UANODE_ID AND a.Value <> @ValueFloat ORDER BY a.SourceTimeStamp DESC))
    ORDER BY a.SourceTimeStamp ASC)  
END
IF @archiveTable ='UA_Data_Current_String_3'
BEGIN
return (SELECT TOP 1 a.SourceTimeStamp  FROM UA_Data_Archive_String_3  as a WHERE a.UANode_ID = @UANODE_ID AND a.Value = @ValueString 
   and a.SourceTimeStamp>( (SELECT TOP 1 a.SourceTimeStamp  FROM UA_Data_Archive_String_3  as a WHERE a.UANode_ID = @UANODE_ID AND a.Value <> @ValueString ORDER BY a.SourceTimeStamp DESC))
    ORDER BY a.SourceTimeStamp ASC)  
END
IF @archiveTable ='UA_Data_Current_DateTime_3'
BEGIN
return (SELECT TOP 1 a.SourceTimeStamp  FROM UA_Data_Archive_DateTime_3  as a WHERE a.UANode_ID = @UANODE_ID AND a.Value = @ValueDateTime 
   and a.SourceTimeStamp>( (SELECT TOP 1 a.SourceTimeStamp  FROM UA_Data_Archive_DateTime_3  as a WHERE a.UANode_ID = @UANODE_ID AND a.Value <> @ValueDateTime ORDER BY a.SourceTimeStamp DESC))
    ORDER BY a.SourceTimeStamp ASC)  
END
IF @archiveTable ='UA_Data_Current_GUID_3'
BEGIN
return (SELECT TOP 1 a.SourceTimeStamp  FROM UA_Data_Archive_GUID_3  as a WHERE a.UANode_ID = @UANODE_ID AND a.Value = @ValueGUID 
   and a.SourceTimeStamp>( (SELECT TOP 1 a.SourceTimeStamp  FROM UA_Data_Archive_GUID_3  as a WHERE a.UANode_ID = @UANODE_ID AND a.Value <> @ValueGUID ORDER BY a.SourceTimeStamp DESC))
    ORDER BY a.SourceTimeStamp ASC)  
END
IF @archiveTable ='UA_Data_Current_ByteString_3'
BEGIN
return (SELECT TOP 1 a.SourceTimeStamp  FROM UA_Data_Archive_ByteString_3  as a WHERE a.UANode_ID = @UANODE_ID AND a.Value = @ValueByteString 
   and a.SourceTimeStamp>( (SELECT TOP 1 a.SourceTimeStamp  FROM UA_Data_Archive_ByteString_3  as a WHERE a.UANode_ID = @UANODE_ID AND a.Value <> @ValueByteString ORDER BY a.SourceTimeStamp DESC))
    ORDER BY a.SourceTimeStamp ASC)  
END
IF @archiveTable ='UA_Data_Current_XMLElement_3'
BEGIN
return (SELECT TOP 1 a.SourceTimeStamp  FROM UA_Data_Archive_XMLElement_3  as a WHERE a.UANode_ID = @UANODE_ID AND  Convert(nvarchar(max), a.Value) = Convert(nvarchar(max),@ValueXMLElement)
   and a.SourceTimeStamp>( (SELECT TOP 1 a.SourceTimeStamp  FROM UA_Data_Archive_XMLElement_3  as a WHERE a.UANode_ID = @UANODE_ID AND  Convert(nvarchar(max), a.Value) <> Convert(nvarchar(max),@ValueXMLElement) ORDER BY a.SourceTimeStamp DESC))
    ORDER BY  a.SourceTimeStamp DESC)  
END
IF @archiveTable ='UA_Data_Current_NodeID_3'
BEGIN
return (SELECT TOP 1 a.SourceTimeStamp  FROM UA_Data_Archive_NodeID_3  as a WHERE a.UANode_ID = @UANODE_ID AND a.ValueNodeID = @ValueUANode_ID 
   and a.SourceTimeStamp>( (SELECT TOP 1 a.SourceTimeStamp  FROM UA_Data_Archive_NodeID_3  as a WHERE a.UANode_ID = @UANODE_ID AND a.ValueNodeID <> @ValueUANode_ID ORDER BY a.SourceTimeStamp DESC))
    ORDER BY a.SourceTimeStamp ASC)  
END
IF @archiveTable ='UA_Data_Current_ExpandedNodeID_3'
BEGIN
return (SELECT TOP 1 a.SourceTimeStamp  FROM UA_Data_Archive_ExpandedNodeID_3  as a WHERE a.UANode_ID = @UANODE_ID AND a.ValueNodeID = @ValueNodeID 
   and a.SourceTimeStamp>( (SELECT TOP 1 a.SourceTimeStamp  FROM UA_Data_Archive_ExpandedNodeID_3  as a WHERE a.UANode_ID = @UANODE_ID AND a.ValueNodeID <> @ValueNodeID ORDER BY a.SourceTimeStamp DESC))
    ORDER BY a.SourceTimeStamp ASC)  
END
IF @archiveTable ='UA_Data_Current_DateTime_3'
BEGIN
return (SELECT TOP 1 a.SourceTimeStamp  FROM UA_Data_Archive_DateTime_3  as a WHERE a.UANode_ID = @UANODE_ID AND a.Value = @ValueDateTime 
   and a.SourceTimeStamp>( (SELECT TOP 1 a.SourceTimeStamp  FROM UA_Data_Archive_DateTime_3  as a WHERE a.UANode_ID = @UANODE_ID AND a.Value <> @ValueDateTime ORDER BY a.SourceTimeStamp DESC))
    ORDER BY a.SourceTimeStamp ASC)  
END
IF @archiveTable ='UA_Data_Current_Boolean_4'
BEGIN
   return (SELECT TOP 1 a.SourceTimeStamp  FROM UA_Data_Archive_Boolean_4  as a WHERE a.UANode_ID = @UANODE_ID AND a.Value = @ValueBoolean 
   and a.SourceTimeStamp>( (SELECT TOP 1 a.SourceTimeStamp  FROM UA_Data_Archive_Boolean_4  as a WHERE a.UANode_ID = @UANODE_ID AND a.Value <> @ValueBoolean ORDER BY a.SourceTimeStamp DESC))
    ORDER BY a.SourceTimeStamp ASC)
END
IF @archiveTable ='UA_Data_Current_Int16_4'
BEGIN
   return (SELECT TOP 1 a.SourceTimeStamp  FROM UA_Data_Archive_Int16_4  as a WHERE a.UANode_ID = @UANODE_ID AND a.Value = @ValueInt16 
   and a.SourceTimeStamp>( (SELECT TOP 1 a.SourceTimeStamp  FROM UA_Data_Archive_Int16_4  as a WHERE a.UANode_ID = @UANODE_ID AND a.Value <> @ValueInt16 ORDER BY a.SourceTimeStamp DESC))
    ORDER BY a.SourceTimeStamp ASC)  
END
IF @archiveTable ='UA_Data_Current_Int32_4'
BEGIN
return (SELECT TOP 1 a.SourceTimeStamp  FROM UA_Data_Archive_Int32_4  as a WHERE a.UANode_ID = @UANODE_ID AND a.Value = @ValueInt32 
   and a.SourceTimeStamp>( (SELECT TOP 1 a.SourceTimeStamp  FROM UA_Data_Archive_Int32_4  as a WHERE a.UANode_ID = @UANODE_ID AND a.Value <> @ValueInt32 ORDER BY a.SourceTimeStamp DESC))
    ORDER BY a.SourceTimeStamp ASC)  
END
IF @archiveTable ='UA_Data_Current_Int64_4'
BEGIN
return (SELECT TOP 1 a.SourceTimeStamp  FROM UA_Data_Archive_Int64_4  as a WHERE a.UANode_ID = @UANODE_ID AND a.Value = @ValueInt64 
   and a.SourceTimeStamp>( (SELECT TOP 1 a.SourceTimeStamp  FROM UA_Data_Archive_Int64_4  as a WHERE a.UANode_ID = @UANODE_ID AND a.Value <> @ValueInt64 ORDER BY a.SourceTimeStamp DESC))
    ORDER BY a.SourceTimeStamp ASC)  
END
IF @archiveTable ='UA_Data_Current_Float_4'
BEGIN
return (SELECT TOP 1 a.SourceTimeStamp  FROM UA_Data_Archive_Float_4  as a WHERE a.UANode_ID = @UANODE_ID AND a.Value = @ValueFloat 
   and a.SourceTimeStamp>( (SELECT TOP 1 a.SourceTimeStamp  FROM UA_Data_Archive_Float_4  as a WHERE a.UANode_ID = @UANODE_ID AND a.Value <> @ValueFloat ORDER BY a.SourceTimeStamp DESC))
    ORDER BY a.SourceTimeStamp ASC)  
END
IF @archiveTable ='UA_Data_Current_String_4'
BEGIN
return (SELECT TOP 1 a.SourceTimeStamp  FROM UA_Data_Archive_String_4  as a WHERE a.UANode_ID = @UANODE_ID AND a.Value = @ValueString 
   and a.SourceTimeStamp>( (SELECT TOP 1 a.SourceTimeStamp  FROM UA_Data_Archive_String_4  as a WHERE a.UANode_ID = @UANODE_ID AND a.Value <> @ValueString ORDER BY a.SourceTimeStamp DESC))
    ORDER BY a.SourceTimeStamp ASC)  
END
IF @archiveTable ='UA_Data_Current_DateTime_4'
BEGIN
return (SELECT TOP 1 a.SourceTimeStamp  FROM UA_Data_Archive_DateTime_4  as a WHERE a.UANode_ID = @UANODE_ID AND a.Value = @ValueDateTime 
   and a.SourceTimeStamp>( (SELECT TOP 1 a.SourceTimeStamp  FROM UA_Data_Archive_DateTime_4  as a WHERE a.UANode_ID = @UANODE_ID AND a.Value <> @ValueDateTime ORDER BY a.SourceTimeStamp DESC))
    ORDER BY a.SourceTimeStamp ASC)  
END
IF @archiveTable ='UA_Data_Current_GUID_4'
BEGIN
return (SELECT TOP 1 a.SourceTimeStamp  FROM UA_Data_Archive_GUID_4  as a WHERE a.UANode_ID = @UANODE_ID AND a.Value = @ValueGUID 
   and a.SourceTimeStamp>( (SELECT TOP 1 a.SourceTimeStamp  FROM UA_Data_Archive_GUID_4  as a WHERE a.UANode_ID = @UANODE_ID AND a.Value <> @ValueGUID ORDER BY a.SourceTimeStamp DESC))
    ORDER BY a.SourceTimeStamp ASC)  
END
IF @archiveTable ='UA_Data_Current_ByteString_4'
BEGIN
return (SELECT TOP 1 a.SourceTimeStamp  FROM UA_Data_Archive_ByteString_4  as a WHERE a.UANode_ID = @UANODE_ID AND a.Value = @ValueByteString 
   and a.SourceTimeStamp>( (SELECT TOP 1 a.SourceTimeStamp  FROM UA_Data_Archive_ByteString_4  as a WHERE a.UANode_ID = @UANODE_ID AND a.Value <> @ValueByteString ORDER BY a.SourceTimeStamp DESC))
    ORDER BY a.SourceTimeStamp ASC)  
END
IF @archiveTable ='UA_Data_Current_XMLElement_4'
BEGIN
return (SELECT TOP 1 a.SourceTimeStamp  FROM UA_Data_Archive_XMLElement_4  as a WHERE a.UANode_ID = @UANODE_ID AND  Convert(nvarchar(max), a.Value) = Convert(nvarchar(max),@ValueXMLElement)
   and a.SourceTimeStamp>( (SELECT TOP 1 a.SourceTimeStamp  FROM UA_Data_Archive_XMLElement_4  as a WHERE a.UANode_ID = @UANODE_ID AND  Convert(nvarchar(max), a.Value) <> Convert(nvarchar(max),@ValueXMLElement) ORDER BY a.SourceTimeStamp DESC))
    ORDER BY  a.SourceTimeStamp DESC)  
END
IF @archiveTable ='UA_Data_Current_NodeID_4'
BEGIN
return (SELECT TOP 1 a.SourceTimeStamp  FROM UA_Data_Archive_NodeID_4  as a WHERE a.UANode_ID = @UANODE_ID AND a.ValueNodeID = @ValueUANode_ID 
   and a.SourceTimeStamp>( (SELECT TOP 1 a.SourceTimeStamp  FROM UA_Data_Archive_NodeID_4  as a WHERE a.UANode_ID = @UANODE_ID AND a.ValueNodeID <> @ValueUANode_ID ORDER BY a.SourceTimeStamp DESC))
    ORDER BY a.SourceTimeStamp ASC)  
END
IF @archiveTable ='UA_Data_Current_ExpandedNodeID_4'
BEGIN
return (SELECT TOP 1 a.SourceTimeStamp  FROM UA_Data_Archive_ExpandedNodeID_4  as a WHERE a.UANode_ID = @UANODE_ID AND a.ValueNodeID = @ValueNodeID 
   and a.SourceTimeStamp>( (SELECT TOP 1 a.SourceTimeStamp  FROM UA_Data_Archive_ExpandedNodeID_4  as a WHERE a.UANode_ID = @UANODE_ID AND a.ValueNodeID <> @ValueNodeID ORDER BY a.SourceTimeStamp DESC))
    ORDER BY a.SourceTimeStamp ASC)  
END
IF @archiveTable ='UA_Data_Current_DateTime_4'
BEGIN
return (SELECT TOP 1 a.SourceTimeStamp  FROM UA_Data_Archive_DateTime_4  as a WHERE a.UANode_ID = @UANODE_ID AND a.Value = @ValueDateTime 
   and a.SourceTimeStamp>( (SELECT TOP 1 a.SourceTimeStamp  FROM UA_Data_Archive_DateTime_4  as a WHERE a.UANode_ID = @UANODE_ID AND a.Value <> @ValueDateTime ORDER BY a.SourceTimeStamp DESC))
    ORDER BY a.SourceTimeStamp ASC)  
END
IF @archiveTable ='UA_Data_Current_Boolean_5'
BEGIN
   return (SELECT TOP 1 a.SourceTimeStamp  FROM UA_Data_Archive_Boolean_5  as a WHERE a.UANode_ID = @UANODE_ID AND a.Value = @ValueBoolean 
   and a.SourceTimeStamp>( (SELECT TOP 1 a.SourceTimeStamp  FROM UA_Data_Archive_Boolean_5  as a WHERE a.UANode_ID = @UANODE_ID AND a.Value <> @ValueBoolean ORDER BY a.SourceTimeStamp DESC))
    ORDER BY a.SourceTimeStamp ASC)
END
IF @archiveTable ='UA_Data_Current_Int16_5'
BEGIN
   return (SELECT TOP 1 a.SourceTimeStamp  FROM UA_Data_Archive_Int16_5  as a WHERE a.UANode_ID = @UANODE_ID AND a.Value = @ValueInt16 
   and a.SourceTimeStamp>( (SELECT TOP 1 a.SourceTimeStamp  FROM UA_Data_Archive_Int16_5  as a WHERE a.UANode_ID = @UANODE_ID AND a.Value <> @ValueInt16 ORDER BY a.SourceTimeStamp DESC))
    ORDER BY a.SourceTimeStamp ASC)  
END
IF @archiveTable ='UA_Data_Current_Int32_5'
BEGIN
return (SELECT TOP 1 a.SourceTimeStamp  FROM UA_Data_Archive_Int32_5  as a WHERE a.UANode_ID = @UANODE_ID AND a.Value = @ValueInt32 
   and a.SourceTimeStamp>( (SELECT TOP 1 a.SourceTimeStamp  FROM UA_Data_Archive_Int32_5  as a WHERE a.UANode_ID = @UANODE_ID AND a.Value <> @ValueInt32 ORDER BY a.SourceTimeStamp DESC))
    ORDER BY a.SourceTimeStamp ASC)  
END
IF @archiveTable ='UA_Data_Current_Int64_5'
BEGIN
return (SELECT TOP 1 a.SourceTimeStamp  FROM UA_Data_Archive_Int64_5  as a WHERE a.UANode_ID = @UANODE_ID AND a.Value = @ValueInt64 
   and a.SourceTimeStamp>( (SELECT TOP 1 a.SourceTimeStamp  FROM UA_Data_Archive_Int64_5  as a WHERE a.UANode_ID = @UANODE_ID AND a.Value <> @ValueInt64 ORDER BY a.SourceTimeStamp DESC))
    ORDER BY a.SourceTimeStamp ASC)  
END
IF @archiveTable ='UA_Data_Current_Float_5'
BEGIN
return (SELECT TOP 1 a.SourceTimeStamp  FROM UA_Data_Archive_Float_5  as a WHERE a.UANode_ID = @UANODE_ID AND a.Value = @ValueFloat 
   and a.SourceTimeStamp>( (SELECT TOP 1 a.SourceTimeStamp  FROM UA_Data_Archive_Float_5  as a WHERE a.UANode_ID = @UANODE_ID AND a.Value <> @ValueFloat ORDER BY a.SourceTimeStamp DESC))
    ORDER BY a.SourceTimeStamp ASC)  
END
IF @archiveTable ='UA_Data_Current_String_5'
BEGIN
return (SELECT TOP 1 a.SourceTimeStamp  FROM UA_Data_Archive_String_5  as a WHERE a.UANode_ID = @UANODE_ID AND a.Value = @ValueString 
   and a.SourceTimeStamp>( (SELECT TOP 1 a.SourceTimeStamp  FROM UA_Data_Archive_String_5  as a WHERE a.UANode_ID = @UANODE_ID AND a.Value <> @ValueString ORDER BY a.SourceTimeStamp DESC))
    ORDER BY a.SourceTimeStamp ASC)  
END
IF @archiveTable ='UA_Data_Current_DateTime_5'
BEGIN
return (SELECT TOP 1 a.SourceTimeStamp  FROM UA_Data_Archive_DateTime_5  as a WHERE a.UANode_ID = @UANODE_ID AND a.Value = @ValueDateTime 
   and a.SourceTimeStamp>( (SELECT TOP 1 a.SourceTimeStamp  FROM UA_Data_Archive_DateTime_5  as a WHERE a.UANode_ID = @UANODE_ID AND a.Value <> @ValueDateTime ORDER BY a.SourceTimeStamp DESC))
    ORDER BY a.SourceTimeStamp ASC)  
END
IF @archiveTable ='UA_Data_Current_GUID_5'
BEGIN
return (SELECT TOP 1 a.SourceTimeStamp  FROM UA_Data_Archive_GUID_5  as a WHERE a.UANode_ID = @UANODE_ID AND a.Value = @ValueGUID 
   and a.SourceTimeStamp>( (SELECT TOP 1 a.SourceTimeStamp  FROM UA_Data_Archive_GUID_5  as a WHERE a.UANode_ID = @UANODE_ID AND a.Value <> @ValueGUID ORDER BY a.SourceTimeStamp DESC))
    ORDER BY a.SourceTimeStamp ASC)  
END
IF @archiveTable ='UA_Data_Current_ByteString_5'
BEGIN
return (SELECT TOP 1 a.SourceTimeStamp  FROM UA_Data_Archive_ByteString_5  as a WHERE a.UANode_ID = @UANODE_ID AND a.Value = @ValueByteString 
   and a.SourceTimeStamp>( (SELECT TOP 1 a.SourceTimeStamp  FROM UA_Data_Archive_ByteString_5  as a WHERE a.UANode_ID = @UANODE_ID AND a.Value <> @ValueByteString ORDER BY a.SourceTimeStamp DESC))
    ORDER BY a.SourceTimeStamp ASC)  
END
IF @archiveTable ='UA_Data_Current_XMLElement_5'
BEGIN
return (SELECT TOP 1 a.SourceTimeStamp  FROM UA_Data_Archive_XMLElement_5  as a WHERE a.UANode_ID = @UANODE_ID AND  Convert(nvarchar(max), a.Value) = Convert(nvarchar(max),@ValueXMLElement)
   and a.SourceTimeStamp>( (SELECT TOP 1 a.SourceTimeStamp  FROM UA_Data_Archive_XMLElement_5  as a WHERE a.UANode_ID = @UANODE_ID AND  Convert(nvarchar(max), a.Value) <> Convert(nvarchar(max),@ValueXMLElement) ORDER BY a.SourceTimeStamp DESC))
    ORDER BY  a.SourceTimeStamp DESC)  
END
IF @archiveTable ='UA_Data_Current_NodeID_5'
BEGIN
return (SELECT TOP 1 a.SourceTimeStamp  FROM UA_Data_Archive_NodeID_5  as a WHERE a.UANode_ID = @UANODE_ID AND a.ValueNodeID = @ValueUANode_ID 
   and a.SourceTimeStamp>( (SELECT TOP 1 a.SourceTimeStamp  FROM UA_Data_Archive_NodeID_5  as a WHERE a.UANode_ID = @UANODE_ID AND a.ValueNodeID <> @ValueUANode_ID ORDER BY a.SourceTimeStamp DESC))
    ORDER BY a.SourceTimeStamp ASC)  
END
IF @archiveTable ='UA_Data_Current_ExpandedNodeID_5'
BEGIN
return (SELECT TOP 1 a.SourceTimeStamp  FROM UA_Data_Archive_ExpandedNodeID_5  as a WHERE a.UANode_ID = @UANODE_ID AND a.ValueNodeID = @ValueNodeID 
   and a.SourceTimeStamp>( (SELECT TOP 1 a.SourceTimeStamp  FROM UA_Data_Archive_ExpandedNodeID_5  as a WHERE a.UANode_ID = @UANODE_ID AND a.ValueNodeID <> @ValueNodeID ORDER BY a.SourceTimeStamp DESC))
    ORDER BY a.SourceTimeStamp ASC)  
END
IF @archiveTable ='UA_Data_Current_DateTime_5'
BEGIN
return (SELECT TOP 1 a.SourceTimeStamp  FROM UA_Data_Archive_DateTime_5  as a WHERE a.UANode_ID = @UANODE_ID AND a.Value = @ValueDateTime 
   and a.SourceTimeStamp>( (SELECT TOP 1 a.SourceTimeStamp  FROM UA_Data_Archive_DateTime_5  as a WHERE a.UANode_ID = @UANODE_ID AND a.Value <> @ValueDateTime ORDER BY a.SourceTimeStamp DESC))
    ORDER BY a.SourceTimeStamp ASC)  
END
IF @archiveTable ='UA_Data_Current_Boolean_6'
BEGIN
   return (SELECT TOP 1 a.SourceTimeStamp  FROM UA_Data_Archive_Boolean_6  as a WHERE a.UANode_ID = @UANODE_ID AND a.Value = @ValueBoolean 
   and a.SourceTimeStamp>( (SELECT TOP 1 a.SourceTimeStamp  FROM UA_Data_Archive_Boolean_6  as a WHERE a.UANode_ID = @UANODE_ID AND a.Value <> @ValueBoolean ORDER BY a.SourceTimeStamp DESC))
    ORDER BY a.SourceTimeStamp ASC)
END
IF @archiveTable ='UA_Data_Current_Int16_6'
BEGIN
   return (SELECT TOP 1 a.SourceTimeStamp  FROM UA_Data_Archive_Int16_6  as a WHERE a.UANode_ID = @UANODE_ID AND a.Value = @ValueInt16 
   and a.SourceTimeStamp>( (SELECT TOP 1 a.SourceTimeStamp  FROM UA_Data_Archive_Int16_6  as a WHERE a.UANode_ID = @UANODE_ID AND a.Value <> @ValueInt16 ORDER BY a.SourceTimeStamp DESC))
    ORDER BY a.SourceTimeStamp ASC)  
END
IF @archiveTable ='UA_Data_Current_Int32_6'
BEGIN
return (SELECT TOP 1 a.SourceTimeStamp  FROM UA_Data_Archive_Int32_6  as a WHERE a.UANode_ID = @UANODE_ID AND a.Value = @ValueInt32 
   and a.SourceTimeStamp>( (SELECT TOP 1 a.SourceTimeStamp  FROM UA_Data_Archive_Int32_6  as a WHERE a.UANode_ID = @UANODE_ID AND a.Value <> @ValueInt32 ORDER BY a.SourceTimeStamp DESC))
    ORDER BY a.SourceTimeStamp ASC)  
END
IF @archiveTable ='UA_Data_Current_Int64_6'
BEGIN
return (SELECT TOP 1 a.SourceTimeStamp  FROM UA_Data_Archive_Int64_6  as a WHERE a.UANode_ID = @UANODE_ID AND a.Value = @ValueInt64 
   and a.SourceTimeStamp>( (SELECT TOP 1 a.SourceTimeStamp  FROM UA_Data_Archive_Int64_6  as a WHERE a.UANode_ID = @UANODE_ID AND a.Value <> @ValueInt64 ORDER BY a.SourceTimeStamp DESC))
    ORDER BY a.SourceTimeStamp ASC)  
END
IF @archiveTable ='UA_Data_Current_Float_6'
BEGIN
return (SELECT TOP 1 a.SourceTimeStamp  FROM UA_Data_Archive_Float_6  as a WHERE a.UANode_ID = @UANODE_ID AND a.Value = @ValueFloat 
   and a.SourceTimeStamp>( (SELECT TOP 1 a.SourceTimeStamp  FROM UA_Data_Archive_Float_6  as a WHERE a.UANode_ID = @UANODE_ID AND a.Value <> @ValueFloat ORDER BY a.SourceTimeStamp DESC))
    ORDER BY a.SourceTimeStamp ASC)  
END
IF @archiveTable ='UA_Data_Current_String_6'
BEGIN
return (SELECT TOP 1 a.SourceTimeStamp  FROM UA_Data_Archive_String_6  as a WHERE a.UANode_ID = @UANODE_ID AND a.Value = @ValueString 
   and a.SourceTimeStamp>( (SELECT TOP 1 a.SourceTimeStamp  FROM UA_Data_Archive_String_6  as a WHERE a.UANode_ID = @UANODE_ID AND a.Value <> @ValueString ORDER BY a.SourceTimeStamp DESC))
    ORDER BY a.SourceTimeStamp ASC)  
END
IF @archiveTable ='UA_Data_Current_DateTime_6'
BEGIN
return (SELECT TOP 1 a.SourceTimeStamp  FROM UA_Data_Archive_DateTime_6  as a WHERE a.UANode_ID = @UANODE_ID AND a.Value = @ValueDateTime 
   and a.SourceTimeStamp>( (SELECT TOP 1 a.SourceTimeStamp  FROM UA_Data_Archive_DateTime_6  as a WHERE a.UANode_ID = @UANODE_ID AND a.Value <> @ValueDateTime ORDER BY a.SourceTimeStamp DESC))
    ORDER BY a.SourceTimeStamp ASC)  
END
IF @archiveTable ='UA_Data_Current_GUID_6'
BEGIN
return (SELECT TOP 1 a.SourceTimeStamp  FROM UA_Data_Archive_GUID_6  as a WHERE a.UANode_ID = @UANODE_ID AND a.Value = @ValueGUID 
   and a.SourceTimeStamp>( (SELECT TOP 1 a.SourceTimeStamp  FROM UA_Data_Archive_GUID_6  as a WHERE a.UANode_ID = @UANODE_ID AND a.Value <> @ValueGUID ORDER BY a.SourceTimeStamp DESC))
    ORDER BY a.SourceTimeStamp ASC)  
END
IF @archiveTable ='UA_Data_Current_ByteString_6'
BEGIN
return (SELECT TOP 1 a.SourceTimeStamp  FROM UA_Data_Archive_ByteString_6  as a WHERE a.UANode_ID = @UANODE_ID AND a.Value = @ValueByteString 
   and a.SourceTimeStamp>( (SELECT TOP 1 a.SourceTimeStamp  FROM UA_Data_Archive_ByteString_6  as a WHERE a.UANode_ID = @UANODE_ID AND a.Value <> @ValueByteString ORDER BY a.SourceTimeStamp DESC))
    ORDER BY a.SourceTimeStamp ASC)  
END
IF @archiveTable ='UA_Data_Current_XMLElement_6'
BEGIN
return (SELECT TOP 1 a.SourceTimeStamp  FROM UA_Data_Archive_XMLElement_6  as a WHERE a.UANode_ID = @UANODE_ID AND  Convert(nvarchar(max), a.Value) = Convert(nvarchar(max),@ValueXMLElement)
   and a.SourceTimeStamp>( (SELECT TOP 1 a.SourceTimeStamp  FROM UA_Data_Archive_XMLElement_6  as a WHERE a.UANode_ID = @UANODE_ID AND  Convert(nvarchar(max), a.Value) <> Convert(nvarchar(max),@ValueXMLElement) ORDER BY a.SourceTimeStamp DESC))
    ORDER BY  a.SourceTimeStamp DESC)  
END
IF @archiveTable ='UA_Data_Current_NodeID_6'
BEGIN
return (SELECT TOP 1 a.SourceTimeStamp  FROM UA_Data_Archive_NodeID_6  as a WHERE a.UANode_ID = @UANODE_ID AND a.ValueNodeID = @ValueUANode_ID 
   and a.SourceTimeStamp>( (SELECT TOP 1 a.SourceTimeStamp  FROM UA_Data_Archive_NodeID_6  as a WHERE a.UANode_ID = @UANODE_ID AND a.ValueNodeID <> @ValueUANode_ID ORDER BY a.SourceTimeStamp DESC))
    ORDER BY a.SourceTimeStamp ASC)  
END
IF @archiveTable ='UA_Data_Current_ExpandedNodeID_6'
BEGIN
return (SELECT TOP 1 a.SourceTimeStamp  FROM UA_Data_Archive_ExpandedNodeID_6  as a WHERE a.UANode_ID = @UANODE_ID AND a.ValueNodeID = @ValueNodeID 
   and a.SourceTimeStamp>( (SELECT TOP 1 a.SourceTimeStamp  FROM UA_Data_Archive_ExpandedNodeID_6  as a WHERE a.UANode_ID = @UANODE_ID AND a.ValueNodeID <> @ValueNodeID ORDER BY a.SourceTimeStamp DESC))
    ORDER BY a.SourceTimeStamp ASC)  
END
IF @archiveTable ='UA_Data_Current_DateTime_6'
BEGIN
return (SELECT TOP 1 a.SourceTimeStamp  FROM UA_Data_Archive_DateTime_6  as a WHERE a.UANode_ID = @UANODE_ID AND a.Value = @ValueDateTime 
   and a.SourceTimeStamp>( (SELECT TOP 1 a.SourceTimeStamp  FROM UA_Data_Archive_DateTime_6  as a WHERE a.UANode_ID = @UANODE_ID AND a.Value <> @ValueDateTime ORDER BY a.SourceTimeStamp DESC))
    ORDER BY a.SourceTimeStamp ASC)  
END
IF @archiveTable ='UA_Data_Current_Boolean_7'
BEGIN
   return (SELECT TOP 1 a.SourceTimeStamp  FROM UA_Data_Archive_Boolean_7  as a WHERE a.UANode_ID = @UANODE_ID AND a.Value = @ValueBoolean 
   and a.SourceTimeStamp>( (SELECT TOP 1 a.SourceTimeStamp  FROM UA_Data_Archive_Boolean_7  as a WHERE a.UANode_ID = @UANODE_ID AND a.Value <> @ValueBoolean ORDER BY a.SourceTimeStamp DESC))
    ORDER BY a.SourceTimeStamp ASC)
END
IF @archiveTable ='UA_Data_Current_Int16_7'
BEGIN
   return (SELECT TOP 1 a.SourceTimeStamp  FROM UA_Data_Archive_Int16_7  as a WHERE a.UANode_ID = @UANODE_ID AND a.Value = @ValueInt16 
   and a.SourceTimeStamp>( (SELECT TOP 1 a.SourceTimeStamp  FROM UA_Data_Archive_Int16_7  as a WHERE a.UANode_ID = @UANODE_ID AND a.Value <> @ValueInt16 ORDER BY a.SourceTimeStamp DESC))
    ORDER BY a.SourceTimeStamp ASC)  
END
IF @archiveTable ='UA_Data_Current_Int32_7'
BEGIN
return (SELECT TOP 1 a.SourceTimeStamp  FROM UA_Data_Archive_Int32_7  as a WHERE a.UANode_ID = @UANODE_ID AND a.Value = @ValueInt32 
   and a.SourceTimeStamp>( (SELECT TOP 1 a.SourceTimeStamp  FROM UA_Data_Archive_Int32_7  as a WHERE a.UANode_ID = @UANODE_ID AND a.Value <> @ValueInt32 ORDER BY a.SourceTimeStamp DESC))
    ORDER BY a.SourceTimeStamp ASC)  
END
IF @archiveTable ='UA_Data_Current_Int64_7'
BEGIN
return (SELECT TOP 1 a.SourceTimeStamp  FROM UA_Data_Archive_Int64_7  as a WHERE a.UANode_ID = @UANODE_ID AND a.Value = @ValueInt64 
   and a.SourceTimeStamp>( (SELECT TOP 1 a.SourceTimeStamp  FROM UA_Data_Archive_Int64_7  as a WHERE a.UANode_ID = @UANODE_ID AND a.Value <> @ValueInt64 ORDER BY a.SourceTimeStamp DESC))
    ORDER BY a.SourceTimeStamp ASC)  
END
IF @archiveTable ='UA_Data_Current_Float_7'
BEGIN
return (SELECT TOP 1 a.SourceTimeStamp  FROM UA_Data_Archive_Float_7  as a WHERE a.UANode_ID = @UANODE_ID AND a.Value = @ValueFloat 
   and a.SourceTimeStamp>( (SELECT TOP 1 a.SourceTimeStamp  FROM UA_Data_Archive_Float_7  as a WHERE a.UANode_ID = @UANODE_ID AND a.Value <> @ValueFloat ORDER BY a.SourceTimeStamp DESC))
    ORDER BY a.SourceTimeStamp ASC)  
END
IF @archiveTable ='UA_Data_Current_String_7'
BEGIN
return (SELECT TOP 1 a.SourceTimeStamp  FROM UA_Data_Archive_String_7  as a WHERE a.UANode_ID = @UANODE_ID AND a.Value = @ValueString 
   and a.SourceTimeStamp>( (SELECT TOP 1 a.SourceTimeStamp  FROM UA_Data_Archive_String_7  as a WHERE a.UANode_ID = @UANODE_ID AND a.Value <> @ValueString ORDER BY a.SourceTimeStamp DESC))
    ORDER BY a.SourceTimeStamp ASC)  
END
IF @archiveTable ='UA_Data_Current_DateTime_7'
BEGIN
return (SELECT TOP 1 a.SourceTimeStamp  FROM UA_Data_Archive_DateTime_7  as a WHERE a.UANode_ID = @UANODE_ID AND a.Value = @ValueDateTime 
   and a.SourceTimeStamp>( (SELECT TOP 1 a.SourceTimeStamp  FROM UA_Data_Archive_DateTime_7  as a WHERE a.UANode_ID = @UANODE_ID AND a.Value <> @ValueDateTime ORDER BY a.SourceTimeStamp DESC))
    ORDER BY a.SourceTimeStamp ASC)  
END
IF @archiveTable ='UA_Data_Current_GUID_7'
BEGIN
return (SELECT TOP 1 a.SourceTimeStamp  FROM UA_Data_Archive_GUID_7  as a WHERE a.UANode_ID = @UANODE_ID AND a.Value = @ValueGUID 
   and a.SourceTimeStamp>( (SELECT TOP 1 a.SourceTimeStamp  FROM UA_Data_Archive_GUID_7  as a WHERE a.UANode_ID = @UANODE_ID AND a.Value <> @ValueGUID ORDER BY a.SourceTimeStamp DESC))
    ORDER BY a.SourceTimeStamp ASC)  
END
IF @archiveTable ='UA_Data_Current_ByteString_7'
BEGIN
return (SELECT TOP 1 a.SourceTimeStamp  FROM UA_Data_Archive_ByteString_7  as a WHERE a.UANode_ID = @UANODE_ID AND a.Value = @ValueByteString 
   and a.SourceTimeStamp>( (SELECT TOP 1 a.SourceTimeStamp  FROM UA_Data_Archive_ByteString_7  as a WHERE a.UANode_ID = @UANODE_ID AND a.Value <> @ValueByteString ORDER BY a.SourceTimeStamp DESC))
    ORDER BY a.SourceTimeStamp ASC)  
END
IF @archiveTable ='UA_Data_Current_XMLElement_7'
BEGIN
return (SELECT TOP 1 a.SourceTimeStamp  FROM UA_Data_Archive_XMLElement_7  as a WHERE a.UANode_ID = @UANODE_ID AND  Convert(nvarchar(max), a.Value) = Convert(nvarchar(max),@ValueXMLElement)
   and a.SourceTimeStamp>( (SELECT TOP 1 a.SourceTimeStamp  FROM UA_Data_Archive_XMLElement_7  as a WHERE a.UANode_ID = @UANODE_ID AND  Convert(nvarchar(max), a.Value) <> Convert(nvarchar(max),@ValueXMLElement) ORDER BY a.SourceTimeStamp DESC))
    ORDER BY  a.SourceTimeStamp DESC)  
END
IF @archiveTable ='UA_Data_Current_NodeID_7'
BEGIN
return (SELECT TOP 1 a.SourceTimeStamp  FROM UA_Data_Archive_NodeID_7  as a WHERE a.UANode_ID = @UANODE_ID AND a.ValueNodeID = @ValueUANode_ID 
   and a.SourceTimeStamp>( (SELECT TOP 1 a.SourceTimeStamp  FROM UA_Data_Archive_NodeID_7  as a WHERE a.UANode_ID = @UANODE_ID AND a.ValueNodeID <> @ValueUANode_ID ORDER BY a.SourceTimeStamp DESC))
    ORDER BY a.SourceTimeStamp ASC)  
END
IF @archiveTable ='UA_Data_Current_ExpandedNodeID_7'
BEGIN
return (SELECT TOP 1 a.SourceTimeStamp  FROM UA_Data_Archive_ExpandedNodeID_7  as a WHERE a.UANode_ID = @UANODE_ID AND a.ValueNodeID = @ValueNodeID 
   and a.SourceTimeStamp>( (SELECT TOP 1 a.SourceTimeStamp  FROM UA_Data_Archive_ExpandedNodeID_7  as a WHERE a.UANode_ID = @UANODE_ID AND a.ValueNodeID <> @ValueNodeID ORDER BY a.SourceTimeStamp DESC))
    ORDER BY a.SourceTimeStamp ASC)  
END
IF @archiveTable ='UA_Data_Current_DateTime_7'
BEGIN
return (SELECT TOP 1 a.SourceTimeStamp  FROM UA_Data_Archive_DateTime_7  as a WHERE a.UANode_ID = @UANODE_ID AND a.Value = @ValueDateTime 
   and a.SourceTimeStamp>( (SELECT TOP 1 a.SourceTimeStamp  FROM UA_Data_Archive_DateTime_7  as a WHERE a.UANode_ID = @UANODE_ID AND a.Value <> @ValueDateTime ORDER BY a.SourceTimeStamp DESC))
    ORDER BY a.SourceTimeStamp ASC)  
END
IF @archiveTable ='UA_Data_Current_Boolean_8'
BEGIN
   return (SELECT TOP 1 a.SourceTimeStamp  FROM UA_Data_Archive_Boolean_8  as a WHERE a.UANode_ID = @UANODE_ID AND a.Value = @ValueBoolean 
   and a.SourceTimeStamp>( (SELECT TOP 1 a.SourceTimeStamp  FROM UA_Data_Archive_Boolean_8  as a WHERE a.UANode_ID = @UANODE_ID AND a.Value <> @ValueBoolean ORDER BY a.SourceTimeStamp DESC))
    ORDER BY a.SourceTimeStamp ASC)
END
IF @archiveTable ='UA_Data_Current_Int16_8'
BEGIN
   return (SELECT TOP 1 a.SourceTimeStamp  FROM UA_Data_Archive_Int16_8  as a WHERE a.UANode_ID = @UANODE_ID AND a.Value = @ValueInt16 
   and a.SourceTimeStamp>( (SELECT TOP 1 a.SourceTimeStamp  FROM UA_Data_Archive_Int16_8  as a WHERE a.UANode_ID = @UANODE_ID AND a.Value <> @ValueInt16 ORDER BY a.SourceTimeStamp DESC))
    ORDER BY a.SourceTimeStamp ASC)  
END
IF @archiveTable ='UA_Data_Current_Int32_8'
BEGIN
return (SELECT TOP 1 a.SourceTimeStamp  FROM UA_Data_Archive_Int32_8  as a WHERE a.UANode_ID = @UANODE_ID AND a.Value = @ValueInt32 
   and a.SourceTimeStamp>( (SELECT TOP 1 a.SourceTimeStamp  FROM UA_Data_Archive_Int32_8  as a WHERE a.UANode_ID = @UANODE_ID AND a.Value <> @ValueInt32 ORDER BY a.SourceTimeStamp DESC))
    ORDER BY a.SourceTimeStamp ASC)  
END
IF @archiveTable ='UA_Data_Current_Int64_8'
BEGIN
return (SELECT TOP 1 a.SourceTimeStamp  FROM UA_Data_Archive_Int64_8  as a WHERE a.UANode_ID = @UANODE_ID AND a.Value = @ValueInt64 
   and a.SourceTimeStamp>( (SELECT TOP 1 a.SourceTimeStamp  FROM UA_Data_Archive_Int64_8  as a WHERE a.UANode_ID = @UANODE_ID AND a.Value <> @ValueInt64 ORDER BY a.SourceTimeStamp DESC))
    ORDER BY a.SourceTimeStamp ASC)  
END
IF @archiveTable ='UA_Data_Current_Float_8'
BEGIN
return (SELECT TOP 1 a.SourceTimeStamp  FROM UA_Data_Archive_Float_8  as a WHERE a.UANode_ID = @UANODE_ID AND a.Value = @ValueFloat 
   and a.SourceTimeStamp>( (SELECT TOP 1 a.SourceTimeStamp  FROM UA_Data_Archive_Float_8  as a WHERE a.UANode_ID = @UANODE_ID AND a.Value <> @ValueFloat ORDER BY a.SourceTimeStamp DESC))
    ORDER BY a.SourceTimeStamp ASC)  
END
IF @archiveTable ='UA_Data_Current_String_8'
BEGIN
return (SELECT TOP 1 a.SourceTimeStamp  FROM UA_Data_Archive_String_8  as a WHERE a.UANode_ID = @UANODE_ID AND a.Value = @ValueString 
   and a.SourceTimeStamp>( (SELECT TOP 1 a.SourceTimeStamp  FROM UA_Data_Archive_String_8  as a WHERE a.UANode_ID = @UANODE_ID AND a.Value <> @ValueString ORDER BY a.SourceTimeStamp DESC))
    ORDER BY a.SourceTimeStamp ASC)  
END
IF @archiveTable ='UA_Data_Current_DateTime_8'
BEGIN
return (SELECT TOP 1 a.SourceTimeStamp  FROM UA_Data_Archive_DateTime_8  as a WHERE a.UANode_ID = @UANODE_ID AND a.Value = @ValueDateTime 
   and a.SourceTimeStamp>( (SELECT TOP 1 a.SourceTimeStamp  FROM UA_Data_Archive_DateTime_8  as a WHERE a.UANode_ID = @UANODE_ID AND a.Value <> @ValueDateTime ORDER BY a.SourceTimeStamp DESC))
    ORDER BY a.SourceTimeStamp ASC)  
END
IF @archiveTable ='UA_Data_Current_GUID_8'
BEGIN
return (SELECT TOP 1 a.SourceTimeStamp  FROM UA_Data_Archive_GUID_8  as a WHERE a.UANode_ID = @UANODE_ID AND a.Value = @ValueGUID 
   and a.SourceTimeStamp>( (SELECT TOP 1 a.SourceTimeStamp  FROM UA_Data_Archive_GUID_8  as a WHERE a.UANode_ID = @UANODE_ID AND a.Value <> @ValueGUID ORDER BY a.SourceTimeStamp DESC))
    ORDER BY a.SourceTimeStamp ASC)  
END
IF @archiveTable ='UA_Data_Current_ByteString_8'
BEGIN
return (SELECT TOP 1 a.SourceTimeStamp  FROM UA_Data_Archive_ByteString_8  as a WHERE a.UANode_ID = @UANODE_ID AND a.Value = @ValueByteString 
   and a.SourceTimeStamp>( (SELECT TOP 1 a.SourceTimeStamp  FROM UA_Data_Archive_ByteString_8  as a WHERE a.UANode_ID = @UANODE_ID AND a.Value <> @ValueByteString ORDER BY a.SourceTimeStamp DESC))
    ORDER BY a.SourceTimeStamp ASC)  
END
IF @archiveTable ='UA_Data_Current_XMLElement_8'
BEGIN
return (SELECT TOP 1 a.SourceTimeStamp  FROM UA_Data_Archive_XMLElement_8  as a WHERE a.UANode_ID = @UANODE_ID AND  Convert(nvarchar(max), a.Value) = Convert(nvarchar(max),@ValueXMLElement)
   and a.SourceTimeStamp>( (SELECT TOP 1 a.SourceTimeStamp  FROM UA_Data_Archive_XMLElement_8  as a WHERE a.UANode_ID = @UANODE_ID AND  Convert(nvarchar(max), a.Value) <> Convert(nvarchar(max),@ValueXMLElement) ORDER BY a.SourceTimeStamp DESC))
    ORDER BY  a.SourceTimeStamp DESC)  
END
IF @archiveTable ='UA_Data_Current_NodeID_8'
BEGIN
return (SELECT TOP 1 a.SourceTimeStamp  FROM UA_Data_Archive_NodeID_8  as a WHERE a.UANode_ID = @UANODE_ID AND a.ValueNodeID = @ValueUANode_ID 
   and a.SourceTimeStamp>( (SELECT TOP 1 a.SourceTimeStamp  FROM UA_Data_Archive_NodeID_8  as a WHERE a.UANode_ID = @UANODE_ID AND a.ValueNodeID <> @ValueUANode_ID ORDER BY a.SourceTimeStamp DESC))
    ORDER BY a.SourceTimeStamp ASC)  
END
IF @archiveTable ='UA_Data_Current_ExpandedNodeID_8'
BEGIN
return (SELECT TOP 1 a.SourceTimeStamp  FROM UA_Data_Archive_ExpandedNodeID_8  as a WHERE a.UANode_ID = @UANODE_ID AND a.ValueNodeID = @ValueNodeID 
   and a.SourceTimeStamp>( (SELECT TOP 1 a.SourceTimeStamp  FROM UA_Data_Archive_ExpandedNodeID_8  as a WHERE a.UANode_ID = @UANODE_ID AND a.ValueNodeID <> @ValueNodeID ORDER BY a.SourceTimeStamp DESC))
    ORDER BY a.SourceTimeStamp ASC)  
END
IF @archiveTable ='UA_Data_Current_DateTime_8'
BEGIN
return (SELECT TOP 1 a.SourceTimeStamp  FROM UA_Data_Archive_DateTime_8  as a WHERE a.UANode_ID = @UANODE_ID AND a.Value = @ValueDateTime 
   and a.SourceTimeStamp>( (SELECT TOP 1 a.SourceTimeStamp  FROM UA_Data_Archive_DateTime_8  as a WHERE a.UANode_ID = @UANODE_ID AND a.Value <> @ValueDateTime ORDER BY a.SourceTimeStamp DESC))
    ORDER BY a.SourceTimeStamp ASC)  
END
IF @archiveTable ='UA_Data_Current_Boolean_9'
BEGIN
   return (SELECT TOP 1 a.SourceTimeStamp  FROM UA_Data_Archive_Boolean_9  as a WHERE a.UANode_ID = @UANODE_ID AND a.Value = @ValueBoolean 
   and a.SourceTimeStamp>( (SELECT TOP 1 a.SourceTimeStamp  FROM UA_Data_Archive_Boolean_9  as a WHERE a.UANode_ID = @UANODE_ID AND a.Value <> @ValueBoolean ORDER BY a.SourceTimeStamp DESC))
    ORDER BY a.SourceTimeStamp ASC)
END
IF @archiveTable ='UA_Data_Current_Int16_9'
BEGIN
   return (SELECT TOP 1 a.SourceTimeStamp  FROM UA_Data_Archive_Int16_9  as a WHERE a.UANode_ID = @UANODE_ID AND a.Value = @ValueInt16 
   and a.SourceTimeStamp>( (SELECT TOP 1 a.SourceTimeStamp  FROM UA_Data_Archive_Int16_9  as a WHERE a.UANode_ID = @UANODE_ID AND a.Value <> @ValueInt16 ORDER BY a.SourceTimeStamp DESC))
    ORDER BY a.SourceTimeStamp ASC)  
END
IF @archiveTable ='UA_Data_Current_Int32_9'
BEGIN
return (SELECT TOP 1 a.SourceTimeStamp  FROM UA_Data_Archive_Int32_9  as a WHERE a.UANode_ID = @UANODE_ID AND a.Value = @ValueInt32 
   and a.SourceTimeStamp>( (SELECT TOP 1 a.SourceTimeStamp  FROM UA_Data_Archive_Int32_9  as a WHERE a.UANode_ID = @UANODE_ID AND a.Value <> @ValueInt32 ORDER BY a.SourceTimeStamp DESC))
    ORDER BY a.SourceTimeStamp ASC)  
END
IF @archiveTable ='UA_Data_Current_Int64_9'
BEGIN
return (SELECT TOP 1 a.SourceTimeStamp  FROM UA_Data_Archive_Int64_9  as a WHERE a.UANode_ID = @UANODE_ID AND a.Value = @ValueInt64 
   and a.SourceTimeStamp>( (SELECT TOP 1 a.SourceTimeStamp  FROM UA_Data_Archive_Int64_9  as a WHERE a.UANode_ID = @UANODE_ID AND a.Value <> @ValueInt64 ORDER BY a.SourceTimeStamp DESC))
    ORDER BY a.SourceTimeStamp ASC)  
END
IF @archiveTable ='UA_Data_Current_Float_9'
BEGIN
return (SELECT TOP 1 a.SourceTimeStamp  FROM UA_Data_Archive_Float_9  as a WHERE a.UANode_ID = @UANODE_ID AND a.Value = @ValueFloat 
   and a.SourceTimeStamp>( (SELECT TOP 1 a.SourceTimeStamp  FROM UA_Data_Archive_Float_9  as a WHERE a.UANode_ID = @UANODE_ID AND a.Value <> @ValueFloat ORDER BY a.SourceTimeStamp DESC))
    ORDER BY a.SourceTimeStamp ASC)  
END
IF @archiveTable ='UA_Data_Current_String_9'
BEGIN
return (SELECT TOP 1 a.SourceTimeStamp  FROM UA_Data_Archive_String_9  as a WHERE a.UANode_ID = @UANODE_ID AND a.Value = @ValueString 
   and a.SourceTimeStamp>( (SELECT TOP 1 a.SourceTimeStamp  FROM UA_Data_Archive_String_9  as a WHERE a.UANode_ID = @UANODE_ID AND a.Value <> @ValueString ORDER BY a.SourceTimeStamp DESC))
    ORDER BY a.SourceTimeStamp ASC)  
END
IF @archiveTable ='UA_Data_Current_DateTime_9'
BEGIN
return (SELECT TOP 1 a.SourceTimeStamp  FROM UA_Data_Archive_DateTime_9  as a WHERE a.UANode_ID = @UANODE_ID AND a.Value = @ValueDateTime 
   and a.SourceTimeStamp>( (SELECT TOP 1 a.SourceTimeStamp  FROM UA_Data_Archive_DateTime_9  as a WHERE a.UANode_ID = @UANODE_ID AND a.Value <> @ValueDateTime ORDER BY a.SourceTimeStamp DESC))
    ORDER BY a.SourceTimeStamp ASC)  
END
IF @archiveTable ='UA_Data_Current_GUID_9'
BEGIN
return (SELECT TOP 1 a.SourceTimeStamp  FROM UA_Data_Archive_GUID_9  as a WHERE a.UANode_ID = @UANODE_ID AND a.Value = @ValueGUID 
   and a.SourceTimeStamp>( (SELECT TOP 1 a.SourceTimeStamp  FROM UA_Data_Archive_GUID_9  as a WHERE a.UANode_ID = @UANODE_ID AND a.Value <> @ValueGUID ORDER BY a.SourceTimeStamp DESC))
    ORDER BY a.SourceTimeStamp ASC)  
END
IF @archiveTable ='UA_Data_Current_ByteString_9'
BEGIN
return (SELECT TOP 1 a.SourceTimeStamp  FROM UA_Data_Archive_ByteString_9  as a WHERE a.UANode_ID = @UANODE_ID AND a.Value = @ValueByteString 
   and a.SourceTimeStamp>( (SELECT TOP 1 a.SourceTimeStamp  FROM UA_Data_Archive_ByteString_9  as a WHERE a.UANode_ID = @UANODE_ID AND a.Value <> @ValueByteString ORDER BY a.SourceTimeStamp DESC))
    ORDER BY a.SourceTimeStamp ASC)  
END
IF @archiveTable ='UA_Data_Current_XMLElement_9'
BEGIN
return (SELECT TOP 1 a.SourceTimeStamp  FROM UA_Data_Archive_XMLElement_9  as a WHERE a.UANode_ID = @UANODE_ID AND  Convert(nvarchar(max), a.Value) = Convert(nvarchar(max),@ValueXMLElement)
   and a.SourceTimeStamp>( (SELECT TOP 1 a.SourceTimeStamp  FROM UA_Data_Archive_XMLElement_9  as a WHERE a.UANode_ID = @UANODE_ID AND  Convert(nvarchar(max), a.Value) <> Convert(nvarchar(max),@ValueXMLElement) ORDER BY a.SourceTimeStamp DESC))
    ORDER BY  a.SourceTimeStamp DESC)  
END
IF @archiveTable ='UA_Data_Current_NodeID_9'
BEGIN
return (SELECT TOP 1 a.SourceTimeStamp  FROM UA_Data_Archive_NodeID_9  as a WHERE a.UANode_ID = @UANODE_ID AND a.ValueNodeID = @ValueUANode_ID 
   and a.SourceTimeStamp>( (SELECT TOP 1 a.SourceTimeStamp  FROM UA_Data_Archive_NodeID_9  as a WHERE a.UANode_ID = @UANODE_ID AND a.ValueNodeID <> @ValueUANode_ID ORDER BY a.SourceTimeStamp DESC))
    ORDER BY a.SourceTimeStamp ASC)  
END
IF @archiveTable ='UA_Data_Current_ExpandedNodeID_9'
BEGIN
return (SELECT TOP 1 a.SourceTimeStamp  FROM UA_Data_Archive_ExpandedNodeID_9  as a WHERE a.UANode_ID = @UANODE_ID AND a.ValueNodeID = @ValueNodeID 
   and a.SourceTimeStamp>( (SELECT TOP 1 a.SourceTimeStamp  FROM UA_Data_Archive_ExpandedNodeID_9  as a WHERE a.UANode_ID = @UANODE_ID AND a.ValueNodeID <> @ValueNodeID ORDER BY a.SourceTimeStamp DESC))
    ORDER BY a.SourceTimeStamp ASC)  
END
IF @archiveTable ='UA_Data_Current_DateTime_9'
BEGIN
return (SELECT TOP 1 a.SourceTimeStamp  FROM UA_Data_Archive_DateTime_9  as a WHERE a.UANode_ID = @UANODE_ID AND a.Value = @ValueDateTime 
   and a.SourceTimeStamp>( (SELECT TOP 1 a.SourceTimeStamp  FROM UA_Data_Archive_DateTime_9  as a WHERE a.UANode_ID = @UANODE_ID AND a.Value <> @ValueDateTime ORDER BY a.SourceTimeStamp DESC))
    ORDER BY a.SourceTimeStamp ASC)  
END
RETURN NULL 
end


go
grant exec on sf_UA_GETLASTMODIFIEDVALUEDATETIME to [UserCalcService]
go

grant exec on sf_UA_GETLASTMODIFIEDVALUEDATETIME to UserDeclarator
go
grant exec on sf_UA_GETLASTMODIFIEDVALUEDATETIME to UserImportService
go
grant exec on sf_UA_GETLASTMODIFIEDVALUEDATETIME to UserExportService
go


-- ======================================================================================
-- Автор:
--
--		Малышев Игорь
--
-- Дата создания:
--
--		Январь, 2013
--
-- Описание:
--
--		Архивные данные в зависимости от таблицы, сортированно по приоритету источника
--
-- ======================================================================================
create proc [dbo].[usp2_UA_DataRead]
	@nodeIDs dbo.BigintType READONLY,
	@dtStart DateTime,
	@dtEnd DateTime,
	@isCurrent bit,
	@isReadArchiveSourceTimeStamp bit = null -- необходимо вернуть время изменения состояния
as
begin
set nocount on
set quoted_identifier, ansi_nulls, ansi_warnings, arithabort, concat_null_yields_null, ansi_padding on
set numeric_roundabort off
set transaction isolation level read uncommitted
set @isReadArchiveSourceTimeStamp = ISNULL(@isReadArchiveSourceTimeStamp, 0)
create table #NodeByTypes
(
	UANode_ID bigint,
	ArchiveTable nvarchar(128),
	ArchiveTransitTable nvarchar(128),
	DataType int,
	DateTypeName nvarchar(128),
	FieldNames nvarchar(128),
	PRIMARY KEY CLUSTERED (ArchiveTable, UANode_ID) WITH (IGNORE_DUP_KEY = OFF)
)
--Определяем тип
insert into #NodeByTypes 
select distinct n.UANode_ID, 
case when @isCurrent = 1 
	then replace(vta.ArchiveTable, '_Archive_', '_Current_')
	else vta.ArchiveTable 
end
 + '_' + ltrim(str(n.TIType - 10,2)),
 vta.ArchiveTable + '_Transit_' + ltrim(str(n.TIType - 10,2)),
vta.DataTypeId, vta.DateTypeName, vta.FieldNames
from @nodeIDs ids
--Описание узлов
join [dbo].[UA_Nodes] n on n.UANode_ID = ids.Id
--Типы таблиц
join [dbo].[UA_Nodes_Attributes_Variable] av on av.UANode_ID = ids.Id
join  UA_Dict_VariableValueType_To_ArchiveTables vta on vta.DataTypeId = [dbo].[usf2_UA_DetectType](av.UADataType)
where n.TIType > 10 and n.UANodeClass_ID = 2 --чтение только Variable
and av.UADataType is not null
--Результирующая таблица
CREATE TABLE #dataArchive(   
	[UANode_ID] bigint NOT NULL,
	[SourceTimeStamp] datetime NOT NULL,
	[StatusCode] bigint NOT NULL,
	[ServerTimeStamp] datetime NULL,
	[SourcePicoseconds] smallint NOT NULL,
	[ServerPicoseconds] smallint NOT NULL,
	--Данные
	[ValueBoolean] bit NULL,
	[ValueByteString] varbinary(max) NULL,
	[ValueDateTime] DateTime NULL,
	[ValueFloat] float NULL,
	[ValueGUID] uniqueidentifier NULL,
	[ValueInt16] smallint NULL,
	[ValueInt32] int NULL,
	[ValueInt64] bigint NULL,
	[ValueString] nvarchar(max) NULL,
	[ValueXMLElement] XML NULL,
	[ValueUANode_ID] bigint NULL,
	[ValueNodeID] varchar(max) NULL,
	--Массив ?
	[ArrayFormat] tinyint NULL,
	[ArrayValue] varbinary(max) NULL,
	[DataTypeDb] bigint NOT NULL, --Тип переменной
	[ArchiveTable] nvarchar(max),
	[DispatchDateTime] datetime NOT NULL,
	[ConfirmDateTime] datetime SPARSE  NULL,
	[ConfirmUser_ID] varchar(22) SPARSE  NULL,
	[ConfirmCode] int SPARSE  NULL,
	[ArchiveSourceTimeStamp] datetime NULL
)

declare @isR bit;

declare @archiveTable nvarchar(128), @ArchiveTransitTable nvarchar(128), @dataType int,	@fieldNames nvarchar(128), @sqlString NVARCHAR(4000), --Строка запроса
@parmDefinition NVARCHAR(1000) -- Параметры запроса
set @parmDefinition = N'@archiveTable nvarchar(128),@dtStart datetime,@dtEnd datetime,@dataType int';
--Выборка данных
declare t cursor LOCAL STATIC FORWARD_ONLY READ_ONLY for select distinct ArchiveTable, ArchiveTransitTable, DataType, FieldNames from #NodeByTypes
open t;
FETCH NEXT FROM t into @archiveTable, @ArchiveTransitTable, @dataType, @fieldNames
WHILE @@FETCH_STATUS = 0
BEGIN

	if (@isReadArchiveSourceTimeStamp = 1 and @fieldNames = 'ValueBoolean' and @isCurrent = 1) begin
		set @isR = 1
	end else begin
		set @isR = 0
	end;


	SET @sqlString = '
	insert into #dataArchive (UANode_ID,
	SourceTimeStamp,
	StatusCode,
	ServerTimeStamp,
	SourcePicoseconds,
	ServerPicoseconds,
	'+ @fieldNames + ',
	ArrayFormat,
	ArrayValue,
	DispatchDateTime,
	ConfirmDateTime,
	ConfirmUser_ID,
	ConfirmCode, DataTypeDb,ArchiveTable' + case when @isR = 1 then ', [ArchiveSourceTimeStamp])' else ')' end +
	'select UANode_ID,
	c.SourceTimeStamp,
	StatusCode,
	ServerTimeStamp,
	SourcePicoseconds,
	ServerPicoseconds,
	' +  case when @archiveTable like '%NodeID_%' then @fieldNames else 'Value'  end + ',
	ArrayFormat,
	ArrayValue,
	DispatchDateTime,
	ConfirmDateTime,
	ConfirmUser_ID,
	ConfirmCode,
	@dataType,
	@archiveTable' +
	case when @isR = 1 then
	',(select max(n.SourceTimeStamp) from ' + @ArchiveTransitTable + ' n
			where n.UANode_ID = c.UANode_ID and n.Value = c.Value) as ArchiveSourceTimeStamp from ' + @archiveTable + ' c ' else ' from ' + @archiveTable + ' c ' end +
	'where UANode_ID in (select UANode_ID from #NodeByTypes where ArchiveTable = @archiveTable and DataType = @dataType)'
	+ case when @isCurrent = 0 then ' and c.SourceTimeStamp between @dtStart and @dtEnd' else '' end
	--print @sqlString
	EXEC sp_executesql @sqlString, @parmDefinition, @archiveTable, @dtStart , @dtEnd, @dataType;
	FETCH NEXT FROM t into @archiveTable, @ArchiveTransitTable, @dataType, @fieldNames
end;
CLOSE t
DEALLOCATE t
select 	[UANode_ID] ,
	[SourceTimeStamp],
	[StatusCode] ,
	[ServerTimeStamp] ,
	[SourcePicoseconds] ,
	[ServerPicoseconds],
	--Данные
	[ValueBoolean] ,
	[ValueByteString] ,
	[ValueDateTime] ,
	[ValueFloat] ,
	[ValueGUID] ,
	[ValueInt16] ,
	[ValueInt32] ,
	[ValueInt64],
	[ValueString] ,
	[ValueXMLElement] ,
	[ValueUANode_ID] ,
	[ValueNodeID],
	--Массив ?
	[ArrayFormat],
	[ArrayValue] ,
	[DataTypeDb], --Тип переменной
	
	[DispatchDateTime] ,
	[ConfirmDateTime] ,
	[ConfirmUser_ID],
	[ConfirmCode] ,
	[ArchiveSourceTimeStamp]  from #dataArchive as A order by UANode_ID, SourceTimeStamp
drop table  #NodeByTypes
drop table  #dataArchive

end

go
   grant EXECUTE on usp2_UA_DataRead to [UserCalcService]
go

grant EXECUTE on usp2_UA_DataRead to UserDeclarator
go
grant EXECUTE on usp2_UA_DataRead to UserImportService
go
grant EXECUTE on usp2_UA_DataRead to UserExportService
go