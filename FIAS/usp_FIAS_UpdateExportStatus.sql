
if exists (select 1
          from sysobjects
          where  id = object_id('dbo.usp_FIAS_UpdateExportStatus')
          and type in ('P','PC'))
   drop procedure dbo.usp_FIAS_UpdateExportStatus
go


create procedure dbo.usp_FIAS_UpdateExportStatus 
@AOID uniqueidentifier,
@Status bit
as
begin
set nocount on
set quoted_identifier, ansi_nulls, ansi_warnings, arithabort, concat_null_yields_null, ansi_padding on
set numeric_roundabort off
set transaction isolation level read uncommitted
/*
��������� ������� "��������������" ��� ���������� ������� 
������ � ��������� � ������������� ���������
*/
	
DECLARE @temp table (AOID uniqueidentifier, PARENTAOID uniqueidentifier,AOGUID uniqueidentifier)
declare @resultcount int

--������ ���� ������������ �������� � ��������� ���� � � �������
--5 ������� ����������� (����, �����, �����, ����������, �����)
--...����� �� ��������
INSERT INTO @temp
(AOID,PARENTAOID,AOGUID)
--������
SELECT DISTINCT AOID,PARENTAOID,AOGUID
FROM
	FIAS_Object O1
WHERE
	AOID = @AOID
UNION
--�������� ������� 1
SELECT DISTINCT  AOID,PARENTAOID,AOGUID
FROM
	FIAS_Object O1
WHERE
	PARENTAOID = @AOID
UNION
--�������� ������� 2
SELECT DISTINCT  AOID,PARENTAOID,AOGUID
FROM
	FIAS_Object O1
WHERE
	PARENTAOID IN (SELECT AOID
				   FROM
					   FIAS_Object O1
				   WHERE
					   PARENTAOID = @AOID)
UNION
--�������� ������� 3
SELECT DISTINCT  AOID,PARENTAOID,AOGUID
FROM
	FIAS_Object O1
WHERE
	PARENTAOID IN (SELECT AOID
				   FROM
					   FIAS_Object O1
				   WHERE
					   PARENTAOID IN (SELECT AOID
									  FROM
										  FIAS_Object O1
									  WHERE
										  PARENTAOID = @AOID))
UNION
--�������� ������� 4
SELECT DISTINCT AOID,PARENTAOID,AOGUID
FROM
	FIAS_Object O1
WHERE
	PARENTAOID IN (SELECT DISTINCT O1.AOID
				   FROM
					   FIAS_Object O1
				   WHERE
					   PARENTAOID IN (SELECT AOID
									  FROM
										  FIAS_Object O1
									  WHERE
										  PARENTAOID IN (SELECT AOID
														 FROM
															 FIAS_Object O1
														 WHERE
															 PARENTAOID = @AOID)))

declare @antiStatus bit
if @status=1
	set @antiStatus=0
else 
	set @antiStatus=1
	
---��������� ������ � �������� �������: ������ ������ true
UPDATE FIAS_Object
SET
	ExportStatus = @Status
FROM
	FIAS_Object
	JOIN @temp temp
		ON temp.AOGUID = FIAS_Object.AOGUID
		

if @status=1
begin
	--��� ������������ �������� ������
	--��� ����� ������������� ������ ����������
	UPDATE FIAS_Object
	SET
		ExportStatus = 0
	FROM
		FIAS_Object
		JOIN @temp temp
			ON temp.AOGUID = FIAS_Object.AOGUID
	WHERE
		(FIAS_Object.ACTSTATUS <> 1 OR
		FIAS_Object.CURRSTATUS <> 0) AND
		isnull(FIAS_Object.ExportStatus, 1) <> 0
end

SELECT @resultcount = count(*)
FROM
	@temp


DELETE FROM @temp

-- � ������������ ������ --- ��� ���� ������������ ��������  �������� 1 ���� ��� �������� � �.�.
--�������� 1
INSERT INTO @temp
( AOID,PARENTAOID,AOGUID)
SELECT distinct  AOID,PARENTAOID,AOGUID
FROM
	FIAS_Object
WHERE
	AOID IN (SELECT PARENTAOID
			 FROM
				 FIAS_Object o2
			 WHERE
				 o2.AOID = @AOID)
UNION
--�������� 2
SELECT distinct  AOID,PARENTAOID,AOGUID
FROM
	FIAS_Object
WHERE
	AOID IN (SELECT PARENTAOID
			 FROM
				 FIAS_Object
			 WHERE
				 AOID IN (SELECT PARENTAOID
						  FROM
							  FIAS_Object
						  WHERE
							  AOID = @AOID))
UNION
--�������� 3
SELECT distinct  AOID,PARENTAOID,AOGUID
FROM
	FIAS_Object
WHERE
	AOID IN (SELECT PARENTAOID
			 FROM
				 FIAS_Object
			 WHERE
				 AOID IN (SELECT PARENTAOID
						  FROM
							  FIAS_Object
						  WHERE
							  AOID IN (SELECT PARENTAOID
									   FROM
										   FIAS_Object
									   WHERE
										   AOID = @AOID)))

UNION
--�������� 4
SELECT distinct  AOID,PARENTAOID,AOGUID
FROM
	FIAS_Object
WHERE
	AOID IN (SELECT PARENTAOID
			 FROM
				 FIAS_Object
			 WHERE
				 AOID IN (SELECT PARENTAOID
						  FROM
							  FIAS_Object
						  WHERE
							  AOID IN (SELECT PARENTAOID
									   FROM
										   FIAS_Object
									   WHERE
										   AOID IN (SELECT PARENTAOID
													FROM
														FIAS_Object
													WHERE
														AOID = @AOID))))
														

SELECT @resultcount = isnull(@resultcount, 0) + isnull((SELECT count(*)
														FROM
															@temp), 0)


--���� � ��������  ���� �������� ��� ������� true �� ������ �� null
UPDATE FIAS_Object
SET
	ExportStatus = CASE
		WHEN EXISTS (SELECT *
					 FROM
						 FIAS_Object
					 WHERE
						 FIAS_Object.PARENTAOID = temp.AOID
						 AND (isnull(FIAS_Object.ExportStatus, @antiStatus) <> @Status)
						 AND FIAS_Object.ACTSTATUS = 1
						 AND FIAS_Object.CURRSTATUS = 0) THEN
			NULL
		ELSE
			@Status
	END
FROM
	FIAS_Object
	JOIN @temp temp
		ON temp.AOGUID = FIAS_Object.AOGUID 

--��������� ��������, ������ ��� �������� �������� �������...
UPDATE FIAS_Object
SET
	ExportStatus = CASE
		WHEN EXISTS (SELECT *
					 FROM
						 FIAS_Object
					 WHERE
						 FIAS_Object.PARENTAOID = temp.AOID
						 AND (isnull(FIAS_Object.ExportStatus, @antiStatus) <> @Status)
						 AND FIAS_Object.ACTSTATUS = 1
						 AND FIAS_Object.CURRSTATUS = 0) THEN
			NULL
		ELSE
			@Status
	END
FROM
	FIAS_Object
	JOIN @temp temp
		ON temp.AOGUID = FIAS_Object.AOGUID


UPDATE FIAS_Object
SET
	ExportStatus = CASE
		WHEN EXISTS (SELECT *
					 FROM
						 FIAS_Object
					 WHERE
						 FIAS_Object.PARENTAOID = temp.AOID
						 AND (isnull(FIAS_Object.ExportStatus, @antiStatus) <> @Status)
						 AND FIAS_Object.ACTSTATUS = 1
						 AND FIAS_Object.CURRSTATUS = 0) THEN
			NULL
		ELSE
			@Status
	END
FROM
	FIAS_Object
	JOIN @temp temp
		ON temp.AOGUID = FIAS_Object.AOGUID


UPDATE FIAS_Object
SET
	ExportStatus = CASE
		WHEN EXISTS (SELECT *
					 FROM
						 FIAS_Object
					 WHERE
						 FIAS_Object.PARENTAOID = temp.AOID
						 AND (isnull(FIAS_Object.ExportStatus, @antiStatus) <> @Status)
						 AND FIAS_Object.ACTSTATUS = 1
						 AND FIAS_Object.CURRSTATUS = 0) THEN
			NULL
		ELSE
			@Status
	END
FROM
	FIAS_Object
	JOIN @temp temp
		ON temp.AOGUID = FIAS_Object.AOGUID



SELECT @resultcount

 END
go

grant EXECUTE on dbo.usp_FIAS_UpdateExportStatus to UserDeclarator
go