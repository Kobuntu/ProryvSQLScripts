if exists (select 1
          from sysobjects
          where  id = object_id('dbo.usp_FIAS_ExportObjectsToFullAddress')
          and type in ('P','PC'))
   drop procedure dbo.usp_FIAS_ExportObjectsToFullAddress
go


create procedure dbo.usp_FIAS_ExportObjectsToFullAddress 
@exportDate datetime, @AOLevel int
as
begin
set nocount on
set quoted_identifier, ansi_nulls, ansi_warnings, arithabort, concat_null_yields_null, ansi_padding on
set numeric_roundabort off
set transaction isolation level read uncommitted

	/*�������� ��������:
	1) ������� �� 500 �������� � ��� �� ����
	������� �� Level ����� ����� ���� ������ ������� ��-�� ��������� ������
	����� ������� � ������� �������� ���� ExportDate �� ������� � � ��������� ���� ����� ��� ������ �� ������ � �������.

	2)��������� ���� � ������� FUllADDRESS ����� AOGUID ��������� PARENTAOGUID
	�������������� ��� ����� AOGUID=FIAS_House.HOUSEGUID  PARENTAOGUID=FIAS_House.AOGUID
	� OFFNAME = HOUSENUM + BUILDNUM - ����� ��������� ��������� ������

	3)������� ������������ �� �������� CURRSTATUS=0 AND ACTSTATUS=1
	4)���� ������������ �� ������� ENDDATE IN (SELECT max(a.ENDDATE) from FIAS_House a where a.HOUSEGUID=FIAS_House.HOUSEGUID)
	5) Merge �� AOGUID
		���� ������ ��������� - ��������� ����� ������ � ����� AOID �� ������ AOGUID - � ������ ���������
		���� ������� ��� - ���������

	���� - ���������� 
	���� ���������  - �� � ������� FIAS_House �������� ����� ������ � ����� HOUSEID � ������ HOUSEGUID
	�.�. �������� ��� ���, �� ����� ��������� ������ (�� ����) � ����� �� HOUSEGUID

	6) � ����� ���������� �����

	*/

 
	--1) ��������� �������
	--�.�. ����� ������, �������� �������� AOGUID � ����� ��������� � ������� ���������

	declare @TempObject table 
	(AOID uniqueidentifier, 
	AOGUID uniqueidentifier, 
	PARENTAOGUID uniqueidentifier, 
	AOLEVEL int)

	--��������� ������ 500 �������� ��������� ������
	INSERT into @TempObject (AOGUID,AOLEVEL)
	SELECT DISTINCT TOP 500 AOGUID,AOLEVEL					  
	FROM
		FIAS_Object
	Where 
	ACTSTATUS=1 AND CURRSTATUS=0
	and (isnull(ExportStatus,1)=1)
	AND isnull(ExportDate, '1900-01-01') < @exportDate
	and AOLEVEL=@AOLevel

	--��������� ��� ������������ ������� ��� ��������� ����
	--��� ����� ���� ����� ExportStatus ��� ���
	DECLARE @i int
	SET @i=1
	WHILE (@i<7)
	Begin
			set @i=@i+1

			INSERT into @TempObject (AOGUID,AOLEVEL)
			SELECT distinct AOGUID,AOLEVEL	
			from FIAS_Object 
			where AOID IN 
					(
						--ID ������������� �������
						SELECT PARENTAOID
						FROM FIAS_Object
						Where	ACTSTATUS=1 AND CURRSTATUS=0
								AND AOGUID IN (SELECT AOGUID from @TempObject)
					)
			--������� ��� ��� � �������
			AND AOGUID NOT IN (SELECT AOGUID from @TempObject)
			--������ ��, ������� ��� �� ���� �������������� �� ��������� ���		
			AND isnull(ExportDate, '1900-01-01') < @exportDate
	END

	DECLARE @Objects FIASFullAddressType
	INSERT INTO  @Objects
		(AOGUID,
		AOLEVEL)
	select DISTINCT 
		AOGUID,
		AOLEVEL 
	from @TempObject
	order by AOLEVEL

	--��������� � ������� ��������� ������ (����������)
	UPDATE @Objects
	SET 
	STATSTATUS=null ,
	COUNTER=null,
	HOUSENUM=null,
	ESTSTATUS=null,
	BUILDNUM=null,
	STRUCNUM=null,
	STRSTATUS=null,
	HOUSEID=null,
	HOUSEGUID=null,
	PARENTGUID=SOURCE.PARENTGUID,
	FORMALNAME=SOURCE.FORMALNAME,
	REGIONCODE=SOURCE.REGIONCODE,
	AUTOCODE=SOURCE.AUTOCODE,
	AREACODE=SOURCE.AREACODE,
	CITYCODE=SOURCE.CITYCODE,
	CTARCODE=SOURCE.CTARCODE,
	PLACECODE=SOURCE.PLACECODE,
	STREETCODE=SOURCE.STREETCODE,
	EXTRCODE=SOURCE.EXTRCODE,
	SEXTCODE=SOURCE.SEXTCODE,
	OFFNAME=SOURCE.OFFNAME,
	POSTALCODE=SOURCE.POSTALCODE,
	IFNSFL=SOURCE.IFNSFL,
	TERRIFNSFL=SOURCE.TERRIFNSFL,
	IFNSUL=SOURCE.IFNSUL,
	TERRIFNSUL=SOURCE.TERRIFNSUL,
	OKATO=SOURCE.OKATO,
	OKTMO=SOURCE.OKTMO,
	UPDATEDATE=SOURCE.UPDATEDATE,
	SHORTNAME=SOURCE.SHORTNAME,
	AOLEVEL=SOURCE.AOLEVEL,
	AOID=SOURCE.AOID,
	PREVID=SOURCE.PREVID,
	NEXTID=SOURCE.NEXTID,
	CODE=SOURCE.CODE,
	PLAINCODE=SOURCE.PLAINCODE,
	ACTSTATUS=SOURCE.ACTSTATUS,
	CENTSTATUS=SOURCE.CENTSTATUS,
	OPERSTATUS=SOURCE.OPERSTATUS,
	CURRSTATUS=SOURCE.CURRSTATUS,
	STARTDATE=SOURCE.STARTDATE,
	ENDDATE=SOURCE.ENDDATE,
	NORMDOC=SOURCE.NORMDOC,
	LIVESTATUS=SOURCE.LIVESTATUS
	--FullAddress = SOURCE.FullAddress, --� Merge �����������
	--��� ������� � merge
	--ObjectLat,
	--ObjectLng
	FROM 
	@Objects as TARGET JOIN FIAS_Object as SOURCE on SOURCE.AOGUID=TARGET.AOGUID
	WHERE
	SOURCE.ACTSTATUS=1 
	AND SOURCE.CURRSTATUS=0


	----������ �� LVL ����� �� ���� ������� � ������������ ��������� ������������/�������� �������� (�.�. ������� ������������ ������� ������)	
	DECLARE @PartObjects FIASFullAddressType, @lvl int

	DECLARE lvl_cursor CURSOR FOR  	
	select distinct AOLEVEL from @Objects order by AOLEVEL  
	OPEN lvl_cursor  
	FETCH NEXT FROM lvl_cursor   INTO @lvl   
	WHILE @@FETCH_STATUS = 0  
	BEGIN  
	
		delete from @PartObjects

		insert into @PartObjects
		select * from @Objects where AOLEVEL=@lvl
		
		--------------------------------
		EXEC [dbo].[usp_FIAS_WriteFullAddress] @PartObjects
		--------------------------------

	FETCH NEXT FROM lvl_cursor   INTO @lvl
	END    
	CLOSE lvl_cursor;  
	DEALLOCATE lvl_cursor; 

	--������� ����
	DECLARE @TempHouse table 
	(HOUSEID uniqueidentifier, 
	HOUSEGUID uniqueidentifier,
	AOGUID uniqueidentifier)

	Insert INTO @TempHouse
	( HOUSEGUID, AOGUID)
	SELECT DISTINCT 				
	FIAS_House.HOUSEGUID, FIAS_House.AOGUID
	from 
	FIAS_House 
	JOIN @Objects obj ON FIAS_House.AOGUID=obj.AOGUID
	--where 
	--isnull(FIAS_House.ExportStatus,0)=1

	DECLARE @House FIASFullAddressType
	--AOGUID = FIAS_House.HOUSEGUID,		---(PK)AOGUID ��� ����� ��� FIAS_House.HOUSEGUID
	--PARENTGUID = FIAS_House.AOGUID,		---(FK)PARENTGUID ��� ����� ��� FIAS_House.AOGUID
	INSERT INTO  @House
		(HOUSEGUID, AOGUID, PARENTGUID)

	select DISTINCT 
		HOUSEGUID, HOUSEGUID, AOGUID
	from @TempHouse

	update @House
	SET 
	STATSTATUS = FIAS_House.STATSTATUS,
	COUNTER = FIAS_House.COUNTER,
	HOUSENUM = FIAS_House.HOUSENUM,
	ESTSTATUS = FIAS_House.ESTSTATUS,
	BUILDNUM = FIAS_House.BUILDNUM,
	STRUCNUM = FIAS_House.STRUCNUM,
	STRSTATUS = FIAS_House.STRSTATUS,
	HOUSEID = FIAS_House.HOUSEID,
	FORMALNAME = FIAS_House.HOUSENUM,
	REGIONCODE = null,-->���� ����� ����� ��� ����� �� �������� ��������
	AUTOCODE = null,
	AREACODE = null,
	CITYCODE = null,
	CTARCODE = null,
	PLACECODE = null,
	STREETCODE = null,
	EXTRCODE = null,
	SEXTCODE = null,--<���� ����� ����� ��� ����� �� �������� ��������
	OFFNAME = FIAS_House.HOUSENUM,
	POSTALCODE = FIAS_House.POSTALCODE,
	IFNSFL = FIAS_House.IFNSFL,
	TERRIFNSFL = FIAS_House.TERRIFNSFL,
	IFNSUL = FIAS_House.IFNSUL,
	TERRIFNSUL = FIAS_House.TERRIFNSUL,
	OKATO = FIAS_House.OKATO,
	OKTMO = FIAS_House.OKTMO,
	UPDATEDATE = FIAS_House.UPDATEDATE,
	SHORTNAME = null,
	AOLEVEL = 9999,
	AOID = null,
	PREVID = null,
	NEXTID = null,
	CODE = null,
	PLAINCODE = null,
	ACTSTATUS = null,
	CENTSTATUS = null,
	OPERSTATUS = null,
	CURRSTATUS = null,
	STARTDATE = FIAS_House.STARTDATE,
	ENDDATE = FIAS_House.ENDDATE,
	NORMDOC = FIAS_House.NORMDOC,
	LIVESTATUS = null,
	FullAddress = null,
	ObjectLat = null,
	ObjectLng = null,
	PARENTAOID = null
	FROM
		FIAS_House
		join @House house on FIAS_House.HOUSEGUID=house.HOUSEGUID
	WHERE
	FIAS_House.ENDDATE IN (SELECT max(a.ENDDATE)
							   FROM
								   FIAS_House a
							   WHERE
								   a.HOUSEGUID = FIAS_House.HOUSEGUID)
				
	---������ �����
	EXEC [dbo].[usp_FIAS_WriteFullAddress] @House						


	--��������� ���� � �������
	MERGE FIAS_Object as target  
	USING @Objects as source   
	ON target.AOGUID = source.AOGUID    
	WHEN MATCHED THEN
		UPDATE SET target.ExportDate=@exportDate;
	

	----����������� ������ - ����������� ���� ����� � ������ �� ����
	--1) ��������� ������ �� ���� ������ ������� 
	--2) ������ ��� �������� ������� PARENTGUID �� ������ �� FIAS
	DECLARE @tempUpdateNewRow table (FIAS_AOGUID uniqueidentifier, MRSK_AOGUID uniqueidentifier)

	declare @GUID Uniqueidentifier
	SET @GUID=newid()
	--�������� ��� ��������� (����� �� ���������� ������...)
	INSERT into @tempUpdateNewRow
	SELECT Distinct 
	FIAS.AOGUID [FIASGuid], 
	MRSK.AOGUID [newGUID]
	from 
	FIAS_FullAddress as FIAS, --����� ������� �� ����
	FIAS_FullAddress as MRSK --����������� ����
	where  
	FIAS.AOGUID<>MRSK.AOGUID
	AND isnull(FIAS.PARENTGUID,@GUID)=isnull(MRSK.PARENTGUID,@GUID)
	AND isnull(FIAS.HOUSENUM,'')=isnull(MRSK.HOUSENUM,'')
	AND isnull(FIAS.BUILDNUM,'')=isnull(MRSK.BUILDNUM,'')
	and isnull(FIAS.STRUCNUM,'')=isnull(MRSK.STRUCNUM,'')
	and isnull(FIAS.OFFNAME,'')=isnull(MRSK.OFFNAME ,'')
	and isnull(FIAS.ESTSTATUS,0)=isnull(MRSK.ESTSTATUS,0)
	and isnull(MRSK.IsNewRecord,0)=1 -- �������� ��� ������, ���� �� �������� (��� �������� ����� ����)

	--��������� ������ ����
	update FIAS_FullAddress
	SET 
	ObjectLat=NEW.ObjectLat,
	ObjectLng=NEW.ObjectLng,
	AOID=NEW.AOID,
	PARENTGUID=NEW.PARENTGUID
	--....
	from 
	FIAS_FullAddress  
	join @tempUpdateNewRow temp on FIAS_FullAddress.AOGUID= temp.FIAS_AOGUID
	JOIN FIAS_FullAddress NEW on NEW.AOGUID=temp.MRSK_AOGUID

	--�������� ������������ �������������� � �������� �������� �� �� �� ����
	update FIAS_FullAddress
	SET PARENTGUID=temp.FIAS_AOGUID
	from 
	FIAS_FullAddress F join @tempUpdateNewRow temp on temp.MRSK_AOGUID= F.PARENTGUID

	--������ ������� ����������� � ��� ������ ������� ���� � @tempUpdateNewRow (�������� � ��� ��� �� ������ ����)
	DELETE FROM FIAS_FullAddress
	where AOGUID in (SELECT MRSK_AOGUID from @tempUpdateNewRow) 
	and IsNewRecord=1

	select count(*) from @Objects

end
go

grant EXECUTE on dbo.usp_FIAS_ExportObjectsToFullAddress to UserDeclarator
go





if exists (select 1
          from sysobjects
          where  id = object_id('dbo.usf_FIAS_GetHouse_GEOAddress')
          and type in ('IF', 'FN', 'TF'))
   drop function dbo.usf_FIAS_GetHouse_GEOAddress
go


create function dbo.usf_FIAS_GetHouse_GEOAddress 
(@HouseID uniqueidentifier)
RETURNS varchar(800)
AS
begin
	
declare @res varchar(800)

SET @res = (SELECT DISTINCT top 1 
								CASE			
							WHEN Region1.OFFNAME IS NOT NULL THEN
									ltrim(isnull(Region1.OFFNAME, '')   + ', ')
								ELSE
									''
							END + CASE
								WHEN Region2.OFFNAME IS NOT NULL THEN
									ltrim(isnull(Region2.OFFNAME, '')   + ', ')
								ELSE
									''
							END + CASE
								WHEN Region3.OFFNAME IS NOT NULL THEN
									ltrim(isnull(Region3.OFFNAME, '')   + ', ')
								ELSE
									''
							END + CASE
								WHEN Region4.OFFNAME IS NOT NULL THEN
									ltrim(isnull(Region4.OFFNAME, '')   + ', ')
								ELSE
									''
							END + CASE
								WHEN Region5.OFFNAME IS NOT NULL THEN
									ltrim(isnull(Region5.OFFNAME, '')   + ', ')
								ELSE
									''					
							END +  ltrim(isnull(House.HOUSENUM, '') +
							CASE
								WHEN House.BUILDNUM IS NOT NULL THEN
									'�'+ltrim(isnull(House.BUILDNUM, '')   + ', ')
								ELSE
									''					
							END +
							CASE
								WHEN House.STRUCNUM IS NOT NULL THEN
									'�'+ltrim(isnull(House.BUILDNUM, '')   + ', ')
								ELSE
									''					
							END
							
							 + ' ') AS FullAddress
			FROM
				dbo.FIAS_House as House
				JOIN dbo.FIAS_Object AS Region5
					ON House.AOGUID=Region5.AOGUID  and Region5.CURRSTATUS=0 and Region5.ACTSTATUS=1
				LEFT OUTER JOIN dbo.FIAS_Object AS Region4
					ON Region5.PARENTAOID = Region4.AOID   and Region4.CURRSTATUS=0 and Region4.ACTSTATUS=1
				LEFT OUTER JOIN dbo.FIAS_Object AS Region3
					ON Region4.PARENTAOID = Region3.AOID   and Region3.CURRSTATUS=0 and Region3.ACTSTATUS=1
				LEFT OUTER JOIN dbo.FIAS_Object AS Region2
					ON Region3.PARENTAOID = Region2.AOID   and Region2.CURRSTATUS=0 and Region2.ACTSTATUS=1
				LEFT OUTER JOIN dbo.FIAS_Object AS Region1
					ON Region2.PARENTAOID = Region1.AOID   and Region1.CURRSTATUS=0 and Region1.ACTSTATUS=1
					 
				LEFT OUTER JOIN dbo.FIAS_AddressObjectType AS Region5Type
					ON Region5.SHORTNAME = Region5Type.SCNAME AND Region5.AOLEVEL = Region5Type.LEVEL
				LEFT OUTER JOIN dbo.FIAS_AddressObjectType AS Region4Type
					ON Region4.SHORTNAME = Region4Type.SCNAME AND Region4.AOLEVEL = Region4Type.LEVEL
				LEFT OUTER JOIN dbo.FIAS_AddressObjectType AS Region3Type
					ON Region3.SHORTNAME = Region3Type.SCNAME AND Region3.AOLEVEL = Region3Type.LEVEL
				LEFT OUTER JOIN dbo.FIAS_AddressObjectType AS Region2Type
					ON Region2.SHORTNAME = Region2Type.SCNAME AND Region2.AOLEVEL = Region2Type.LEVEL
				LEFT OUTER JOIN dbo.FIAS_AddressObjectType AS Region1Type
					ON Region1.SHORTNAME = Region1Type.SCNAME AND Region1.AOLEVEL = Region1Type.LEVEL

			WHERE
				House.HOUSEID = @HouseID)

 
  if @res is not null
  begin
SET @res = replace(@res, ' ,', ',')

		if right(rtrim(@res),1) like ','
		begin
SET @res = rtrim(@res)
SET @res = left(@res, len(@res) - 1)
		end
  end

  if @res=''
  set @res='��� ������'

	return @res
end
go

grant EXECUTE on dbo.usf_FIAS_GetHouse_GEOAddress to UserDeclarator
go