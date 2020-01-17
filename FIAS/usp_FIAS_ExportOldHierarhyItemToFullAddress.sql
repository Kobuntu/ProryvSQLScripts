if exists (select 1
          from sysobjects
          where  id = object_id('dbo.usp_FIAS_ExportOldHierarhyItemToFullAddress')
          and type in ('P','PC'))
   drop procedure dbo.usp_FIAS_ExportOldHierarhyItemToFullAddress
go

Create procedure [dbo].usp_FIAS_ExportOldHierarhyItemToFullAddress 
@HierLvl_ID int, @ProryvLevel tinyint
as
begin
/*
����� ����� ����� � ����� ���� ��������� �������
����� ���������� �����.
� ��� � Hierlvl2 ���� ��������� �� ������ � �.�.
� ��� ����� 
������ ��������� ������� ������ �����
� ��������� �� ������� ������������...
*/
	DECLARE 
	@exportedOFFNAME varchar(200),  
	@exportedType varchar(400),
	@newAOGUID uniqueidentifier,
	@result varchar(400),
	@FIASName varchar(400), 
	@FIASGUID uniqueidentifier,
	@FIASPARENTName varchar(400), 
	@FIASPARENTOffName varchar(400),
	@FIASPARENTGUID uniqueidentifier
	
	SET @newAOGUID=newid()
	SET @exportedOFFNAME = CASE
	WHEN @ProryvLevel = 1 THEN (SELECT StringName  FROM 	 Dict_HierLev1  WHERE 	 HierLev1_ID = @HierLvl_ID)	
	WHEN @ProryvLevel = 2 THEN (SELECT StringName  FROM 	 Dict_HierLev2  WHERE 	 HierLev2_ID = @HierLvl_ID)	
	WHEN @ProryvLevel = 3 THEN (SELECT StringName  FROM 	 Dict_HierLev3  WHERE 	 HierLev3_ID = @HierLvl_ID)	
	WHEN @ProryvLevel = 4 THEN (SELECT StringName  FROM 	 Dict_PS  WHERE 	 PS_ID = @HierLvl_ID) 
	ELSE '��� ������' END

	SET @exportedType = CASE
	WHEN @ProryvLevel = 1 THEN (SELECT StringName  FROM 	 Dict_Hier_Names  WHERE 	 HierLevel=1)	
	WHEN @ProryvLevel = 2 THEN (SELECT StringName  FROM 	  Dict_Hier_Names  WHERE 	 HierLevel=2)
	WHEN @ProryvLevel = 3 THEN (SELECT StringName  FROM 	 Dict_Hier_Names  WHERE 	 HierLevel=3)
	WHEN @ProryvLevel = 4 THEN '' 
	ELSE '��� ������' END
  
--print @exportedOFFNAME


	declare @FullAddress_ID int
  					

    if isnull(@exportedOFFNAME,'')=''
	begin
		set @result='������ �� ������'
	end
	--!=========== ���� ��� ������� ������� (�����-�����) ==============
	else if @ProryvLevel=1 
	BEGIN  
 			--1) ���� ������ ������� �� ������� FullAddress
 			select @FIASName=FullAddress, @FIASGUID=FIAS_FullAddress.AOGUID 
 			FROM FIAS_FullAddress join FIAS_FullAddressToHierarchy 
 			on FIAS_FullAddressToHierarchy.AOGUID=FIAS_FullAddress.AOGUID
 			where  ISNULL(HierLev1_ID,0)=@HierLvl_ID
 			if  (@FIASName is not null)
 			begin
				  set @result ='��� ������� ����� ���������� ������� � �������� ����: '+@FIASName + ', ID = '+convert(varchar(100),@FIASGUID)
				  set @newAOGUID=@FIASGUID
 			end
 			--2) ���� ������ ��� - ���� �� �������� ����� ������� <=7 ��� parent=null
 			else begin  
 			
					  select 
  						@FIASName=FullAddress, 
  						@FIASGUID=FIAS_FullAddress.AOGUID 
				  		FROM FIAS_FullAddress 
					  Where FullAddress like '%'+@exportedOFFNAME+'%' AND OFFNAME like @exportedOFFNAME AND AOLEVEL<=7	and PARENTGUID is null 
	 
 				--3) ���� ���� ����� �������� �� ��������� ������ �� ����	
					if  (@FIASName is  null)
					begin  			 				  
  						INSERT INTO FIAS_FullAddress (STATSTATUS , COUNTER , HOUSENUM , ESTSTATUS , BUILDNUM , STRUCNUM , STRSTATUS , HOUSEID , HOUSEGUID , AOGUID , PARENTGUID , FORMALNAME , REGIONCODE , AUTOCODE , AREACODE , CITYCODE , CTARCODE , PLACECODE , STREETCODE , EXTRCODE , SEXTCODE , OFFNAME ,
						   POSTALCODE , IFNSFL , TERRIFNSFL , IFNSUL , TERRIFNSUL , OKATO , OKTMO , UPDATEDATE , SHORTNAME , 
						   AOLEVEL , AOID , PREVID , NEXTID , CODE , PLAINCODE , ACTSTATUS , 
						   CENTSTATUS , OPERSTATUS , CURRSTATUS , 
						   STARTDATE , ENDDATE , NORMDOC , LIVESTATUS , FullAddress , ObjectLat , ObjectLng , PARENTAOID, IsNewRecord)
  						VALUES
							(NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, @newAOGUID, NULL, @exportedOFFNAME, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, @exportedOFFNAME, 
							NULL, NULL, NULL, NULL, NULL, NULL, NULL, getdate(), @exportedType, 
							1, newid(), NULL, NULL, NULL, NULL, 1, 
							NULL, NULL, 0, 
							getdate(), '2079-01-01', NULL, NULL, @exportedType+' '+@exportedOFFNAME, NULL, NULL, NULL,1)			
  						
					 end
					 else 
						set @newAOGUID=@FIASGUID
				
  					
					select @FullAddress_ID= MAX(FullAddress_ID) from FIAS_FullAddressToHierarchy
					set @FullAddress_ID=isnull(@FullAddress_ID,0)+1
	
					--� ��������� �����
  					if not exists(select top 1 1 from FIAS_FullAddressToHierarchy where AOGUID=@newAOGUID and ISNULL(HierLev1_ID,0)=@HierLvl_ID )
  					insert into FIAS_FullAddressToHierarchy(FullAddress_ID,AOGUID, HierLev1_ID) values (@FullAddress_ID,@newAOGUID,@HierLvl_ID)
  					 					 	
  					
  					set @result = '������ ������������� � ���� � ��������� ����� � ���.' 	
				   
 			end 	
	END
	ELSE if  @ProryvLevel=2
	BEGIN
			 --��� ��������� �������� ������ ���� ������ ������������ ������ ����� �� ���������
			 /*
			 ������� ������������ ������ ���� ����� ��������.. ���� ���, ����������� ������������
			 +- 1 ������� ������, ����..���
			 ������ ������� AOLEvel = �������������
			 */
			 --1) ������� ������������ ������
			 declare @HierLev1_ID int
			 select @HierLev1_ID = HierLev1_ID from Dict_HierLev2 where HierLev2_ID=@HierLvl_ID

			 --2) ������� AOGUID �� FullAddress ��� ������������� �������, ���� ��� - ������� ���� �� ���������� � ����� �������
			 select @FIASPARENTName=FullAddress, @FIASPARENTGUID=FIAS_FullAddress.AOGUID 
			 FROM FIAS_FullAddress join FIAS_FullAddressToHierarchy on FIAS_FullAddressToHierarchy.AOGUID=FIAS_FullAddress.AOGUID
			 where isnull(HierLev1_ID,0)=@HierLev1_ID 
			 
			 if  (@FIASPARENTGUID is  null)
			 begin
 				exec [usp_FIAS_ExportOldHierarhyItemToFullAddress] @HierLev1_ID,1   
 				select @FIASPARENTName=FullAddress, @FIASPARENTGUID=FIAS_FullAddress.AOGUID 
 				FROM FIAS_FullAddress join FIAS_FullAddressToHierarchy on FIAS_FullAddressToHierarchy.AOGUID=FIAS_FullAddress.AOGUID
 				where isnull(HierLev1_ID,0)=@HierLev1_ID 
			 end

			 if @FIASPARENTName is not null
			 begin

			--�� �������� � ������� 1
			--1) ���� ������ ������� �� ������� FullAddress
 				select @FIASName=FullAddress, @FIASGUID=FIAS_FullAddress.AOGUID 
 				FROM FIAS_FullAddress 
 				join FIAS_FullAddressToHierarchy on FIAS_FullAddressToHierarchy.AOGUID=FIAS_FullAddress.AOGUID
 				where isnull(HierLev2_ID,0)=@HierLvl_ID 
 				if  (@FIASName is not null)
 				begin
					set @result ='��� ������� ����� ���������� ������� � �������� ����: '+@FIASName + ', ID = '+convert(varchar(100),@FIASGUID)
					set @newAOGUID=@FIASGUID
 				end
 				
 				--2) ���� ������ ��� - ���� �� �������� ����� �������� ������� �������� ������
 				else 
				begin  	
					select 
  					@FIASName=FullAddress, 
  					@FIASGUID=AOGUID 
					from	FIAS_FullAddress 
					where FullAddress like '%'+@exportedOFFNAME+'%' AND OFFNAME like @exportedOFFNAME AND PARENTGUID=@FIASPARENTGUID and PARENTGUID is not null
 
 					--3) ���� ���� ����� �������� ��
					if  (@FIASName is  null)
					begin
  						
  						INSERT INTO FIAS_FullAddress (STATSTATUS , COUNTER , HOUSENUM , ESTSTATUS , BUILDNUM , STRUCNUM , STRSTATUS , HOUSEID , HOUSEGUID , AOGUID , PARENTGUID , FORMALNAME , REGIONCODE , AUTOCODE , AREACODE , CITYCODE , CTARCODE , PLACECODE , STREETCODE , EXTRCODE , SEXTCODE , OFFNAME ,
							POSTALCODE , IFNSFL , TERRIFNSFL , IFNSUL , TERRIFNSUL , OKATO , OKTMO , UPDATEDATE , SHORTNAME , 
							AOLEVEL , AOID , PREVID , NEXTID , CODE , PLAINCODE , ACTSTATUS , 
							CENTSTATUS , OPERSTATUS , CURRSTATUS , 
							STARTDATE , ENDDATE , NORMDOC , LIVESTATUS , FullAddress , ObjectLat , ObjectLng , PARENTAOID, IsNewRecord)
  						VALUES
							(NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, @newAOGUID, @FIASPARENTGUID, @exportedOFFNAME, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, @exportedOFFNAME, 
							NULL, NULL, NULL, NULL, NULL, NULL, NULL, getdate(), @exportedType, 
							5, newid(), NULL, NULL, NULL, NULL, 1, 
							NULL, NULL, 0, 
							getdate(), '2079-01-01', NULL, NULL,@FIASPARENTName+', '+ @exportedType+' '+@exportedOFFNAME, NULL, NULL, NULL,1)	
  	
					end 
					 else 
						set @newAOGUID=@FIASGUID
				
  					
					select @FullAddress_ID= MAX(FullAddress_ID) from FIAS_FullAddressToHierarchy
					set @FullAddress_ID=isnull(@FullAddress_ID,0)+1
					
					  					
					--� ��������� �����
  					if not exists(select top 1 1 from FIAS_FullAddressToHierarchy where AOGUID=@newAOGUID and ISNULL(HierLev2_ID,0)=@HierLvl_ID )
  					insert into FIAS_FullAddressToHierarchy(FullAddress_ID,AOGUID, HierLev2_ID) values (@FullAddress_ID,@newAOGUID,@HierLvl_ID)
  					 					 	
  					
  					set @result = '������ ������������� � ���� � ��������� ����� � ���.'  
					
 				end 

			 end
	END
	---������� 3 ��� ��� ��� 2
	ELSE if  @ProryvLevel=3
	BEGIN
 
		 --1) ������� ������������ ������
		 declare @HierLev2_ID int
		 select @HierLev2_ID = HierLev2_ID from Dict_HierLev3 where HierLev3_ID=@HierLvl_ID

		 --2) ������� AOGUID �� FullAddress ��� ������������� �������, ���� ��� - ������� ���� �� ���������� � ����� �������
		 select @FIASPARENTName=FullAddress, @FIASPARENTGUID=FIAS_FullAddress.AOGUID 
		 FROM FIAS_FullAddress 		 		 
		  join FIAS_FullAddressToHierarchy on FIAS_FullAddressToHierarchy.AOGUID=FIAS_FullAddress.AOGUID 
		  where isnull(HierLev2_ID,0)=@HierLev2_ID
		  
		 if  (@FIASPARENTGUID is  null)
		 begin
 			--���� ������������ � ���� �� ��������� - ���������
 			exec [usp_FIAS_ExportOldHierarhyItemToFullAddress] @HierLev2_ID,2  
 			
 			select @FIASPARENTName=FullAddress, @FIASPARENTGUID=FIAS_FullAddress.AOGUID 
			 FROM FIAS_FullAddress 		 		 
			  join FIAS_FullAddressToHierarchy on FIAS_FullAddressToHierarchy.AOGUID=FIAS_FullAddress.AOGUID 
			  where isnull(HierLev2_ID,0)=@HierLev2_ID
		 end

		 if @FIASPARENTName is not null and @FIASPARENTGUID is not null
		 begin
			--�� �������� � ������� 1
			--1) ���� ������ ������� �� ������� FullAddress
 			select @FIASName=FullAddress, @FIASGUID=FIAS_FullAddress.AOGUID FROM FIAS_FullAddress 
 			 join FIAS_FullAddressToHierarchy on FIAS_FullAddressToHierarchy.AOGUID=FIAS_FullAddress.AOGUID
 			 where isnull(HierLev3_ID,0)=@HierLvl_ID 
 			
 			if  (@FIASName is not null)
 			begin
				set @result ='��� ������� ����� ���������� ������� � �������� ����: '+@FIASName + ', ID = '+convert(varchar(100),@FIASGUID)
				set @newAOGUID=@FIASGUID
 			end	
 		--2) ���� ������ ��� - ���� �� �������� ����� �������� ������� ����������� ������
 			else 
			begin  	
				select 
  				@FIASName=FullAddress, 
  				@FIASGUID=AOGUID 
				from	FIAS_FullAddress 
				where FullAddress like '%'+@exportedOFFNAME+'%' AND OFFNAME like @exportedOFFNAME AND PARENTGUID=@FIASPARENTGUID	and PARENTGUID is not null
 
 				--3) ���� ���� ����� �������� �� ��������� ������ �� ����	
				if  (@FIASName is null)
				begin
  				
  				INSERT INTO FIAS_FullAddress (STATSTATUS , COUNTER , HOUSENUM , ESTSTATUS , BUILDNUM , STRUCNUM , STRSTATUS , HOUSEID , HOUSEGUID , AOGUID , PARENTGUID , FORMALNAME , REGIONCODE , AUTOCODE , AREACODE , CITYCODE , CTARCODE , PLACECODE , STREETCODE , EXTRCODE , SEXTCODE , OFFNAME ,
					POSTALCODE , IFNSFL , TERRIFNSFL , IFNSUL , TERRIFNSUL , OKATO , OKTMO , UPDATEDATE , SHORTNAME , 
					AOLEVEL , AOID , PREVID , NEXTID , CODE , PLAINCODE , ACTSTATUS , 
					CENTSTATUS , OPERSTATUS , CURRSTATUS , 
					STARTDATE , ENDDATE , NORMDOC , LIVESTATUS , FullAddress , ObjectLat , ObjectLng , PARENTAOID, IsNewRecord)
  				VALUES
					(NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, @newAOGUID, @FIASPARENTGUID, @exportedOFFNAME, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, @exportedOFFNAME, 
					NULL, NULL, NULL, NULL, NULL, NULL, NULL, getdate(), @exportedType, 
					6, newid(), NULL, NULL, NULL, NULL, 1, 
					NULL, NULL, 0, 
					getdate(), '2079-01-01', NULL, NULL, @FIASPARENTName+', '+ @exportedType+' '+@exportedOFFNAME, NULL, NULL, NULL,1)	
   	
				end 
					 else 
						set @newAOGUID=@FIASGUID
				
  					
					select @FullAddress_ID= MAX(FullAddress_ID) from FIAS_FullAddressToHierarchy
					set @FullAddress_ID=isnull(@FullAddress_ID,0)+1
					
					  					
					--� ��������� �����
  					if not exists(select top 1 1 from FIAS_FullAddressToHierarchy where AOGUID=@newAOGUID and ISNULL(HierLev3_ID,0)=@HierLvl_ID )
  					insert into FIAS_FullAddressToHierarchy(FullAddress_ID,AOGUID, HierLev3_ID) values (@FullAddress_ID,@newAOGUID,@HierLvl_ID)
  					 					 	
  					
  					set @result = '������ ������������� � ���� � ��������� ����� � ���.' 
				
						
			end
		 end
	END

	---������� 4 - ����������- ����������
	--�.�. ���� ������� ���������� - ������� �������� ����� �� ������
	-- � ��� ������ ���������� � �����+������+��������
	ELSE if  @ProryvLevel=4
	BEGIN 
	
		 --1) ������� ������������ ������
		 declare @HierLev3_ID int
		 select @HierLev3_ID = HierLev3_ID from Dict_PS where PS_ID=@HierLvl_ID

		 --2) ������� AOGUID �� FullAddress ��� ������������� �������, ���� ��� - ������� ���� �� ���������� � ����� �������
		 select @FIASPARENTName=FullAddress, 
		 @FIASPARENTGUID=FIAS_FullAddress.AOGUID, 
		 @FIASPARENTOffName=OFFNAME
		 FROM FIAS_FullAddress 
		  join FIAS_FullAddressToHierarchy on FIAS_FullAddressToHierarchy.AOGUID=FIAS_FullAddress.AOGUID 
		 where isnull(HierLev3_ID,0)=@HierLev3_ID
		 
		 if  (@FIASPARENTGUID is  null)
		 begin
 			--���� ������������ � ���� �� ��������� - ���������
 			exec [usp_FIAS_ExportOldHierarhyItemToFullAddress] @HierLev3_ID,3 
 			 
			select @FIASPARENTName=FullAddress, 
			 @FIASPARENTGUID=FIAS_FullAddress.AOGUID, 
			 @FIASPARENTOffName=OFFNAME
			 FROM FIAS_FullAddress 
			  join FIAS_FullAddressToHierarchy on FIAS_FullAddressToHierarchy.AOGUID=FIAS_FullAddress.AOGUID 
			 where isnull(HierLev3_ID,0)=@HierLev3_ID
		 end


		 if @FIASPARENTName is not null and @FIASPARENTGUID is not null
		 begin

		 -->�������� ��������(����� ������� ������ ������� � ����� � ��������� ���� ����� ��������.. ������� ��� ������ �� �����)		  
		 if len(@FIASPARENTName)>3
		 begin
			set @exportedOFFNAME=ltrim(rtrim(replace(@exportedOFFNAME,rtrim(@FIASPARENTOffName)+ ' �.','')))
			set @exportedOFFNAME=ltrim(rtrim(replace(@exportedOFFNAME,rtrim(@FIASPARENTOffName),'')))
		 end

		 
		 --�� �������� � ������� 1
		 --1) ���� ������ ������� �� ������� FullAddress
 			select @FIASName=FullAddress, @FIASGUID=FIAS_FullAddress.AOGUID FROM FIAS_FullAddress  join FIAS_FullAddressToHierarchy on FIAS_FullAddressToHierarchy.AOGUID=FIAS_FullAddress.AOGUID 
 			where isnull(PS_ID,0)=@HierLvl_ID 
 			if  (@FIASName is not null)
 			begin
				  set @result ='��� ������� ����� ���������� ������� � �������� ����: '+@FIASName + ', ID = '+convert(varchar(100),@FIASGUID)
				  set @newAOGUID=@FIASGUID
 			end
 			
 			--2) ���� ������ ��� - ���� �� �������� ����� �������� ������� ����������� ������
 			else begin  	
				  select 
  					@FIASName=FullAddress, 
  					@FIASGUID=AOGUID 
				  FROM	FIAS_FullAddress 
				  Where FullAddress like '%'+@exportedOFFNAME+'%' AND OFFNAME like @exportedOFFNAME AND PARENTGUID=@FIASPARENTGUID	and PARENTGUID is not null
 
 			--3) ���� ���� ����� �������� �� ��������� ������ �� ����	
				  if  (@FIASName is  null)
				  begin		
						declare @PSType int
						select @PSType=PSType from Dict_PS where PS_ID=@HierLvl_ID

						set @exportedType= case
									when  @PSType=2 THEN '���'
									when  @PSType=1 THEN '��. ��'
									when  @PSType=0 THEN '��'
									ELSE '' end	
									
  						INSERT INTO FIAS_FullAddress (STATSTATUS , COUNTER , HOUSENUM , ESTSTATUS , BUILDNUM , STRUCNUM , STRSTATUS , HOUSEID , HOUSEGUID , AOGUID , PARENTGUID , FORMALNAME , REGIONCODE , AUTOCODE , AREACODE , CITYCODE , CTARCODE , PLACECODE , STREETCODE , EXTRCODE , SEXTCODE , OFFNAME ,
						   POSTALCODE , IFNSFL , TERRIFNSFL , IFNSUL , TERRIFNSUL , OKATO , OKTMO , UPDATEDATE , SHORTNAME , 
						   AOLEVEL , AOID , PREVID , NEXTID , CODE , PLAINCODE , ACTSTATUS , 
						   CENTSTATUS , OPERSTATUS , CURRSTATUS , 
						   STARTDATE , ENDDATE , NORMDOC , LIVESTATUS , FullAddress , ObjectLat , ObjectLng , PARENTAOID, IsNewRecord)
  						VALUES
						   (NULL, NULL, @exportedOFFNAME, NULL, NULL, NULL, NULL, newid(), newid(), @newAOGUID, @FIASPARENTGUID, @exportedOFFNAME, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, @exportedOFFNAME, 
						   NULL, NULL, NULL, NULL, NULL, NULL, NULL, getdate(), @exportedType, 
						   9999, newid(), NULL, NULL, NULL, NULL, 1, 
						   NULL, NULL, 0, 
						   getdate(), '2079-01-01', NULL, NULL, replace(@FIASPARENTName+', '+ @exportedType+' '+@exportedOFFNAME,'  ',' '), NULL, NULL, NULL,1)	
		  
				  end 
					 else 
						set @newAOGUID=@FIASGUID
				
  					
					select @FullAddress_ID= MAX(FullAddress_ID) from FIAS_FullAddressToHierarchy
					set @FullAddress_ID=isnull(@FullAddress_ID,0)+1
					
					  					
					--� ��������� �����
  					if not exists(select top 1 1 from FIAS_FullAddressToHierarchy where AOGUID=@newAOGUID and ISNULL(PS_ID,0)=@HierLvl_ID )
  					insert into FIAS_FullAddressToHierarchy(FullAddress_ID,AOGUID, PS_ID) values (@FullAddress_ID,@newAOGUID,@HierLvl_ID)
  					 					 	
  					
  					set @result = '������ ������������� � ���� � ��������� ����� � ���.' 
			  
			  
 			end 
		 end
	END
	select 	@newAOGUID as ID, @result as Result
	--����� ���������� ����������� Fulladress
end
go

grant EXECUTE on dbo.usp_FIAS_ExportOldHierarhyItemToFullAddress to UserDeclarator
go