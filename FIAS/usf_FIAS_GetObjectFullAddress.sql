if exists (select 1
          from sysobjects
          where  id = object_id('dbo.usf_FIAS_GetObjectFullAddress')
          and type in ('IF', 'FN', 'TF'))
   drop function dbo.usf_FIAS_GetObjectFullAddress
go

 
create function [dbo].[usf_FIAS_GetObjectFullAddress] 
(@AOID uniqueidentifier)
RETURNS varchar(800)
AS
begin
 
declare @res varchar(800)

SET @res = (SELECT  TOP 1 CASE
       WHEN Region1.OFFNAME IS NOT NULL THEN
         isnull(lower(Region1Type.SOCRNAME) + ' ', '')+ ' ' +ltrim(isnull(Region1.OFFNAME, '')   + ', ')
        ELSE
         ''
       END + CASE
        WHEN Region2.OFFNAME IS NOT NULL THEN
         isnull(lower(Region2Type.SOCRNAME) + ' ', '')+ ' ' +ltrim(isnull(Region2.OFFNAME, '')   + ', ')
        ELSE
         ''
       END + CASE
        WHEN Region3.OFFNAME IS NOT NULL THEN
         isnull(lower(Region3Type.SOCRNAME) + ' ', '')+ ' ' +ltrim(isnull(Region3.OFFNAME, '')   + ', ')
        ELSE
         ''
       END + CASE
        WHEN Region4.OFFNAME IS NOT NULL THEN
         isnull(lower(Region4Type.SOCRNAME) + ' ', '')+ ' ' +ltrim(isnull(Region4.OFFNAME, '')   + ', ')
        ELSE
         ''
       END + CASE
        WHEN Region5.OFFNAME IS NOT NULL THEN
         isnull(lower(Region5Type.SOCRNAME) + ' ', '')+ ' ' +ltrim(isnull(Region5.OFFNAME, '')   + ', ')
        ELSE
         ''
       END 
       
       AS FullAddress
   FROM
    dbo.FIAS_Object AS Region5
    LEFT OUTER JOIN dbo.FIAS_Object AS Region4
     ON Region5.PARENTAOID = Region4.AOID
    LEFT OUTER JOIN dbo.FIAS_Object AS Region3
     ON Region4.PARENTAOID = Region3.AOID
    LEFT OUTER JOIN dbo.FIAS_Object AS Region2
     ON Region3.PARENTAOID = Region2.AOID
    LEFT OUTER JOIN dbo.FIAS_Object AS Region1
     ON Region2.PARENTAOID = Region1.AOID

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
    Region5.AOID = @AOID
    AND Region5.ACTSTATUS = 1
     ORDER BY Region1.ACTSTATUS DESC,Region2.ACTSTATUS DESC,Region3.ACTSTATUS DESC,Region4.ACTSTATUS DESC,Region5.ACTSTATUS DESC,
    Region1.LIVESTATUS DESC,Region2.LIVESTATUS DESC,Region3.LIVESTATUS DESC,Region4.LIVESTATUS DESC,Region5.LIVESTATUS DESC)

  if @res is not null
  begin
  SET @res = replace(@res, ' ,', ',')

  if right(rtrim(@res),1) like ','
  begin
   SET @res = rtrim(@res)
   SET @res = left(@res, len(@res) - 1)
  end
  end

 return @res
end
go

grant EXECUTE on dbo.usf_FIAS_GetObjectFullAddress to UserDeclarator
go