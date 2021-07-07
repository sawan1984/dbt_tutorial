#!/usr/bin/env python
# coding: utf-8


import platform
from hdbcli import dbapi
import pandas as pd
import os
import pyodbc
from sqlalchemy import create_engine
from datetime import date



#verify that this is a 64 bit version of Python
print ("Platform architecture: " + platform.architecture()[0])

# #engine = create_engine('hana://jod8674:KraftHeinz5@tlg2098.mykft.net:30015')
for i in ['1']:
  engine = create_engine('hana://catalysthanaprd'+i+'.mykft.net:30015')
  connection= engine.connect()
  print("Connected to ", i)


c009_query='''
SELECT * from (

select

DOCUMENT,

ISSUE_QTY,

CONFIRMED_QTY,

MATERIAL,

sum(ISSUE_QTY) over ( order by DOCUMENT ) as ATP,

SOLD_TO,

PLANT,

DATE_1,

SHIP_DATE,

ELEMENT,

INDEX,

TXTLG from (

select

A.DOCUMENT,

A.ISSUE_QTY,

A.CONFIRMED_QTY,

A.MATERIAL,

--sum(ISSUE_QTY) over ( partition by MATERIAL , plant order by INDEX ) as ATP ,

A.SOLD_TO,

A.PLANT,

A.DATE_1,

A.SHIP_DATE,

A."ELEMENT",

A."INDEX",

B.TXTLG from

(select

 DELIV_NUMB || '/' || DELIV_ITEM as DOCUMENT ,

--DELIV_ITEM as DOC_ITEM,

-1 * sum(DLV_QTY)  as ISSUE_QTY,

sum(DLV_QTY) as CONFIRMED_QTY,

MATERIAL,

SOLD_TO,

PLANT,

GI_DATE as DATE_1,

SHIP_DATE ,

case when DEL_TYPE = 'ZNL' then 'StkTrsfDel' else 'DEL' end as "ELEMENT",

2 as "INDEX"

from "SAPBWP"."/BIC/AZOCNNO0900" where (GOODSMV_ST != 'C' )

and DELIV_ITEM not like '9%' and (SOLD_TO != ''or DEL_TYPE = 'ZNL') and COMP_CODE like 'U%'

and year(GI_DATE) = year(current_date) and GI_DATE between add_days(current_date,-5) and add_days(current_date,4)

 

group by

  MATERIAL,

  SOLD_TO,

  DEL_TYPE,

PLANT,

GI_DATE,

DELIV_NUMB,

DELIV_ITEM,

SHIP_DATE

 

UNION ALL

 

/*Sales Order*/

select

DOC_NUMBER || '/' ||  S_ORD_ITEM as DOCUMENT,

--S_ORD_ITEM as DOC_ITEM,

-1*sum(CORR_QTY) as ISSUE_QTY,

SUM(CONF_QTY) as CONFIRMED_QTY,

MATERIAL,

SOLD_TO,

PLANT,

"MATAV_DATE" as DATE_1,

'' as SHIP_DATE,

'SO' as "ELEMENT",

3 as "INDEX"

from "SAPBWP"."/BIC/AZOCNNO1400" where  "MATAV_DATE" between add_days(current_date,-5) and add_days(current_date,4) and PLANT != ''

and year("MATAV_DATE") = year(current_date)  and COMP_CODE like 'U%' and DOC_NUMBER not in ( select distinct DOC_NUMBER from "SAPBWP"."/BIC/AZOCNNO0900")

--and 

--MATERIAL like '%000000130000000300' and PLANT = '0021'

--and FORWAGENT = ''

--and REQ_QTY = 0  

  group by

 DOC_NUMBER,

  S_ORD_ITEM,

  MATERIAL,

  SOLD_TO,

PLANT,

"MATAV_DATE"

 

UNION ALL

 

/*Reciepts, RRP4*/

select

"/BIC/ZREQELMNT" as "DOCUMENT",

sum("/BIC/ZREQM_QTY") as  "ISSUE_QTY",

sum("/BIC/ZREQM_QTY")  as "CONFIRMED_QTY",

"MATERIAL",

'' AS SOLD_TO,

"/BIC/ZTARG_LOC" as "PLANT",

"/BIC/ZREQDT" as  DATE_1,

'' as SHIP_DATE,

"/BIC/ZCATNMREQ" as "ELEMENT",

1 as "INDEX"

from "SAPBWP"."/BIC/AZOCNNO0R00" where  "/BIC/ZREQDT" between add_days(current_date,-5) and add_days(current_date,4)

and  "/BIC/ZALD_DATE" = current_date and

--MATERIAL like '%210000075300' and "/BIC/ZTARG_LOC" = '0021' and

left("/BIC/ZEND_TIME",2) =  (select max(left("/BIC/ZEND_TIME",2)) from  "SAPBWP"."/BIC/AZOCNNO0R00" where  "/BIC/ZALD_DATE" = current_date )

 

and "/BIC/ZCATNMREQ" in ('SNP: PReq','SLocSt','DEP: PReq','InspctnStk')

group by

--DOC_NUMBER,

--DOC_ITEM,

MATERIAL,

"/BIC/ZREQELMNT",

"/BIC/ZREQDT",

"/BIC/ZTARG_LOC",

"/BIC/ZCATNMREQ"

 

UNION ALL

 

select

A.EBELN || '/' || A.EBELP  as DOCUMENT,

--A.EBELP as DOC_ITEM,

-1 * sum(A.MENGE)  as ISSUE_QTY,

sum(A.MNG02) as CONFIRMED_QTY,

C.MATNR as MATERIAL,

'' as SOLD_TO,

B.RESWK as PLANT,

A.MBDAT as DATE_1,

'' as SHIP_DATE,

'ConRel'  as "ELEMENT",

4 as "INDEX"

from "SAP_EWP"."EKET" A left outer join "SAP_EWP"."EKKO" B on A.EBELN = B.EBELN left outer join "SAP_EWP"."EKPO" C on A.EBELP = C.EBELP and A.EBELN = C.EBELN

left outer join SAP_EWP.MARC D on D.MATNR = C.MATNR and B.RESWK = D.WERKS 

--left outer join

--"SAPBWP"."/BIC/AZSNNNO3600" E on A.EBELN = E.DOC_NUM and A.EBELP = right(E.DOC_ITEM,5)

--and A.ETENR = E.SCHED_LINE  

where

A.EBELN not in (select distinct DOC_NUMBER from "SAPBWP"."/BIC/AZOCNNO0900") and A.MBDAT between add_days(current_date,-5) and add_days(current_date,4)

--and year(GI_DATE) = '2021'

--and MATERIAL like '%210000075300' and PLANT = '0021' 

and  B.BUKRS like 'U%' and C.LOEKZ = '' and D.MTVFP != 'Y5' and C.ELIKZ = '' and C.MATNR not in ('000000000010018580','000000000010018583')

--and C.MATNR like '%210006116100' and B.RESWK = '0398'

group by

C.MATNR,

B.RESWK,

A.MBDAT,

A.EBELN,

A.EBELP,

A.ETENR

 

UNION ALL

 

select

BANFN || '/' || BNFPO as DOCUMENT,

--BNFPO as DOC_ITEM,

-1*sum(MENGE-BSMNG) as ISSUE_QTY,

sum(MNG02) as CONFIRMED_QTY,

MATNR as MATERIAL ,

'' as SOLD_TO ,

RESWK as PLANT,

FRGDT as DATE_1,

'' as SHIP_DATE,

'DEP:ConRI' as "ELEMENT",

5 as "INDEX"

from "SAP_EWP"."EBAN" where  FRGDT between add_days(current_date,-5) and add_days(current_date,12)

and LOEKZ != 'X' and MENGE > BSMNG and WERKS not like '8%' and RESWK != '' and RESWK not like '8%'

--and 

--MATERIAL like '%000000130000000300' and PLANT = '0021'

--and FORWAGENT = ''

--and REQ_QTY = 0  

  group by

BANFN,

BNFPO,

  MATNR,

RESWK,

FRGDT

 

UNION ALL

select

A.EBELN ||'/'|| A.EBELP || '/'|| A.ETENR as DOCUMENT,

sum(A.MENGE) as ISSUE_QTY,

0 as CONFIRMED_QTY,

B.MATNR as "MATERIAL",

'' as "SOLD_TO",

B.WERKS as "PLANT",

--A.ETENR,

case when A.ELDAT = '00000000' then add_days(A.EINDT,C.WEBAZ) else A.ELDAT end as DATE_1,

A.TDDAT as SHIP_DATE,

'PchOrd' as "ELEMENT",

6 as "INDEX"

from SAP_EWP.EKET A inner join SAP_EWP.EKPO B on A.EBELN = B.EBELN and A.EBELP = B.EBELP left outer join SAP_EWP.MARC C on B.MATNR = C.MATNR and B.WERKS = C.WERKS

where A.EINDT between add_days(current_date,-10) and add_days(current_date,12) and B.ELIKZ = '' and

B.WERKS not like '8%'

--and A.MENGE > 0

and A.EBELN in (select distinct DOC_NUMBER from "SAPBWP"."/BIC/AZOCNNO0900")

and B.MATNR not in ('000000000010018583','000000000010018580')

--and

--B.MATNR like '%430000866800' and B.WERKS = '4086'

--and B.WERKS = '5719' and B.MATNR = '000000210000522800'

group by

A.EBELN,

A.EBELP,

A.ETENR,

A.MENGE,

B.MATNR,

B.WERKS,

A.ELDAT,

A.EINDT,

C.WEBAZ,

A.TDDAT

 

UNION ALL

 

select

"/BIC/ZREQELMNT" as "DOCUMENT",

sum("/BIC/ZREQM_QTY") as  "ISSUE_QTY",

sum("/BIC/ZREQM_QTY")  as "CONFIRMED_QTY",

"MATERIAL",

'' AS SOLD_TO,

"/BIC/ZTARG_LOC" as "PLANT",

"/BIC/ZREQDT" as  DATE_1,

'' as SHIP_DATE,

"/BIC/ZCATNMREQ" as "ELEMENT",

0 as "INDEX"

from "SAPBWP"."/BIC/AZOCNNO0R00" where  "/BIC/ZREQDT" between add_days(current_date,-5) and add_days(current_date,4)

and  "/BIC/ZALD_DATE" = current_date

 

and

left("/BIC/ZEND_TIME",2)  = (select max(left("/BIC/ZEND_TIME",2)) from  "SAPBWP"."/BIC/AZOCNNO0R00" where  "/BIC/ZALD_DATE" = current_date )

 

and

"/BIC/ZCATNMREQ" in ('Stock')

group by

--DOC_NUMBER,

--DOC_ITEM,

MATERIAL,

"/BIC/ZREQELMNT",

"/BIC/ZREQDT",

"/BIC/ZTARG_LOC",

"/BIC/ZCATNMREQ"

 

) A left outer join "SAPBWP"."/BI0/TCUSTOMER" B on

A.SOLD_TO = B.CUSTOMER 

--where MATERIAL like '%411290770000' and PLANT = '4086'

) )order by DOCUMENT

'''

APO_query = '''
select * from "_SYS_BIC"."kraft.SCM.Logistics_Reporting/CV_XBIC_AZSNNNO1000"
where "BACKUP_DATE" = CURRENT_DATE and "APO_PLVERS" = '002' AND "CATEGORY" = 'Lunchables'
'''
date = date.today().strftime("%b-%d")

# C009 Pull
print("Fetching C009")
c009_df=pd.read_sql(c009_query, connection)
c009_filename = "C:/Users/kdu9786/Documents/KHC/cfr/c009/C009 Daily Pull "+str(date)+".xlsx"
print("Writing C009")
c009_df.to_excel(c009_filename)
print(c009_df.head(10))

# Lunchables Pull
print("Fetching APO")
APO_df=pd.read_sql(APO_query, connection)
APO_filename = filename = 'C:/Users/kdu9786/Documents/KHC/cfr/Lunchables/Lunchables_APO '+str(date)+'.xlsx'
print("Writing APO")
APO_df.to_excel(APO_filename)
print(APO_df.head(10))


