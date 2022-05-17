INSERT INTO `proyecto-mi-dw.datawarehouse.ResultadosAutomotriz`
SELECT a.*
FROM (SELECT ROW_NUMBER() OVER() + (select count(1) from `proyecto-mi-dw.datawarehouse.ResultadosAutomotriz`) as IDResultado,
a.IDCampanaMedio,
c.IDTipoCambio,
'FacebookAds' as FuenteResultado,
a.HomologacionCampana,
a.HomologacionCampanaOriginal,
a.FechaResultado,
a.ResultadosImpresiones,
a.ResultadosClics,
b.ResultadosFormularios,
a.ResultadosValorNeto,
TRUNC(a.ResultadosValorNeto*c.CambioUSD,1) ResultadosValorNetoCalculado  FROM
((SELECT 
campaignName HomologacionCampana,
HomologacionCampanaOriginal,
date FechaResultado ,
IDCampanaMedio, 
SUM(impressions) ResultadosImpresiones,
TRUNC(SUM(adCost),1) ResultadosValorNeto,
SUM(adClicks) ResultadosClics,
FROM
(select REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REGEXP_REPLACE(a.campaignName, r"[^a-zA-Z0-9-_Ñ$.&]", ''),"Ñ","N"),"__","_"),"LEAD-ADS","LEADADS"),"CAMPANA-COVID","CAMPANACOVID"),"LINK-ADS","LINKADS"),"..",""),"NUEVA-SUCURSAL-VITACURA","NUEVASUCURSALVITACURA") campaignName,a.campaignName HomologacionCampanaOriginal,a.date,a.impressions,a.adCost,a.adClicks,b.IDCampanaMedio
from (select campaignName ,date,sum(impressions) impressions,sum(adCost) adCost,sum(adClicks) adClicks
FROM `proyecto-mi-dw.OWOXBI_CostData_6905379b709b276e50966834aefc6d7b.Ads_CostData` group by campaignName,date) a
left join `proyecto-mi-dw.datawarehouse.CampanaMedios` b on REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REGEXP_REPLACE(a.campaignName, r"[^a-zA-Z0-9-_Ñ$.&]", ''),"Ñ","N"),"__","_"),"LEAD-ADS","LEADADS"),"CAMPANA-COVID","CAMPANACOVID"),"LINK-ADS","LINKADS"),"..",""),"NUEVA-SUCURSAL-VITACURA","NUEVASUCURSALVITACURA") =b.Taxonomia and a.date=b.FechaMeta
)
GROUP BY 1,2,3,4) a
left join (select *
from `proyecto-mi-dw.datawarehouse.TiposCambio` a
where a.FechaActualizacion = (select max(FechaActualizacion) from `proyecto-mi-dw.datawarehouse.TiposCambio` b
where a.Cliente=b.Cliente and a.Industria=b.Industria and a.AnoTipoCambio=b.AnoTipoCambio and a.MesTipoCambio=b.MesTipoCambio
)) c 
on (EXTRACT(YEAR FROM a.FechaResultado) = c.AnoTipoCambio and EXTRACT(MONTH FROM a.FechaResultado) = c.MesTipoCambio and c.Industria='Automotriz' and c.Cliente='BMW')
left join 
(SELECT Homologacion CAMPANA_REGISTRO,
Fecha,
COUNT(Homologacion) ResultadosFormularios 
FROM `proyecto-mi-dw.datawarehouse.Leads`
GROUP BY 1,2) b  
ON a.FechaResultado = b.Fecha AND a.HomologacionCampana =b.CAMPANA_REGISTRO)) a 
LEFT JOIN `proyecto-mi-dw.datawarehouse.Resultados` d on a.HomologacionCampana=d.HomologacionCampana and a.FechaResultado=d.FechaResultado
where d.HomologacionCampana is null and d.FechaResultado is null