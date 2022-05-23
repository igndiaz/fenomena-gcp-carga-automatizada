INSERT INTO `proyecto-mi-dw.datawarehouse.ResultadosAutomotriz`
SELECT a.*
FROM (SELECT ROW_NUMBER() OVER() + (select count(1) from `proyecto-mi-dw.datawarehouse.ResultadosAutomotriz`) as IDResultado,
'BMW' as Cliente,
a.IDCampanaMedio,
c.IDTipoCambio,
'GoogleAds' as FuenteResultado,
a.HomologacionCampana,
a.HomologacionCampanaOriginal,
a.FechaResultado,
a.ResultadosImpresiones,
a.ResultadosClics,
null ResultadosFormularios,
a.ResultadosValorNeto,
a.ResultadosValorNeto ResultadosValorNetoCalculado  FROM
(SELECT 
CampaignName HomologacionCampana,
HomologacionCampanaOriginal,
_DATA_DATE FechaResultado ,
IDCampanaMedio, 
SUM(Impressions) ResultadosImpresiones,
TRUNC(SUM(cast(Cost/1000000 as numeric)),1) ResultadosValorNeto,
SUM(Clicks) ResultadosClics,
FROM
(select REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REGEXP_REPLACE(a.CampaignName, r"[^a-zA-Z0-9-_Ñ$.&]", ''),"Ñ","N"),"__","_"),"LEAD-ADS","LEADADS"),"CAMPANA-COVID","CAMPANACOVID"),"LINK-ADS","LINKADS"),"..",""),"NUEVA-SUCURSAL-VITACURA","NUEVASUCURSALVITACURA") CampaignName,a.CampaignName HomologacionCampanaOriginal,a._DATA_DATE,a.Impressions,a.Cost,a.Clicks ,b.IDCampanaMedio
FROM (select c.CampaignName,c._DATA_DATE,SUM(cs.Impressions) Impressions,SUM(cs.Cost) Cost,SUM(cs.Clicks) Clicks
FROM `proyecto-mi-dw.datawarehouse.Campaign_4856933837` c 
INNER JOIN `proyecto-mi-dw.datawarehouse.CampaignBasicStats_4856933837` cs
ON (c.CampaignId = cs.CampaignId AND c._DATA_DATE=cs._DATA_DATE) group by c.CampaignName,c._DATA_DATE) a
LEFT JOIN `proyecto-mi-dw.datawarehouse.CampanaMediosAutomotriz` b on REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REGEXP_REPLACE(a.CampaignName, r"[^a-zA-Z0-9-_Ñ$.&]", ''),"Ñ","N"),"__","_"),"LEAD-ADS","LEADADS"),"CAMPANA-COVID","CAMPANACOVID"),"LINK-ADS","LINKADS"),"..",""),"NUEVA-SUCURSAL-VITACURA","NUEVASUCURSALVITACURA")=b.Taxonomia and a._DATA_DATE=b.FechaMeta
UNION ALL
SELECT REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REGEXP_REPLACE(a.TAXONOMIA, r"[^a-zA-Z0-9-_Ñ$.&]", ''),"Ñ","N"),"__","_"),"LEAD-ADS","LEADADS"),"CAMPANA-COVID","CAMPANACOVID"),"LINK-ADS","LINKADS"),"..",""),"NUEVA-SUCURSAL-VITACURA","NUEVASUCURSALVITACURA") CampaignName,a.TAXONOMIA HomologacionCampanaOriginal,a.FECHA _DATA_DATE,a.IMPRESIONES Impressions,a.INVERSION Cost,a.CLICKS Clicks,b.IDCampanaMedio 
FROM `proyecto-mi-dw.datawarehouse.MAXIMORENDI_BMW` a
LEFT JOIN `proyecto-mi-dw.datawarehouse.CampanaMediosAutomotriz` b on REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REGEXP_REPLACE(a.TAXONOMIA, r"[^a-zA-Z0-9-_Ñ$.&]", ''),"Ñ","N"),"__","_"),"LEAD-ADS","LEADADS"),"CAMPANA-COVID","CAMPANACOVID"),"LINK-ADS","LINKADS"),"..",""),"NUEVA-SUCURSAL-VITACURA","NUEVASUCURSALVITACURA")=b.Taxonomia and a.FECHA=b.FechaMeta
WHERE a.TIPOCAMPANA='Performance Max'
)
GROUP BY 1,2,3,4) a
left join (select *
from `proyecto-mi-dw.datawarehouse.TiposCambio` a
where a.FechaActualizacion = (select max(FechaActualizacion) from `proyecto-mi-dw.datawarehouse.TiposCambio` b
where a.Cliente=b.Cliente and a.Industria=b.Industria and a.AnoTipoCambio=b.AnoTipoCambio and a.MesTipoCambio=b.MesTipoCambio
)) c 
on (EXTRACT(YEAR FROM a.FechaResultado) = c.AnoTipoCambio and EXTRACT(MONTH FROM a.FechaResultado) = c.MesTipoCambio and c.Industria='Automotriz' and c.Cliente='BMW')) a
LEFT JOIN `proyecto-mi-dw.datawarehouse.ResultadosAutomotriz` d on a.HomologacionCampana=d.HomologacionCampana and a.FechaResultado=d.FechaResultado
where d.HomologacionCampana is null and d.FechaResultado is null