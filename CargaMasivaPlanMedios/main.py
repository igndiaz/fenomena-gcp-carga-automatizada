import pandas as pd
from pandas import ExcelWriter
from pandas import ExcelFile
from google.cloud import storage
from google.cloud import bigquery
import openpyxl
from openpyxl.utils import range_boundaries
import sendgrid
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail, Email
from python_http_client.exceptions import HTTPError
import datetime
import re
from os import getenv
import os
import sys

#Enviar correos
def email_error(contenido,TipoCarga,error):

    sg = SendGridAPIClient(f'{getenv("SENDGRID")}')
    correo1=os.environ.get('CORREO1', 'No especificado')
    correo2=os.environ.get('CORREO2', 'No especificado')
    correo3=os.environ.get('CORREO3', 'No especificado')
    html_content = f"""\
    <html>
      <head><p style="font-size:15px;font-family:sans-serif;">Atención, </p></head>
      <body> 
          <p style="font-size:15px;font-family:sans-serif;"> Ha ocurrido un error en la carga de {TipoCarga}, asociado al proceso de <strong>{contenido}</strong>. A continuación el detalle:
        </p>
        <h2 style="font-size:15px;font-weight: bold;font-family:sans-serif;"> {error}
        </h2>
        <p style="font-size:15px;font-family:sans-serif;"> Por favor, verificar que las estructura del archivo a cargar y el archivo de parámetros estén estructurados según lo indicado en manual de carga. 
        </p>
        <p style="font-size:15px;font-family:sans-serif;"> Saludos. 
        </p>
      </body>
    </html>
    """

    message = Mail(
        to_emails=correo1,
        from_email=Email('dubraska.diaz@fenomena.cl', "Dubraska Díaz"),
        subject=f"[Atención] Error Carga de Datos {TipoCarga}",
        html_content=html_content
        )
    if correo2 != 'No especificado':
        message.add_to(correo2)
    if correo3 != 'No especificado':
        message.add_cc(correo3)

    try:
        response = sg.send(message)
#        return f"email.status_code={response.status_code}"
        sys.exit(0)
        #expected 202 Accepted

    except HTTPError as e:
        return e.message
    
def email_exito(TipoCarga):

    sg = SendGridAPIClient(f'{getenv("SENDGRID")}')
    correo1=os.environ.get('CORREO1', 'No especificado')
    correo2=os.environ.get('CORREO2', 'No especificado')
    correo3=os.environ.get('CORREO3', 'No especificado')
    html_content = f"""\
    <html>
      <head><p style="font-size:15px;font-family:sans-serif;">Felicitaciones, </p></head>
      <body> 
          <p style="font-size:15px;font-family:sans-serif;"> Se ha ejecutado correctamente el proceso carga de {TipoCarga}</p>
        <p style="font-size:15px;font-family:sans-serif;"> Por favor, verificar que la información se cargó de forma correcta en las tablas de BigQuery. 
        </p>
        <p style="font-size:15px;font-family:sans-serif;"> Saludos. 
        </p>
      </body>
    </html>
    """

    message = Mail(
        to_emails=correo1,
        from_email=Email('dubraska.diaz@fenomena.cl', "Dubraska Díaz"),
        subject=f"Proceso de Carga de Datos {TipoCarga} Exitoso",
        html_content=html_content
        )
    if correo2 != 'No especificado':
        message.add_to(correo2)
    if correo3 != 'No especificado':
        message.add_cc(correo3)

    try:
        response = sg.send(message)
        return f"email.status_code={response.status_code}"
        #expected 202 Accepted

    except HTTPError as e:
        return e.message

#Conteo tabla BigQuery
def contar(tabla):
    bqclient = bigquery.Client()
    sql = f"""
    SELECT count(1) as conteo
    FROM `proyecto-mi-dw.datawarehouse.{tabla}`;
    """
    query_job = bqclient.query(sql)
    rows=query_job.result()
    for row in rows:
        result=row.conteo
    return result

#Maximo tabla BigQuery
def maximo(tabla,campo):
    bqclient = bigquery.Client()
    sql = f"""
    SELECT IFNULL(max({campo}),0) as maximo
    FROM `proyecto-mi-dw.datawarehouse.{tabla}`;
    """
    query_job = bqclient.query(sql)
    rows=query_job.result()
    for row in rows:
        result=row.maximo
    return result

#Encontrar IDCliente
def encontrar_idCliente(dato):
    bqclient = bigquery.Client()
    sql = """
    SELECT IDCliente
    FROM `proyecto-mi-dw.datawarehouse.Clientes`
    WHERE Marca = @cliente;
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("cliente", "STRING", dato)
        ]
    )
    query_job = bqclient.query(sql, job_config=job_config)
    rows=query_job.result()
    for row in rows:
        result=row.IDCliente
    return result
#Carga de Datos en BigQuery
def carga_bq(dataframe,tabla_destino):
    client = bigquery.Client()
    job_config = bigquery.LoadJobConfig(
    write_disposition="WRITE_APPEND"
    )
    job = client.load_table_from_dataframe(
    dataframe, tabla_destino, job_config=job_config
    )  # Make an API request.
    job.result()

#Permite quitar caracteres especiales de una columna
def QuitarCaracteresEspeciales(dataframe,column):
    for index,row in dataframe.iterrows():
        dataframe.loc[index,column]=re.sub(r"[^a-zA-Z0-9-_Ñ$.&]","",dataframe[column][index])
    return dataframe

#Obtener tablas de Consumo
def getConsumo(IDCliente,Column,Value):
    dfClientesConsumos=pd.read_gbq(f"SELECT TipoConsumo,BaseConsumo,TablaConsumo FROM `proyecto-mi-dw.datawarehouse.ClientesConsumos` where IDCliente={IDCliente}", project_id="proyecto-mi-dw")
    try:
        Maximo=dfClientesConsumos[Column].loc[dfClientesConsumos['TipoConsumo'] == Value].values[0]
    except:
        Maximo="No existe"
    return Maximo

#Eliminar Resultados
def deleteResultados(NombreCliente,industria):
    bqclient = bigquery.Client()
    sql = f"""
    DELETE FROM `proyecto-mi-dw.datawarehouse.Resultados{industria}` where Cliente='{NombreCliente}';
    """
    query_job = bqclient.query(sql)
    query_job.result()

#Query para cargar resultados
def cargar_resultados(cliente_carga,idcarga,industria):
    bqclient = bigquery.Client()
    
    sqlMaximo=""
    if (getConsumo(idcarga,"TablaConsumo","MAXIMO") != 'No Existe'):
        sqlMaximo=f"""
        UNION ALL
        SELECT REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REGEXP_REPLACE(a.TAXONOMIA, r"[^a-zA-Z0-9-_Ñ$.&]", ''),"Ñ","N"),"__","_"),"LEAD-ADS","LEADADS"),"CAMPANA-COVID","CAMPANACOVID"),"LINK-ADS","LINKADS"),"..",""),"NUEVA-SUCURSAL-VITACURA","NUEVASUCURSALVITACURA") CampaignName,a.TAXONOMIA HomologacionCampanaOriginal,a.FECHA _DATA_DATE,a.IMPRESIONES Impressions,a.INVERSION Cost,a.CLICKS Clicks,b.IDCampanaMedio 
        FROM `proyecto-mi-dw.{getConsumo(idcarga,"BaseConsumo","MAXIMO")}.{getConsumo(idcarga,"TablaConsumo","MAXIMO")}` a
        LEFT JOIN `proyecto-mi-dw.datawarehouse.CampanaMedios{industria}` b on REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REGEXP_REPLACE(a.TAXONOMIA, r"[^a-zA-Z0-9-_Ñ$.&]", ''),"Ñ","N"),"__","_"),"LEAD-ADS","LEADADS"),"CAMPANA-COVID","CAMPANACOVID"),"LINK-ADS","LINKADS"),"..",""),"NUEVA-SUCURSAL-VITACURA","NUEVASUCURSALVITACURA")=b.Taxonomia and a.FECHA=b.FechaMeta
        WHERE a.TIPOCAMPANA='Performance Max'
        """
    sql = f"""
    INSERT INTO `proyecto-mi-dw.datawarehouse.Resultados{industria}`
    SELECT ROW_NUMBER() OVER()  as IDResultado,'{cliente_carga}' as Cliente,*
    FROM (
    -- FacebookAds
    SELECT 
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
    FROM `proyecto-mi-dw.{getConsumo(idcarga,"BaseConsumo","FacebookAds")}.{getConsumo(idcarga,"TablaConsumo","FacebookAds")}` group by campaignName,date) a
    left join `proyecto-mi-dw.datawarehouse.CampanaMedios{industria}` b on REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REGEXP_REPLACE(a.campaignName, r"[^a-zA-Z0-9-_Ñ$.&]", ''),"Ñ","N"),"__","_"),"LEAD-ADS","LEADADS"),"CAMPANA-COVID","CAMPANACOVID"),"LINK-ADS","LINKADS"),"..",""),"NUEVA-SUCURSAL-VITACURA","NUEVASUCURSALVITACURA") =b.Taxonomia and a.date=b.FechaMeta
    )
    GROUP BY 1,2,3,4) a
    left join (select *
    from `proyecto-mi-dw.datawarehouse.TiposCambio` a
    where a.FechaActualizacion = (select max(FechaActualizacion) from `proyecto-mi-dw.datawarehouse.TiposCambio` b
    where a.Cliente=b.Cliente and a.Industria=b.Industria and a.AnoTipoCambio=b.AnoTipoCambio and a.MesTipoCambio=b.MesTipoCambio
    )) c 
    on (EXTRACT(YEAR FROM a.FechaResultado) = c.AnoTipoCambio and EXTRACT(MONTH FROM a.FechaResultado) = c.MesTipoCambio and c.Industria='{industria}' and c.Cliente='{cliente_carga}')
    left join 
    (SELECT Homologacion CAMPANA_REGISTRO,
    Fecha,
    COUNT(Homologacion) ResultadosFormularios 
    FROM `proyecto-mi-dw.datawarehouse.Leads{industria}`
    GROUP BY 1,2) b  
    ON a.FechaResultado = b.Fecha AND a.HomologacionCampana =b.CAMPANA_REGISTRO)
    UNION ALL
    --Google Ads
    SELECT 
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
    FROM `proyecto-mi-dw.{getConsumo(idcarga,"BaseConsumo","GoogleCampaign")}.{getConsumo(idcarga,"TablaConsumo","GoogleCampaign")}` c 
    INNER JOIN `proyecto-mi-dw.{getConsumo(idcarga,"BaseConsumo","GoogleCampaignBasicStats")}.{getConsumo(idcarga,"TablaConsumo","GoogleCampaignBasicStats")}` cs
    ON (c.CampaignId = cs.CampaignId AND c._DATA_DATE=cs._DATA_DATE) group by c.CampaignName,c._DATA_DATE) a
    LEFT JOIN `proyecto-mi-dw.datawarehouse.CampanaMedios{industria}` b on REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REGEXP_REPLACE(a.CampaignName, r"[^a-zA-Z0-9-_Ñ$.&]", ''),"Ñ","N"),"__","_"),"LEAD-ADS","LEADADS"),"CAMPANA-COVID","CAMPANACOVID"),"LINK-ADS","LINKADS"),"..",""),"NUEVA-SUCURSAL-VITACURA","NUEVASUCURSALVITACURA")=b.Taxonomia and a._DATA_DATE=b.FechaMeta
    {sqlMaximo}
    )
    GROUP BY 1,2,3,4) a
    left join (select *
    from `proyecto-mi-dw.datawarehouse.TiposCambio` a
    where a.FechaActualizacion = (select max(FechaActualizacion) from `proyecto-mi-dw.datawarehouse.TiposCambio` b
    where a.Cliente=b.Cliente and a.Industria=b.Industria and a.AnoTipoCambio=b.AnoTipoCambio and a.MesTipoCambio=b.MesTipoCambio
    )) c
    on (EXTRACT(YEAR FROM a.FechaResultado) = c.AnoTipoCambio and EXTRACT(MONTH FROM a.FechaResultado) = c.MesTipoCambio and c.Industria='{industria}' and c.Cliente='{cliente_carga}'))
    """
    query_job = bqclient.query(sql)
    result=query_job.result()
    return result

#Query para crear y cargar DataStudio
def cargar_resultadosDataStudio(industria):
    bqclient = bigquery.Client()
    sql=f"""
    CREATE OR REPLACE TABLE `proyecto-mi-dw.datawarehouse.ResultadosDataStudio{industria}` AS
    (select 
    a.IDCampanaMedio,
    a.InicioCampana,
    a.FinCampana,
    a.FechaMeta,
    a.Version,
    a.FechaCargaPlan,
    a.Taxonomia,
    a.MetaImpresiones,
    a.MetaCPM,
    a.MetaClics,
    a.MetaCPC,
    a.MetaCTR,
    a.MetaFormularios,
    a.MetaCPL,
    a.MetaViews,
    a.MetaCPV,
    a.MetaValorNeto,
    b.IDRegionCampana,
    b.Subcategoria,
    b.Target,
    b.Foco,
    c.Soporte,
    c.Formato,
    c.Ubicacion,
    c.TipoCompra,
    d.IDCliente,
    d.NombrePlan,
    d.AnoPlan,
    d.MesPlan,
    f.IDTipoCambio,
    f.FuenteResultado,
    f.HomologacionCampana,
    f.HomologacionCampanaOriginal,
    f.ResultadosImpresiones,
    f.ResultadosClics,
    f.ResultadosFormularios,
    f.ResultadosValorNeto,
    f.ResultadosValorNetoCalculado
    from `proyecto-mi-dw.datawarehouse.CampanaMedios{industria}` a
    left join `proyecto-mi-dw.datawarehouse.Campanas{industria}` b using (IDCampana)
    left join `proyecto-mi-dw.datawarehouse.Medios{industria}` c using (IDMedio)
    left join `proyecto-mi-dw.datawarehouse.PlanMedios{industria}` d using (IDPlan)
    left join `proyecto-mi-dw.datawarehouse.Resultados{industria}` f on (a.IDCampanaMedio=f.IDCampanaMedio and a.FechaMeta=f.FechaResultado));
    """
    query_job = bqclient.query(sql)
    result=query_job.result()
    return result

def carga(event, context):
    try:
        path_file=f'{getenv("ARCHIVO")}.xlsx'
        client = storage.Client()
        bucket = client.get_bucket(f'{getenv("BUCKET")}')
        blob = bucket.blob(path_file)
        dest_file = f'/tmp/{path_file}'
        blob.download_to_filename(dest_file)

        # Diccionarios con nombres por tabla PlanMedios y CampanaMedios
        dict_masivoCampanaMedio={'Categoria':'NombrePlan','Region':'RegionCampana','Sucursal':'SucursalCampana','Producto':'Subcategoria','Detalle Soporte':'Formato','Meta Clicks':'MetaClics','Meta Registros':'MetaFormularios'}
        dict_masivoPlanMedio={'Categoria':'NombrePlan','Region':'RegionCampana','Sucursal':'SucursalCampana','Producto':'Subcategoria','Detalle Soporte':'Formato','Meta Clicks':'MetaPlanClics','Meta Registros':'MetaPlanFormularios','Inversión Medios':'MetaPlanValorNeto','fecha de inicio':'InicioCampana','fecha de término':'FinCampana','Nombre de Campaña':'Taxonomia'}

        # Columnas a utilizar de archivo Excel
        listaCampanaMedios=['Categoria','Region','Sucursal','Producto','Soporte','Detalle Soporte','Ciclo','Fecha','Meta Clicks','Meta Registros']
        listaPlanMedios=['Categoria','Region','Sucursal','Producto','Soporte','Detalle Soporte','Fecha','Ciclo','Meta Clicks','Meta Registros','Inversión Medios','fecha de inicio','fecha de término','Nombre de Campaña']

        #Generación dataframe PlanMedios y Carga tabla en BigQuery
        dfPlanMedios = pd.read_excel(dest_file,sheet_name='Metas',header=1,usecols=listaPlanMedios,na_values='-')
        dfPlanMedios=dfPlanMedios.rename(columns=dict_masivoPlanMedio)
        dfPlanMedios['AnoPlan'] = pd.DatetimeIndex(dfPlanMedios['Fecha']).year
        dfPlanMedios['MesPlan'] = (pd.DatetimeIndex(dfPlanMedios['Fecha']).month).astype(str)
        dict_month = {'1': 'ENERO', '2': 'FEBRERO', '3': 'MARZO', '4': 'ABRIL', '5': 'MAYO',
                    '6': 'JUNIO', '7': 'JULIO', '8': 'AGOSTO', '9': 'SEPTIEMBRE', '10': 'OCTUBRE', '11': 'NOVIEMBRE', '12': 'DICIEMBRE'}
        dfPlanMedios['MesPlan'] = dfPlanMedios['MesPlan'].apply(lambda x: dict_month[x])
        NombreCliente=getenv("CLIENTE")
        IDCliente=encontrar_idCliente(NombreCliente)
        dfPlanMedios.insert(0, 'IDCliente', IDCliente)
        dfPlanMedios.insert(2, 'Version',1)
        dfPlanMedios.insert(3, 'FechaCargaPlan',datetime.datetime.now())
        dfPlanMediosIds=dfPlanMedios[["IDCliente","NombrePlan","AnoPlan","MesPlan"]].drop_duplicates()
        dfPlanMediosIds.insert(0, 'IDPlan',range(contar(f'PlanMedios{getenv("INDUSTRIA")}')+1,contar(f'PlanMedios{getenv("INDUSTRIA")}') + len(dfPlanMediosIds)+1))
        dfPlanMedios=pd.merge(dfPlanMedios, dfPlanMediosIds, how="inner", on=["IDCliente","NombrePlan","AnoPlan","MesPlan"])
        dfPlanMediosCarga=dfPlanMedios[['IDPlan','IDCliente','NombrePlan','Version','FechaCargaPlan','AnoPlan','MesPlan','MetaPlanClics','MetaPlanFormularios','MetaPlanValorNeto']]
        carga_bq(dfPlanMediosCarga.groupby(['IDPlan','IDCliente','NombrePlan','Version','FechaCargaPlan','AnoPlan','MesPlan']).sum(),f'proyecto-mi-dw.datawarehouse.PlanMedios{getenv("INDUSTRIA")}')

        #Tabla Plan Medios Historico y carga BigQuery
        dfPlanMediosCarga.insert(0, 'IDPlanMediosHistorico',contar(f'PlanMediosHistorico{getenv("INDUSTRIA")}')+1)
        carga_bq(dfPlanMediosCarga.groupby(['IDPlanMediosHistorico','IDPlan','IDCliente','NombrePlan','Version','FechaCargaPlan','AnoPlan','MesPlan']).sum(),f'proyecto-mi-dw.datawarehouse.PlanMediosHistorico{getenv("INDUSTRIA")}')

        # CampanaMedios
        dfCampanaMedios = pd.read_excel(dest_file,sheet_name='MCG',header=1,usecols=listaCampanaMedios,na_values='-')
        dfCampanaMedios=dfCampanaMedios.rename(columns=dict_masivoCampanaMedio)
        dfCampanaMedios=pd.merge(dfCampanaMedios, dfPlanMedios.drop_duplicates(['IDPlan','NombrePlan','Ciclo','Subcategoria','Soporte','Formato']), how="left", on=['NombrePlan','Ciclo','Subcategoria','Soporte','Formato','SucursalCampana','RegionCampana'])
        dfCampanaMedios=pd.merge(dfCampanaMedios,dfPlanMedios[['IDPlan','NombrePlan','Ciclo','Version','FechaCargaPlan']].drop_duplicates(), how="left", on=['NombrePlan','Ciclo'])
        dfCampanaMedios=dfCampanaMedios[['IDPlan_y','RegionCampana','SucursalCampana','Subcategoria','Soporte','Formato','InicioCampana','FinCampana','Fecha_x','Version_y','FechaCargaPlan_y','Taxonomia','MetaClics','MetaFormularios']]
        dfCampanaMedios=dfCampanaMedios.rename(columns={"IDPlan_y":"IDPlan",'Fecha_x':'Fecha','Version_y':'Version','FechaCargaPlan_y':'FechaCargaPlan'})

        # RegionCampana
        dfRegionCampana=dfCampanaMedios[["RegionCampana","SucursalCampana"]].drop_duplicates()
        dfRegionCampana.insert(1, 'PaisCampana','Chile')
        dfRegionCampana.insert(0, 'IDRegionCampana', range(contar(f'RegionCampana{getenv("INDUSTRIA")}')+1,contar(f'RegionCampana{getenv("INDUSTRIA")}') + len(dfRegionCampana)+1))
        carga_bq(dfRegionCampana,f'proyecto-mi-dw.datawarehouse.RegionCampana{getenv("INDUSTRIA")}')
        dfRegionCampanasBQ=pd.read_gbq(f'SELECT * FROM `proyecto-mi-dw.datawarehouse.RegionCampana{getenv("INDUSTRIA")}`', project_id="proyecto-mi-dw")   
        dfCampanaMedios=pd.merge(dfCampanaMedios, dfRegionCampanasBQ, how="inner", on=["RegionCampana","SucursalCampana"])

        # Campanas
        dfCampanas = dfCampanaMedios[["IDRegionCampana","Subcategoria"]].drop_duplicates()
        dfCampanas.insert(0, 'IDCampana', range(contar(f'Campanas{getenv("INDUSTRIA")}')+1,contar(f'Campanas{getenv("INDUSTRIA")}') + len(dfCampanas)+1))
        carga_bq(dfCampanas,f'proyecto-mi-dw.datawarehouse.Campanas{getenv("INDUSTRIA")}')

        # Medios
        dfMedios = dfCampanaMedios[["Soporte","Formato"]].drop_duplicates()
        dfMedios.insert(0, 'IDMedio', range(contar(f'Medios{getenv("INDUSTRIA")}')+1,contar(f'Medios{getenv("INDUSTRIA")}') + len(dfMedios)+1))
        carga_bq(dfMedios,f'proyecto-mi-dw.datawarehouse.Medios{getenv("INDUSTRIA")}')

        # Carga de CampanaMedios
        dfCampanaMediosCarga=pd.merge(dfCampanaMedios, dfCampanas, how="inner", on=["IDRegionCampana","Subcategoria"])
        dfCampanaMediosCarga = pd.merge(dfCampanaMediosCarga, dfMedios, how="inner", on=["Soporte","Formato"])
        dfCampanaMediosCarga=dfCampanaMediosCarga[["IDPlan","IDMedio","IDCampana","InicioCampana","FinCampana","Fecha","Version","FechaCargaPlan","Taxonomia","MetaClics","MetaFormularios"]]
        dfCampanaMediosCargaIds=dfCampanaMediosCarga[["IDPlan","IDMedio","IDCampana","Taxonomia"]].drop_duplicates()
        dfCampanaMediosCargaIds.insert(0, 'IDCampanaMedio', range(maximo(f'CampanaMedios{getenv("INDUSTRIA")}','IDCampanaMedio')+1,maximo(f'CampanaMedios{getenv("INDUSTRIA")}','IDCampanaMedio') + len(dfCampanaMediosCargaIds)+1))
        dfCampanaMediosCarga=pd.merge(dfCampanaMediosCarga, dfCampanaMediosCargaIds, how="inner", on=["IDPlan","IDMedio","IDCampana","Taxonomia"])
        dfCampanaMediosCarga=dfCampanaMediosCarga.rename(columns={"Fecha":"FechaMeta"})
        dfCampanaMediosCarga["Taxonomia"].fillna('', inplace=True)
        dfCampanaMediosCarga= QuitarCaracteresEspeciales(dfCampanaMediosCarga,"Taxonomia")
        carga_bq(dfCampanaMediosCarga,f'proyecto-mi-dw.datawarehouse.CampanaMedios{getenv("INDUSTRIA")}')

        # Carga de CampanaMedios
        dfCampanaMediosCarga["IDCampanaMedioHistorico"]=dfCampanaMediosCarga["IDCampanaMedio"]+contar(f'CampanaMediosHistorico{getenv("INDUSTRIA")}')
        carga_bq(dfCampanaMediosCarga,f'proyecto-mi-dw.datawarehouse.CampanaMediosHistorico{getenv("INDUSTRIA")}')
        deleteResultados(getenv("CLIENTE"),getenv("INDUSTRIA"))
        cargar_resultados(getenv("CLIENTE"),IDCliente,getenv("INDUSTRIA"))
        cargar_resultadosDataStudio(getenv("INDUSTRIA"))
        email_exito("PlanMediosMasivo")
        blob.delete()
    except Exception as e:
            email_error("Error en el proceso de carga","PlanMediosMasivo",e)