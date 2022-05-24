# Librerias Python
import pandas as pd
from pandas import ExcelWriter
from pandas import ExcelFile
from google.cloud import storage
from google.cloud import bigquery
import openpyxl
from openpyxl.utils import range_boundaries
import json
import sendgrid
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail, Email
from python_http_client.exceptions import HTTPError
from os import getenv
import datetime
import re
import sys
import numpy as np
from datetime import timedelta
from os import getenv
import os

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

#Extraer archivo json de bucket
def leer_archivo_json(bucket,nombre):
    client = storage.Client()
    bucket = client.get_bucket(bucket)
    blob = bucket.blob(nombre)
    dest_file = f'/tmp/{nombre}'
    blob.download_to_filename(dest_file)
    
    with open(dest_file) as jsonFile:
        result = json.load(jsonFile)
        jsonFile.close()
        return result
# Retorna corte fila archivo
def filaPlanTaxonomia(worksheet):
    for cell in worksheet['C']:
        if cell.value == 'Estructura base del plan de medios':
            return (cell.row - 7)
#Extraer archivo a cargar
def extraer_archivo_carga(listado):
    a=[]
    for i in listado:
        a=i.find('Plan')
        if ( a != -1):
            break
    return listado[a]
#Extraer cliente de lista
def extraer_cliente(listado):
    a=[]
    for i in listado:
        a=i.find('Plan')
        if ( a != -1):
            break
    return listado[a].split()[1]
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
#Generar listado de bucket
def list_blobs(bucket_name):
    storage_client = storage.Client()
    blobs = storage_client.list_blobs(bucket_name)
    listado=[]
    for blob in blobs:
        listado.append(blob.name)
    return listado
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
    print(f"Carga exitosa en {tabla_destino}")

#Eliminar IDPlan de CampanaMedios
def deleteIDPlan(Tabla,IdPlan):
    bqclient = bigquery.Client()
    sql = f"""
    DELETE FROM `proyecto-mi-dw.datawarehouse.{Tabla}` where IDPlan={IdPlan};
    """
    query_job = bqclient.query(sql)
    query_job.result()
#Determinar version del plan
def versionPlan(idPlan,industria):
    bqclient = bigquery.Client()
    sql = f"""
    SELECT Version
    FROM `proyecto-mi-dw.datawarehouse.PlanMedios{industria}`
    WHERE IDPlan={idPlan}
    """
    query_job = bqclient.query(sql)
    rows=query_job.result()
    for row in rows:
        result=row.Version
    return result

#Determinar si ya existe una versión del PLan de Medios
def OldPlanMedios(dataframe,industria):
    bqclient = bigquery.Client()
    sql = f'SELECT IDPlan FROM `proyecto-mi-dw.datawarehouse.PlanMedios{industria}` WHERE IDCliente={dataframe["IDCliente"].iloc[0]} and NombrePlan="{dataframe["NombrePlan"].iloc[0]}" and AnoPlan={dataframe["AnoPlan"].iloc[0]} and MesPlan="{dataframe["MesPlan"].iloc[0]}"'
    query_job = bqclient.query(sql)
    rows=query_job.result()
    for row in rows:
        result=row.IDPlan
    if rows.total_rows == 0:
        result=0
    return result
# Función que actualiza los valores de metas de Plan de Medios
def UpdatePlanMedios(dataframe,IDEncontrado,lista,industria):
    bqclient = bigquery.Client()
    #SQL Update PlanMedios
    sql=f'UPDATE `proyecto-mi-dw.datawarehouse.PlanMedios{industria}` SET Version=Version+1,FechaCargaPlan=\'{dataframe["FechaCargaPlan"].iloc[0]}\''
    for i in lista:
        sql += f',{i}={dataframe[i].iloc[0]}'
    sql+=f' WHERE IDPlan={IDEncontrado}'
    query_job = bqclient.query(sql)
    query_job.result()
#Deja vacio el campo en el dataframe que se entregue en una lista
def string_vacio(dataframe,lista):
    for i in lista:
        dataframe[i].fillna('',inplace=True)
    return dataframe
#Utilidad crear campo con valor organico a partir de una lista
def CreateOrganico(dataframe,lista):
    for i in lista:
        dataframe.insert(0,i,"ORGANICO",True)
    return dataframe
#Permite quitar caracteres especiales de una columna
def QuitarCaracteresEspeciales(dataframe,column):
    for index,row in dataframe.iterrows():
        dataframe.loc[index,column]=re.sub(r"[^a-zA-Z0-9-_Ñ$.&]","",dataframe[column][index])
    return dataframe

#Reemplazar String
def replaceString(dataframe,columna,diccionario):
    dataframe[columna]=dataframe[columna].replace(diccionario,regex=True)
    return dataframe

#Utilidad division cero
def division_zero(n, d):
    return n / d if d else 0
    
#Obtener tablas de Consumo
def getConsumo(IDCliente,Column,Value):
    dfClientesConsumos=pd.read_gbq(f"SELECT TipoConsumo,BaseConsumo,TablaConsumo FROM `proyecto-mi-dw.datawarehouse.ClientesConsumos` where IDCliente={IDCliente}", project_id="proyecto-mi-dw")
    try:
        Maximo=dfClientesConsumos[Column].loc[dfClientesConsumos['TipoConsumo'] == Value].values[0]
    except:
        Maximo="No existe"
    return Maximo

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
    left join `proyecto-mi-dw.datawarehouse.Resultados{industria}` f using (IDCampanaMedio));
    """
    query_job = bqclient.query(sql)
    result=query_job.result()
    return result

def find_dict(diccionario,texto):
    keys = list(diccionario.keys())
    index = keys.index(texto)
    return index

#Eliminar Resultados
def deleteResultados(NombreCliente,industria):
    bqclient = bigquery.Client()
    sql = f"""
    DELETE FROM `proyecto-mi-dw.datawarehouse.Resultados{industria}` where Cliente='{NombreCliente}';
    """
    query_job = bqclient.query(sql)
    query_job.result()

# EJECUCION
# Carga de parámetros
def carga(event, context):
    if event.get('name') == f'{getenv("PARAMETROS")}.json':
        print("Solo se cargan parámetros, no hay carga en BigQuery")
    else:
        try:
            parametros=leer_archivo_json(f'{getenv("BUCKET")}',f'{getenv("PARAMETROS")}.json')
        except Exception as e:
            email_error("carga de parámetros","PlanMedios",e)

        #Determinar lista de archivos en bucket
        try:
            listado_archivos=list_blobs(f'{getenv("BUCKET")}')
        except Exception as e:
            email_error("determinar lista de archivos en bucket","PlanMedios",e)

        #Determinar path de archivo a cargar.
        try:
            path_file=extraer_archivo_carga(listado_archivos)
        except Exception as e:
            email_error("determinar path de archivo a cargar","PlanMedios",e)

        #Determinar lista de archivos en bucket
        try:
            client = storage.Client()
            bucket = client.get_bucket(f'{getenv("BUCKET")}')
            blob = bucket.blob(path_file)
            dest_file = f'/tmp/{path_file}'
            blob.download_to_filename(dest_file)
        except Exception as e:
            email_error("carga de archivo desde bucket","PlanMedios",e)
        # Encuentro Cliente y Parametros a utilizar
        try:
            cliente_carga=extraer_cliente(listado_archivos)
        except Exception as e:
            email_error("extracción de cliente desde ruta de archivo","PlanMedios",e)

        try:
            idcliente=encontrar_idCliente(cliente_carga)
        except Exception as e:
            email_error("encontrar idCliente en tabla de Clientes de BigQuery","PlanMedios",e)

        #Busco posiciones dentro de archivo Excel y calculo valores
        try:    
            workbook=openpyxl.load_workbook(dest_file,data_only=True) # ruta de archivo
            worksheetPlanTaxonomia = workbook['PLAN + TAXONOMIA']
            limitePlanTaxonomia=filaPlanTaxonomia(worksheetPlanTaxonomia)
        except Exception as e:
            email_error("detectar cantidad de campañas dentro de archivo","PlanMedios",e)

        #Genero dataframes ligados a modelo de datos.
        try:    
            dfCM = pd.DataFrame(columns = [*parametros["CampanaMedios"].keys()])
            dfPM = pd.DataFrame(columns = [*parametros["PlanMedios"].keys()])
        except Exception as e:
            email_error("creación de dataframe base según parámetros","PlanMedios",e)

        #Gnero Dataframe asociado a CampanaMedios
        try:    
            dfCampanaMedios = pd.read_excel(dest_file,sheet_name='PLAN + TAXONOMIA',header=2,nrows=limitePlanTaxonomia,na_values='-')
            dfCampanaMedios.columns=dfCampanaMedios.columns.str.strip()
            dfCampanaMedios = pd.concat([dfCM,dfCampanaMedios])[[*parametros["CampanaMedios"].keys()]]
        except Exception as e:
            email_error("creación de dataframe PlanMedios","PlanMedios",e)

        #Gnero Dataframe asociado a PlandeMedios
        #Extraigo información de metas y lo filtro por el modelo de datos.
        try:
            dfPlanMedios = pd.concat([dfPM,dfCampanaMedios])[list([*parametros["PlanMedios"].keys()])[find_dict(parametros["PlanMedios"],"IMPRESIONES"):]]
            dfPlanMedios=dfPlanMedios.sum(skipna=True).to_frame().transpose()
        except Exception as e:
            email_error("creación de dataframe PlanCliente según modelo de datos","PlanMedios",e)

        #Genero dataframe final incorporando información asociada a campaña
        try:
            dfPlanMedios=pd.concat([dfCampanaMedios[list([*parametros["PlanMedios"].keys()])[:find_dict(parametros["PlanMedios"],"IMPRESIONES")]].head(1),dfPlanMedios], axis=1)
        except Exception as e:
            email_error("creación de dataframe definitivo PlanCliente","PlanMedios",e)

        #Se asociada nombres del modelo de datos.
        try:
            dfPlanMedios=dfPlanMedios.rename(columns=parametros["PlanMedios"])
        except Exception as e:
            email_error("renombrar columnas en dataframe PlanCliente según modelo de datos","PlanMedios",e)

        #Calculo valores de indicadores
        try:
            dfPlanMedios["MetaPlanCPM"]=dfPlanMedios["MetaPlanValorNeto"]/dfPlanMedios["MetaPlanImpresiones"]*1000
            dfPlanMedios["MetaPlanCPC"]=dfPlanMedios["MetaPlanValorNeto"]/dfPlanMedios["MetaPlanClics"]
            dfPlanMedios["MetaPlanCTR"]=dfPlanMedios["MetaPlanClics"]/dfPlanMedios["MetaPlanImpresiones"]
            dfPlanMedios["MetaPlanCPL"]=division_zero(dfPlanMedios["MetaPlanValorNeto"].values,dfPlanMedios["MetaPlanFormularios"].values)
        except Exception as e:
            email_error("calculo valores indicadores plan","PlanMedios",e)
            
        #Carga Tabla Plan de Medios
        # Incorporo IDCliente y Fecha Carga del PLan
        try:
            dfPlanMedios.insert(1, 'IDCliente', idcliente)
            dfPlanMedios.insert(3, 'FechaCargaPlan',datetime.datetime.now())
            dfCampanaMedios.insert(0, 'FechaCargaPlan',datetime.datetime.now())
        except Exception as e:
            email_error("incorporación de IDCliente y FechaCargaPlan en dataframe PlanCliente","PlanMedios",e)

        #Se verifica si existe versión de Plan de Medios anterior, si existe se actualiza el Plan con valores de metas sino se inserta.
        try:
            IDPlan=OldPlanMedios(dfPlanMedios,parametros["Industria"])
        except Exception as e:
            email_error("encontrar valor asociado a IDPlanMedio","PlanMedios",e)

        try:
            dfPlanMedios.fillna(0,inplace=True)
        except Exception as e:
            email_error("convertir valores nulos a 0 en dataframe PlanCliente","PlanMedios",e)

        try:
            if IDPlan == 0:
                dfPlanMedios.insert(0, 'IDPlan',contar(f'PlanMedios{parametros["Industria"]}')+1)
                dfCampanaMedios.insert(0, 'IDPlan',contar(f'PlanMedios{parametros["Industria"]}')+1)
                dfPlanMedios.insert(3, 'Version',1)
                dfCampanaMedios.insert(1, 'Version',1)
                carga_bq(dfPlanMedios,f'proyecto-mi-dw.datawarehouse.PlanMedios{parametros["Industria"]}')
            else:
                UpdatePlanMedios(dfPlanMedios,IDPlan,list(parametros["PlanMedios"].values())[find_dict(parametros["PlanMedios"],"IMPRESIONES"):],parametros["Industria"])
                version=versionPlan(IDPlan,parametros["Industria"])
                dfPlanMedios.insert(0, 'IDPlan',IDPlan)
                dfPlanMedios.insert(3, 'Version',version)
                dfCampanaMedios.insert(0, 'IDPlan',IDPlan)
                dfCampanaMedios.insert(1, 'Version',version)
        except Exception as e:
            email_error("insertar o actualizar información de tabla PlanMedios","PlanMedios",e)

        #Siempre se inserta el Plan del archivo al historico de Plan de Medios.
        try:
            dfPlanMedios.insert(0, 'IDPlanMediosHistorico',contar(f'PlanMediosHistorico{parametros["Industria"]}')+1)
        except Exception as e:
            email_error("incorporar IDPlanMediosHistorico en dataframe PlanMedioHistorico","PlanMedios",e)
        try:
            carga_bq(dfPlanMedios,f'proyecto-mi-dw.datawarehouse.PlanMediosHistorico{parametros["Industria"]}')
        except Exception as e:
            email_error("carga de datos PlanMediosHistorico en BigQuery","PlanMedios",e)

        try: 
            dfCampanaMedios=dfCampanaMedios.rename(columns=parametros["CampanaMedios"])
            dfCampanaMedios = QuitarCaracteresEspeciales(dfCampanaMedios,"Taxonomia")
            dfCampanaMedios= replaceString(dfCampanaMedios,"Taxonomia",parametros["Reemplazos"])
            dfCampanaMedios=string_vacio(dfCampanaMedios,[*parametros["CamposStringVacios"].values()])
        except Exception as e:
            email_error("limpieza de caracteres especiales y reemplazar texto en columna Taxonomía de dataframe PlanMedios","PlanMedios",e)

        # Extraigo información de RegioCampana para incorporar a tabla de BigQuery
        try:
            dfRegionCampana=dfCampanaMedios[[*parametros["RegionCampana"].values()]].drop_duplicates()
        except Exception as e:
            email_error("creación de dataframe RegionCampana","PlanMedios",e)
        try:
            sqlString=','.join([str(elem) for elem in [*parametros["RegionCampana"].values()]])
            dfRegionCampanaBQ=pd.read_gbq(f'SELECT {sqlString} FROM `proyecto-mi-dw.datawarehouse.RegionCampana{parametros["Industria"]}`', project_id="proyecto-mi-dw")
            dfRegionCampana=pd.merge(dfRegionCampana, dfRegionCampanaBQ, how="left", indicator=True).query('_merge=="left_only"').iloc[: , :-1]
        except Exception as e:
            email_error("detectar filas existentes en tabla RegionCampana de BigQuery","PlanMedios",e)
        try:
            dfRegionCampana.insert(0, 'IDRegionCampana', range(contar(f'RegionCampana{parametros["Industria"]}')+1,contar(f'RegionCampana{parametros["Industria"]}') + len(dfRegionCampana)+1))
        except Exception as e:
            email_error("incorporación de IDRegionCampana en dataframe dfRegionCampana","PlanMedios",e)
        try:
            carga_bq(dfRegionCampana,f'proyecto-mi-dw.datawarehouse.RegionCampana{parametros["Industria"]}')
        except Exception as e:
            email_error("carga de datos RegionCampana en BigQuery","PlanMedios",e)
        try:
            dfRegionCampanasBQ=pd.read_gbq(f'SELECT * FROM `proyecto-mi-dw.datawarehouse.RegionCampana{parametros["Industria"]}`', project_id="proyecto-mi-dw")   
            dfCampanaMedios=pd.merge(dfCampanaMedios, dfRegionCampanasBQ, how="inner", on=[*parametros["RegionCampana"].values()])
        except Exception as e:
            email_error("incorporación a PlanMedios de IDRegionCampana","PlanMedios",e)

        #Carga Tabla Campanas
        try:
            dfCampanas=dfCampanaMedios[[*parametros["Campana"].values()]].drop_duplicates()
            dfCampanas.fillna('', inplace=True)
        except Exception as e:
            email_error("generar dataframe Campanas eliminando duplicados y valores nulos","PlanMedios",e)
        try:    
            sqlString=','.join([str(elem) for elem in [*parametros["Campana"].values()]])
            dfCampanasBQ=pd.read_gbq(f'SELECT {sqlString} FROM `proyecto-mi-dw.datawarehouse.Campanas{parametros["Industria"]}`', project_id="proyecto-mi-dw")
            dfCampanas=pd.merge(dfCampanas, dfCampanasBQ, how="left", indicator=True).query('_merge=="left_only"').iloc[: , :-1]
        except Exception as e:
            email_error("detectar filas existentes en tabla Campanas de BigQuery","PlanMedios",e)
        try:
            dfCampanas.insert(0, 'IDCampana', range(contar(f'Campanas{parametros["Industria"]}')+1,contar(f'Campanas{parametros["Industria"]}') + len(dfCampanas)+1))
        except Exception as e:
            email_error("generar IDCampana y renombrar columnas según modelo de datos","PlanMedios",e)
        try:    
            carga_bq(dfCampanas,f'proyecto-mi-dw.datawarehouse.Campanas{parametros["Industria"]}')
        except Exception as e:
            email_error("carga de datos Campana en BigQuery","PlanMedios",e)
        #Carga Tabla Medios
        try:
            dfMedios=dfCampanaMedios[[*parametros["Medios"].values()]].drop_duplicates()
            dfMedios.fillna('', inplace=True)
        except Exception as e:
            email_error("generar dataframe Medios eliminando duplicados","PlanMedios",e)
        try:
            sqlString=','.join([str(elem) for elem in [*parametros["Medios"].values()]])
            dfMediosBQ=pd.read_gbq(f'SELECT {sqlString} FROM `proyecto-mi-dw.datawarehouse.Medios{parametros["Industria"]}`', project_id="proyecto-mi-dw")
            dfMedios=pd.merge(dfMedios, dfMediosBQ, how="left", indicator=True).query('_merge=="left_only"').iloc[: , :-1]
        except Exception as e:
            email_error("detectar filas existentes en tabla Medios de BigQuery","PlanMedios",e)
        try:
            dfMedios.insert(0, 'IDMedio', range(contar(f'Medios{parametros["Industria"]}')+1,contar(f'Medios{parametros["Industria"]}') + len(dfMedios)+1))
            
        except Exception as e:
            email_error("generar IDMedio y renombrar columnas según modelo de datos","PlanMedios",e)
        try:
            carga_bq(dfMedios,f'proyecto-mi-dw.datawarehouse.Medios{parametros["Industria"]}')
        except Exception as e:
            email_error("carga de datos Medios en BigQuery","PlanMedios",e)

        #Extraer información Medios y Campanas
        try:
            dfCampanasBQ=pd.read_gbq(f'SELECT * FROM `proyecto-mi-dw.datawarehouse.Campanas{parametros["Industria"]}`', project_id="proyecto-mi-dw")
            dfMediosBQ=pd.read_gbq(f'SELECT * FROM `proyecto-mi-dw.datawarehouse.Medios{parametros["Industria"]}`', project_id="proyecto-mi-dw")
        except Exception as e:
            email_error("extraer información de tablas Medios y Campanas","PlanMedios",e)
        
        try:

            dfCampanaMedios=pd.merge(dfCampanaMedios, dfCampanasBQ, how="inner", on=[*parametros["Campana"].values()])
            dfCampanaMedios = pd.merge(dfCampanaMedios, dfMediosBQ, how="inner", on=[*parametros["Medios"].values()])
            dfCampanaMedios = dfCampanaMedios[["IDPlan","IDMedio","IDCampana","InicioCampana","FinCampana","Version","FechaCargaPlan","Taxonomia"]+list([*parametros["CampanaMedios"].values()])[find_dict(parametros["CampanaMedios"],"IMPRESIONES"):]]
            dfCampanaMedios.fillna(0,inplace=True)
        except Exception as e:
            email_error("generar dataframe CampanaMedios y convertir valores nulos en 0","PlanMedios",e)
        try:
            if IDPlan != 0:
                deleteIDPlan(f'CampanaMedios{parametros["Industria"]}',dfCampanaMedios["IDPlan"].iloc[0])
                deleteIDPlan(f'CampanaMediosPeriodo{parametros["Industria"]}',dfCampanaMedios["IDPlan"].iloc[0])
            else:
                print("Es un nuevo Plan de Medios a registrar")
        except Exception as e:
            email_error("eliminar datos de IDPlan en tabla CampanaMedios de BigQuery","PlanMedios",e)

        #Genero Ids de Campañas
        try:
            dfCampanaMediosIds=dfCampanaMedios[["IDPlan","IDMedio","IDCampana","Taxonomia"]].drop_duplicates()
            dfCampanaMediosIds.insert(0, 'IDCampanaMedio', range(maximo(f'CampanaMedios{parametros["Industria"]}','IDCampanaMedio')+1,maximo(f'CampanaMedios{parametros["Industria"]}','IDCampanaMedio') + len(dfCampanaMediosIds)+1))
            dfCampanaMedios=pd.merge(dfCampanaMedios, dfCampanaMediosIds, how="inner", on=["IDPlan","IDMedio","IDCampana","Taxonomia"])
        except Exception as e:
            email_error("generar IDCampanaMedio","PlanMedios",e)

        #Carga de CampanaMediosPeriodo
        try:
            carga_bq(dfCampanaMedios,f'proyecto-mi-dw.datawarehouse.CampanaMediosPeriodo{parametros["Industria"]}')
        except Exception as e:
            email_error("carga de datos CampanaMediosPeriodo en BigQuery","PlanMedios",e)

        #Calculo dias por campaña
        try:
            dfCampanaMedios["Days"]=(dfCampanaMedios["FinCampana"] - dfCampanaMedios["InicioCampana"]).apply(lambda x: x/np.timedelta64(1, 'D')).fillna(0).astype('int64')
        except Exception as e:
            email_error("generar dias de periodo Campaña","PlanMedios",e)

        # Desagregar por dias campañana medios
        try:
            dfCampanaMediosCargar = pd.DataFrame(columns = dfCampanaMedios.columns)
            for index,row in dfCampanaMedios.iterrows():
                diasPeriodo=(dfCampanaMedios["Days"][index:index+1]+1).iloc[0]
                newdf = pd.DataFrame(np.repeat(dfCampanaMedios[index:index+1].values, diasPeriodo, axis=0), columns=dfCampanaMedios.columns)
                newdf[list(parametros["CampanaMedios"].values())[find_dict(parametros["CampanaMedios"],"IMPRESIONES"):]]=newdf[list(parametros["CampanaMedios"].values())[find_dict(parametros["CampanaMedios"],"IMPRESIONES"):]]/diasPeriodo
                newdf["FechaMeta"]=newdf["InicioCampana"]
                for index,row in newdf.iterrows():
                    newdf.loc[index,"FechaMeta"]=newdf.loc[index,"InicioCampana"] + timedelta(days=index)
                dfCampanaMediosCargar = pd.concat([dfCampanaMediosCargar,newdf], ignore_index=True)
            del dfCampanaMediosCargar["Days"]
        except Exception as e:
            email_error("Desagrego por dias y divido por dias las metas","PlanMedios",e)

        try:
            carga_bq(dfCampanaMediosCargar,f'proyecto-mi-dw.datawarehouse.CampanaMedios{parametros["Industria"]}')
        except Exception as e:
            email_error("carga de datos CampanaMedios en BigQuery","PlanMedios",e)
        try:
            dfCampanaMediosIds.insert(0, 'IDCampanaMedioHistorico', range(maximo(f'CampanaMediosHistorico{parametros["Industria"]}','IDCampanaMedioHistorico')+1,maximo(f'CampanaMediosHistorico{parametros["Industria"]}','IDCampanaMedioHistorico') + len(dfCampanaMediosIds)+1))
            dfCampanaMediosCargar=pd.merge(dfCampanaMediosCargar, dfCampanaMediosIds[["IDCampanaMedio","IDCampanaMedioHistorico"]], how="inner", on=["IDCampanaMedio"])
            carga_bq(dfCampanaMediosCargar,f'proyecto-mi-dw.datawarehouse.CampanaMediosHistorico{parametros["Industria"]}')
        except Exception as e:
            email_error("carga de datos CampanaMediosHistoricos en BigQuery","PlanMedios",e)

        #Cargar resultados bigquery
        try:
            deleteResultados(cliente_carga,parametros["Industria"])
            cargar_resultados(cliente_carga,idcliente,parametros["Industria"])
            cargar_resultadosDataStudio(parametros["Industria"])
            email_exito("PlanMedios")
            print("Actualizada tabla resultados BigQuery")
        except Exception as e:
            email_error("cargar resultados BigQuery","PlanMedios",e)
        #Borro archivo a cargar
        try:
            blob.delete()
        except Exception as e:
            email_error("eliminar archivo cargado","PlanMedios",e)