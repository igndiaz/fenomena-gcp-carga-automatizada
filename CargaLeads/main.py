# Librerias Python
import pandas as pd
from pandas import ExcelWriter
from pandas import ExcelFile
from google.cloud import storage
from google.cloud import bigquery
import openpyxl
import json
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail, Email
from python_http_client.exceptions import HTTPError
import sendgrid
import re
import sys
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

# Leer archivos json
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

# Leer archivos xlsx
def leer_archivo_leads(bucket,nombre,cliente,campanas,parametros):
    client = storage.Client()
    bucket = client.get_bucket(bucket)
    blob = bucket.blob(nombre)
    dest_file = f'/tmp/{nombre}'
    blob.download_to_filename(dest_file)
    blob.delete()
    d = {}
    union = pd.DataFrame(columns = parametros["BASE"])
    for name in campanas:
        d[name] = pd.read_excel(dest_file,sheet_name=name,usecols=list(parametros[cliente][name].values())).rename(columns=invertir_dic(parametros[cliente][name]))
        order=list(parametros[cliente][name].keys())
        d[name]=d[name][order]
        union = pd.concat([union,d[name]], ignore_index=True)
    return union

#Generar listado de bucket
def list_blobs(bucket_name):
    storage_client = storage.Client()
    blobs = storage_client.list_blobs(bucket_name)
    listado=[]
    for blob in blobs:
        listado.append(blob.name)
    return listado
#Extraer archivo a cargar
def extraer_archivo_carga(listado):
    a=[]
    for i in listado:
        a=i.find('Leads')
        if ( a != -1):
            break
    return listado[a]
#Extraer cliente de lista
def extraer_cliente(listado):
    a=[]
    for i in listado:
        a=i.find('Leads')
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
#Encontrar hojas a cargar según parametros
def hojas_carga(json,cliente):
    hojas = json[cliente]
    listado=[]
    for key in hojas:
        listado.append(key)
    return listado
# invertir diccionario
def invertir_dic(dic):
    return {v: k for k, v in dic.items()}

#Conteo Leads
def contar_leads(industria):
    bqclient = bigquery.Client()
    sql = f"""
    SELECT count(1) as conteo
    FROM `proyecto-mi-dw.datawarehouse.Leads{industria}`;
    """
    query_job = bqclient.query(sql)
    rows=query_job.result()
    for row in rows:
        result=row.conteo
    return result

# Carga dataframe Leads hacia bigquery
def carga_leadsbq(dataframe,tabla_destino):
    client = bigquery.Client()
    job_config = bigquery.LoadJobConfig(
    write_disposition="WRITE_APPEND"
    )
    job = client.load_table_from_dataframe(
    dataframe, tabla_destino, job_config=job_config
    )  # Make an API request.
    job.result()

def QuitarCaracteresEspeciales(dataframe,column):
    for index,row in dataframe.iterrows():
        dataframe.loc[index,column]=re.sub(r"[^a-zA-Z0-9-_Ñ$.&]","",dataframe[column][index])
    return dataframe

def convert_excel_time(dataframe,columna):
    for index,row in dataframe.iterrows():
        dataframe.loc[index,columna]=pd.to_datetime('1899-12-30') + pd.to_timedelta(dataframe.loc[index,columna],'D')
    dataframe[columna] = pd.to_datetime(dataframe[columna]).dt.date
    return dataframe

#Reemplazar String
def replaceString(dataframe,columna,diccionario):
    dataframe[columna]=dataframe[columna].replace(diccionario,regex=True)
    return dataframe

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
    left join `proyecto-mi-dw.datawarehouse.Resultados{industria}` f on (a.IDCampanaMedio=f.IDCampanaMedio and a.FechaMeta=f.FechaResultado));
    """
    query_job = bqclient.query(sql)
    result=query_job.result()
    return result

#Eliminar Resultados
def deleteResultados(NombreCliente,industria):
    bqclient = bigquery.Client()
    sql = f"""
    DELETE FROM `proyecto-mi-dw.datawarehouse.Resultados{industria}` where Cliente='{NombreCliente}';
    """
    query_job = bqclient.query(sql)
    query_job.result()

#Ejecucion de pasos
# Carga de parámetros
def carga(event, context):
    if event.get('name') == f'{getenv("PARAMETROS")}.json':
        print("Solo se cargan parámetros, no hay carga en BigQuery")
    else:
        try:
            parametros=leer_archivo_json(f'{getenv("BUCKET")}',f'{getenv("PARAMETROS")}.json')
        except Exception as e:
            email_error("carga de parámetros","Leads",e)

        #Determinar lista de archivos en bucket
        try:
            listado_archivos=list_blobs(f'{getenv("BUCKET")}')
        except Exception as e:
            email_error("determinar lista de archivos en bucket","Leads",e)

        #Determinar path de archivo a cargar.
        try:
            archivo_leads=extraer_archivo_carga(listado_archivos)
        except Exception as e:
            email_error("determinar path de archivo a cargar","Leads",e)

        # Encuentro Cliente en nombre de archivo
        try:
            cliente_carga=extraer_cliente(listado_archivos)
        except Exception as e:
            email_error("extracción de cliente desde ruta de archivo","Leads",e)

        # Encuentro IDCliente en BigQuery
        try:
            idcliente=encontrar_idCliente(cliente_carga)
        except Exception as e:
            email_error("encontrar idCliente en tabla de Clientes de BigQuery","Leads",e)

        # Encuentro Hojas a cargar en dataframe
        try:
            lista_campana=hojas_carga(parametros,cliente_carga)
        except Exception as e:
            email_error("encontrar hojas de campañas a cargar en dataframe de leads","Leads",e)

        # Generar dataframe Leads a partir de información de Excel.
        try:
            dfLeads=leer_archivo_leads(f'{getenv("BUCKET")}',archivo_leads,cliente_carga,lista_campana,parametros)
            dfLeads.insert(1, 'IDCliente', idcliente)
            dfLeads["Rut"]=dfLeads["Rut"].astype(str)
            dfLeads=convert_excel_time(dfLeads,"Fecha")
        except Exception as e:
            email_error("generación de dataframe Leads a partir de Excel","Leads",e)

        #Generar dataframe CampanaMedios desde BigQuery para obtener IDCampanaMedio.
        try:
            dfCampanaMedios=pd.read_gbq(f'SELECT IDCampanaMedio,Taxonomia as Homologacion FROM `proyecto-mi-dw.datawarehouse.CampanaMedios{parametros["Industria"]}`', project_id="proyecto-mi-dw")
        except Exception as e:
            email_error("generación dataframe CampanaMedios desde BigQuery para obtener IDCampanaMedio","Leads",e)

        #Quitar carácteres especiales en campo homologación de Leads.
        try:
            dfLeads=QuitarCaracteresEspeciales(dfLeads,"Homologacion")
            dfLeads=replaceString(dfLeads,"Homologacion",parametros["REEMPLAZOS"])
        except Exception as e:
            email_error("quitar carácteres especiales a campo Homologacion en dataframe Leads","Leads",e)

        #Quitar carácteres especiales en campo homologación de Leads.
        try:
            dfLeads=pd.merge(dfLeads, dfCampanaMedios[dfCampanaMedios["Homologacion"]!="Organico"].drop_duplicates(), how="left", on=["Homologacion"])
            dfLeads["InteractionDetail"].fillna('',inplace=True)
            dfLeads["InteractionDetail"]=dfLeads["InteractionDetail"].astype(str)
        except Exception as e:
            email_error("incorporación de IDCampanaMedio medio a dataframe de Leads","Leads",e)

        #Generar dataframe a partir de Leads en BigQuery para comparar.
        try:
            sqlString=','.join([str(elem) for elem in [*parametros["BASE"]]])
            dfLeadsBQ=pd.read_gbq(f'SELECT IDCliente,IDCampanaMedio,{sqlString} FROM `proyecto-mi-dw.datawarehouse.Leads{parametros["Industria"]}`', project_id="proyecto-mi-dw")
            dfLeadsBQ["Rut"]=dfLeadsBQ["Rut"].astype(str)
        except Exception as e:
            email_error("generación dataframe a partir de datos de Leads en BigQuery","Leads",e)

        #Gemerar dataframe Leads final al realizar merge entre datos BQ y Leads de Excel.
        try:
            dfLeads=pd.merge(dfLeads, dfLeadsBQ, how="left", indicator=True).query('_merge=="left_only"')
            dfLeads = dfLeads.iloc[: , :-1]
            conteo=contar_leads(parametros["Industria"])+1
            dfLeads.insert(0, 'IDLead', range(conteo,conteo + len(dfLeads)))
        except Exception as e:
            email_error("generación dataframe Leads final mediante merge entre datos BigQuery y Leads","Leads",e)

        #Gemerar dataframe ligado a Dependencias con información dentro de Leads
        try:
            dfDependencias= pd.DataFrame(columns =parametros["DEPENDENCIAS"])
            dfDependencias=pd.concat([dfDependencias,dfLeads[["IDDependencia","IDCliente"]].drop_duplicates()])
            dfDependencias.dropna(subset=['IDDependencia'],inplace=True)
        except Exception as e:
            email_error("generación dataframe Dependencias con datos de dataframe Leads","Leads",e)

        #Generar dataframe a partir de Dependencias en BigQuery para comparar.
        try:
            dfDependenciasBQ=pd.read_gbq(f'SELECT * FROM `proyecto-mi-dw.datawarehouse.Dependencias{parametros["Industria"]}`', project_id="proyecto-mi-dw")
        except Exception as e:
            email_error("generación dataframe a partir de datos de Dependencias en BigQuery","Leads",e)

        #Generar dataframe Dependencias final al realizar merge entre datos BQ y Leads.
        try:
            dfDependencias=pd.merge(dfDependencias, dfDependenciasBQ, how="left", indicator=True).query('_merge=="left_only"')
            dfDependencias = dfDependencias.iloc[: , :-1]
        except Exception as e:
            email_error("generación dataframe definitivo con merge entre datos de BigQuery y Leads","Leads",e)

        #Carga de dataframe Dependencias en tabla correspondiente de BigQuery
        try:
            carga_leadsbq(dfDependencias,f'proyecto-mi-dw.datawarehouse.Dependencias{parametros["Industria"]}')
        except Exception as e:
            email_error("proceso de carga tabla Dependencias en BigQuery","Leads",e)

        #Carga de Leads en BigQuery
        try:
            if dfLeads.empty:
                print('Los Leads a cargar ya existen en la base de datos')
                email_exito("Leads")
            else:
                dfLeads["Rut"]=dfLeads["Rut"].astype(str)
                dfLeads["Telefono"]=dfLeads["Telefono"].astype(str)
                carga_leadsbq(dfLeads,f'proyecto-mi-dw.datawarehouse.Leads{parametros["Industria"]}')
                print("Carga exitosa")
        except Exception as e:
            email_error("proceso de carga tabla Leads en BigQuery","Leads",e)

        #delete resultados bigquery
        try:
            deleteResultados(cliente_carga,parametros["Industria"])
        except Exception as e:
            email_error("borrar resultados BigQuery","Leads",e)

        #Cargar resultados bigquery
        try:
            cargar_resultados(cliente_carga,idcliente,parametros["Industria"])
            cargar_resultadosDataStudio(parametros["Industria"])
            print("Actualizada tabla resultados BigQuery")
            email_exito("Leads")
        except Exception as e:
            email_error("cargar resultados BigQuery","Leads",e)