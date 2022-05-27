import pandas as pd
from pandas import ExcelWriter
from pandas import ExcelFile
from google.cloud import storage
from google.cloud import bigquery
import openpyxl
from openpyxl.utils import range_boundaries
import datetime
import re
from os import getenv
import os

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

def carga(event, context):
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
    print(NombreCliente)
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