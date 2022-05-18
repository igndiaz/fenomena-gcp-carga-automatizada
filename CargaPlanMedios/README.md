# Carga Automatizada Plan de Medios

Este repositorio contiene el código y archivo de parámetros asociado para configurar en GCP el proceso de carga automatizado de Plan de Medios.

## Protocolo de Carga
### **1. Revisión archivo Excel**

Lo primero que debemos hacer es verificar lo siguiente en el archivo de Leads a cargar:

- El archivo debe ser de extensión .xlsx (archivo de Excel)
- El nombre del archivo debe tener la estructura “Plan [Nombre Cliente] [Información Adicional].xlsx. Por ejemplo “Plan BMW - AFS - Enero 2022_v3.xlsx”. (Es crítico que entre Plan, el nombre del cliente y la información adicional exista la separación por espacios).
- El archivo debe tener una hoja con la información de plan de medios llamada __PLAN + TAXONOMIA__(Es importante que este escrito de esta misma forma).
- La tabla de información del Plan de Medios, debe comenzar después de dos espacios al comienzo.
- La tabla de información debe finalizar y después de dos espacios, aparecer el cuadrado que contenga el texto _Estructura base del plan de medios_

### **2. Construcción de archivo Parámetros**

Para la correcta ejecución del proceso de carga de Plan de Medios, se debe construir un archivo tipo JSON asociado a los parámetros de carga. En este archivo se debe definir lo siguiente:

- La industria correspondiente al cliente (*Campo Industria*)
- Diccionario con el mapeo de campos y estructura base de tabla PlanMedios (*Campo PlanMedios*)
- Diccionario con el mapeo de campos y estructura base de tabla CampanaMedios (*Campo CampanaMedios*)
- Diccionario con el mapeo de campos y estructura base de tabla RegionCampana (*Campo RegionCampana*)
- Diccionario con el mapeo de campos y estructura base de tabla Campana (*Campo Campana*)
- Diccionario con el mapeo de campos y estructura base de tabla Medios (*Campo Medios*)
- Diccionario con el mapeo de campos, asociado a columnas del tipo String que pudieran tener valores nulos (*Campo CamposStringVacios*)
- Diccionario de reemplazos a ejecutar en la Taxonomía (*Campo REEMPLAZOS*)

  
A continuación un archivo *json* de ejemplo:

```json
{
"Industria":"Automotriz",
"PlanMedios": {"CAMPAÑA":"NombrePlan","AÑO":"AnoPlan","MES / PERIODO":"MesPlan","IMPRESIONES":"MetaPlanImpresiones","CLICS":"MetaPlanClics","CPC":"MetaPlanCPC","CTR":"MetaPlanCTR","CPM":"MetaPlanCPM","FORMULARIOS":"MetaPlanFormularios","CPL":"MetaPlanCPL","VIEWS":"MetaPlanViews","CPV":"MetaPlanCPV","VALOR NETO":"MetaPlanValorNeto"},
"CampanaMedios": {"CAMPAÑA":"NombrePlan","AÑO":"AnoPlan","MES / PERIODO":"MesPlan","REGION":"RegionCampana","PAIS":"PaisCampana","SUCURSAL":"SucursalCampana","SUBCATEGORIA":"Subcategoria","TARGET":"Target","FOCO":"Foco","SOPORTE":"Soporte","FORMATO":"Formato","UBICACION":"Ubicacion","TIPO DE COMPRA":"TipoCompra","INICIO CAMPAÑA":"InicioCampana","FIN CAMPAÑA":"FinCampana","TAXONOMÍA":"Taxonomia","IMPRESIONES":"MetaImpresiones","CLICS":"MetaClics","CPC":"MetaCPC","CTR":"MetaCTR","CPM":"MetaCPM","FORMULARIOS":"MetaFormularios","CPL":"MetaCPL","VIEWS":"MetaViews","CPV":"MetaCPV","VALOR NETO":"MetaValorNeto"},
"RegionCampana":{"REGION":"RegionCampana","PAIS":"PaisCampana","SUCURSAL":"SucursalCampana"},
"Campana":{"IDRegionCampana":"IDRegionCampana","SUBCATEGORIA":"Subcategoria","TARGET":"Target","FOCO":"Foco"},
"Medios":{"SOPORTE":"Soporte","FORMATO":"Formato","UBICACION":"Ubicacion","TIPO DE COMPRA":"TipoCompra"},
"CamposStringVacios":{"SUBCATEGORIA":"Subcategoria","TARGET":"Target","FOCO":"Foco","SUCURSAL":"SucursalCampana","UBICACION":"Ubicacion","TIPO DE COMPRA":"TipoCompra"},
"Reemplazos":{"Ñ":"N","__":"_","LEAD-ADS":"LEADADS","CAMPANA-COVID":"CAMPANACOVID","LINK-ADS":"LINKADS","\\..":"","NUEVA-SUCURSAL-VITACURA":"NUEVASUCURSALVITACURA"}
}    
```

>Es importante considerar que los diccionarios deben responder al nombre de los campos en el archivo de Plan de Medios (.xls). Esto debido a que permite hacer la transformación del nombre del campo en el archivo al nombre del campo en la tabla de BigQuery.