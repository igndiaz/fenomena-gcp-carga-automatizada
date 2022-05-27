# Carga Masiva Plan de Medios

Este repositorio contiene el código y archivo de parámetros asociado para configurar en GCP el proceso de carga automatizado masivo de Plan de Medios.

## Configuración de proceso

Para generar el proceso de carga masiva de Plan de Medios, se deben generar los siguientes elementos:

1. Se debe crear un bucket en GCP, para esto nos vamos al apartado de Cloud Storage y nos vamos a *Create Bucket*. 
   
   La configuración del bucket a nivel general es la siguiente:
   - __Nombre bucket__: Que responda a la nomenclatura ***bucket-plan-medios-masivo-industria-proyecto-mi-dw***. (Por ejemplo, si la nueva industria a incorporar es Retail se debiera llamar bucket-plan-medios-masivo-retail-proyecto-mi-dw).
   - __Location Type__: Region (us-east1)
   - __Storage Class__: Standard
   - __Access Control__: Uniform (Es el que viene por default)

2. Debemos crear la Cloud Function asociada a la industria en particular. Para esto nos vamos al apartado Cloud Functions en GCP y nos vamos a *Create Function*, debiendo configurar lo siguiente:
   
   **Apartado Configuración**

   __Basics__
   - Environment: 1st gen
   - Function name: Que siga la nomenclatura __CargaPlanMediosMasivoIndustria__ (Por ejemplo si es Retail, debiera llamarse _CargaPlanMediosMasivoRetail_)
   - __Region__: us-east1 (Lo ideal es que sea igual a la del bucket asociado a la industria)
  
    __Trigger__

    - __Trigger type__: Cloud Storage
    - __Event type__: Finalize/Create
    - __Bucket__: Debe corresponder al bucket creado previamente para la industria (bucket-plan-medios-masivo-industria-proyecto-mi-dw)

    __Runtime, build, connections and security settings__
    - __Memory allocated__: 512 MB
    - __Timeout__: 540
    - __Runtime service account__: App Engine default service account
  
    __Autoscaling__
    - __Minimum number of instances__: 0
    - __Maximum number of instances__: 3

    __Runtime environment variables__

    En esta sección se deben crear cinco variables asociadas al entorno de ejecución, estas son:
    - __INDUSTRIA__: Se debe definir la industria asociada al cliente .
    - __BUCKET__: Esta variable referencia en el script el bucket generado para la industria de esta Cloud Function.
    - __ARCHIVO__: Se debe ingresar el nombre del archivo a cargar con el script.
    - __CLIENTE__: Acá se debe ingresar el nombre del cliente asociado al archivo, correspondiente al nombre en la tabla Clientes.
  
3. Creadas las componentes de GCP, se debe proceder a crear las siguientes tablas en BigQuery. 
- PlanMediosIndustria
- PlanMediosIndustriaHistorico
- CampanasMediosIndustria
- CampanaMediosIndustriaHistorico
- CampanaMediosIndustriaPeriodo
- MediosIndustria
- CampanaIndustria
- RegionCampanaIndustria
- ResultadosIndustria
  
Para crear estas tablas se pueden ayudar en los DDL dentro del directorio SQL de este repositorio. La idea es que las tablas estén creadas con la nomenclatura asociada a la industria (Por ejemplo para la industria del Retail, debieramos crear tablas llamadas PlanMediosRetail y CampanaMediosRetail)

### Proceso de Carga

Ya con la construcción de los componentes listo. Se deben realizar los siguientes pasos:

1. Cargar el archivo de carga masiva asociado al Plan de Medios (.xlsx) al mismo bucket definido como trigger para la Cloud Function.

2. Verificar en la sección de logs de la Cloud Function, que exista un status ok de la ejecución de la función.