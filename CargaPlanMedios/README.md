# Carga Automatizada Plan de Medios

Este repositorio contiene el código y archivo de parámetros asociado para configurar en GCP el proceso de carga automatizado de Plan de Medios.

## Protocolo de Carga
### **1. Revisión archivo Excel**

Lo primero que debemos hacer es verificar lo siguiente en el archivo de Leads a cargar:

- El archivo debe ser de extensión .xlsx (archivo de Excel)
- El nombre del archivo debe tener la estructura “Plan [Nombre Cliente] [Información Adicional].xlsx. Por ejemplo “Plan BMW - AFS - Enero 2022_v3.xlsx”. (Es crítico que entre Plan, el nombre del cliente y la información adicional exista la separación por espacios).
- El archivo debe tener una hoja con la información de plan de medios llamada __PLAN + TAXONOMIA__(Es importante que este escrito de esta misma forma).
- La tabla de información debe comenzar después de dos espacios.