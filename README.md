## Tabla de contenidos
1. [General Info](#general-info)
2. [Tecnologias](#Tecnologias)
3. [Recursos CML](#Recursos_CML)
4. [Colaboradores](#Colaboradores)
5. [Instalacion](#Instalacion)
6. [Ejecucion](#Ejecucion)
7. [Explicacion](#Explicacion)
8. [Sugerencias y pasos siguientes](#Sugerencias)
9. [Estructura de carpetas](#plantilla_proyectos_python)


## General Info
***

El proyecto tiene tiene los siguientes objetivos:

1.-Indice del nivel de oscuridad:
La personas naturales tienen un indice preestablecido de oscuridad que va de 0 a 1, que indica que tanta informacion tributaria hay disponible de esa entidad. El objetivo es extender esta medicion de materia oscura a sociedades.  Un subproducto de este proceso es la visualizacion de relaciones en mallas societarias. 

2.-Indice de grado de familiaridad de las sociedades:  permita establecer que grado de familiariedad tienen las personas naturales ligadas a una sociedad juridica. 
Como subproducto del proceso de calculo de familiaridad se deriva el calculo de las sociedades relacionadas con cada entidad natural.


## Tecnologias
***

Se utilizó el lenguaje de programación Python.
En este proyecto se utilizó los siguientes packages de Python:
- pandas (manipulacion de dataframes)
- pyspark (motor de analisis para procesamiento de datos e gran escala)
- seaborn (creación de gráficos estadísticos)
- numpy  (cálculo numérico y el análisis de datos)
- matplotib (generacion de gráficos bidimensionales)
- networkx (estudios de gráficos y análisis de redes)
- pyvis (generador de grafos en pyhton con el mínimo de código)

Cabe destacar que en el pipeline de ejecución del proyecto, no se considera la instalacion de estos packages (en caso de ser necesario). Sin embargo, en caso de ser necesario, existe un notebook de Jupyter que realiza dicha tarea. 

Ahora bien, para cada objetivo hay una forma diferente de diseño del algoritmo.

1.-Indice de oscuridad: a partir de las sociedades y sus composiciones de socios, los cuales tienen cierta participacion en la entidad. A su vez, estos socios pueden ser personas naturales con vierto nivel de oscuridad. El output es una tabla de entidades completas con su nivel de oscuridad, numero de iteraciones y entidades incompletas con desglose de participacion de socios. A partir de estas relaciones de socios veremos como se propaga esta oscuridad a traves de sus malla societaria.

Se utilizaron metodos de estos recursos para poder procesar la data de forma iterativa usando consultas SQL para acelerar el proceso. Inicialmente se utilizó el package Pandas de Python pero fue reemplazado por una estructura en SQL por la poca eficiencia del proceso. La idea  del algoritmo es es para cada entidad verificar si todos sus socios tienen oscuridad y luego calcular el indice de oscuridad para esa entidad utilizando la participacion de socios. Esta informacion se utiliza iterativamente para completar otras sociedades.

2.-Indice de familiaridad: a partir de las sociedades y sus composiciones de socios,se puede rastrear las personas naturales a las cuales esta asociada una entidad juridica. Luego de ello, es posible mendiante una tabla de relaciones familiares obtenida del registro civil, hacer una comparativa de todas las relaciones entre socios en una sociedad. Con ello se puede comparar con la data de relaciones familiares y de ese modo obtener el numero exacto de personas asociadas a una entidad juridica que por lo menos tienen un grado de relacion familiar con otro socio. Esto se establece como un indice entre 0 y 1, ambos inclusive, donde 0 indicaria que ninguna persona natura tiene una relacion familiar con otra persona natural relacionada con esa entidad y 1 cuando todas las personas naturales relacionadas tienen por lo menos un familiar en ese pool de personas.


## Recursos_CML
***
Para este proyecto, se crearon notebook de jupiyter que permiten realizar diferentes tareas:

*depuracion_sociedades.ipynb*: archivo que permite hacer una depuracion de la data de sociedades previo a las ejecuciones de busqueda de materia oscura y familiariedad. Esto permite eliminar informacion duplicada o anómala en la malla societaria, donde el total de porcentajes de participacion es mayor al 100% o menos al 90%.

*materia_oscura.ipynb*: archivo que permite completar materia oscura en sociedades mediante un algoritmo de iteracion, basandose en la composicion societaria y tomando como punto de partida el nivel de oscuridad para personas naturales, se propaga por la red societaria este nivel de oscuridad para asignarlo a personas jurídicas. Mas detalles del funcionamiento están en el notebook correspondiente. De ello se puede obtener una tabla con rut de sociedades y su respectivo nivel de oscuridad e iteraciones de cálculo, asi como la composicion de cada sociedad con el nivel de oscuridad de sus socios.

*familiares_sociedad_v2.ipynb*: archivo que permite obtener el grado de familiaridad de sociedades como tambien las sociedades asociadas a ciertos rut de personas naturales, tambien basado en un algoritmo de iteracion. El output que se obtiene del mismo es una tabla con el rut de la sociedad con su indice de familiaridad asi como otra tabla con el número de sociedades a las cuales está ligado una persona natural. Mas detalles del funcionamiento están en el notebook correspondiente.

*visualizacion_relaciones_societarias.ipynb*: archivo que permite obtener una representación gráfica de las relaciones societarias que involucran a cierta entidad, permitiendo ver de quien es socio cierta entidad y quíenes son los socios de la misma. Este archivo no se utiliza directamente en el pipeline pero si ayuda a obtener informacion adicional de las relaciones de los contribuyentes.

*to_py.ipynb*: archivo que permite transformar el codigo de los Jupyter notebook a archivos ejecutables de python que se requieren para construir el pipeline de ejecución.

*install_packages.ipynb*: archivo que permite instalar los packages utilizados en el proyecto en caso de que sea necesario.

Algunos de estos notebooks fueron convertidos a archivos .py con el fin de implementar los procesos de ejecucion en el pipeline. 



## Colaboradores
***
En este proyecto participa Henry Vega, Data Analyst de APIUX asi como el equipo del Área de Riesgo e Informalidad del SII, compuesto por Daniel Cantillana, Jorge Bravo y Yasser Nanjari, además Arnol Garcia, jefe de área y Manuel Barrientos, con soporte técnico en el espacio de trabajo y Jorge Estéfane, jefe de proyecto por parte de APIUX.

## Instalacion
***
Los pasos para instalar el proyecto y sus configuraciones en un entorno nuevo en caso que sea necesario, por ejemplo:
1. Instalar paquetes
    - Se ejecuta el notebook *install_packages.ipynb* el cual contiene los packages utilizados en los scripst del proyecto, a excepción de Spark

***

## Ejecución
Orden y explicación de los archivos que deben ser ejecutados para el correcto desempeño del proyecto:

1. En primer lugar, mencionar que no se debe ubicar ninguna database en las carpetas del proyecto y se trabaja a traves de consultas en Spark al warehouse del SII. 

2. Ejecucion del pipeline a partir del job `Depuracion _sociedades`. Este script gestiona la base de datos de relaciones societarias y realiza su depuración. Automáticamente luego de ello se ejecutan los procesos de `Indice_familiaridad` y `Materia_oscura`. Junto con ello, luego se ejecuta automaticamente el proceso  `Reporte_mteria_oscura`, que cra un reporte en formato html con los principales resultados de materia oscura en sociedades.
3. Ejecución automática de de visualización de relaciones societarias a partir del notebook `visualizacion_relaciones_societarias.ipynb` .
...

## Outputs
***
El proyecto tiene una serie de outputs que se obtienen a partir de la ejecución del pipeline original.
1. `artefactos/Oscuridad/Sociedades_oscuridad_completa.csv` es el archivo que contiene rut de sociedades con su respectivo nivel de oscuridad e iteraciones. Corresponde a sociedades que pudieron ser completadas en oscuridad en las iteraciones correspondientes

2. `artefactos/Oscuridad/Sociedades_oscuridad_completa.csv` para todas las sociedades disponibles en la base de datos, muestra sus socios y su nivel respectivo de oscuridad.

3. `artefactos/Familiaridad/familiaridad.csv` archivo con rut de sociedades y un índice de familiariedad entre 0 y 1 para cada sociedad.

4. `artefactos/Familiaridad/sociedades_por_socio.csv` archivo con rut de socios y el numero de entidades relacionadas.

## Sugerencias
***

La principal sugerencia futuras para mejorar el proyecto es complejizar las fuerzas de las relaciones entre socios y sociedades para incluir otros aspectos de transmision de oscuridad, tal como se realiza en los modelos de riesgo. Esto puede ser aspectos comercialesy tributarios. Por otro lado se puede anexar el calculo de otro tipo de ratios o valors que apoye las decisiones  respecto a las entidades con mayor oscuridad. 


## plantilla_proyectos_python
***
La estrutura de proyectos debería ser la siguiente:
(además en cada carpeta dejé un archivo txt q explica el contenido que debe llevar la carpeta)

### Recuerden que se aceptan sugerencias de todo tipo...respecto a la estructura de carpetas
.
├── data  
│   ├── external                          __Data de fuentes de 3eras partes__  
│   ├── interim                           __Datos intermedios que han sido transformados__  
│   ├── processed                         __Datos procesados, que serán usados para el modelo__  
│   └── raw                               __Data cruda, datos descargados sin modificar__  
│     
├── docs                                  __Cualquier cosa__  
├── models/artefactos                     __Modelos entrenados o serializados, predicciones__  
├── README.md                             __Debe tener la info relevante del proyecto__      
├── references                            __Diccionario de datos, manuales, y cualquier otro material explicativo__  
├── reports                               __análisis generados como pdf, html, latex, etc__  
│   └── figures                           __Gráficas generadas a hacer usados en los reportes__  
├── requirements.txt                      __lista de paquetes requeridos para reproducir el entorno, pip freeze > requirements.txt__  
├── src                                   __CODIGO FUENTE DEL PROYECTO__  
├── notebooks                             __Notebooks de jupyter,deben llamar a los codigos en src o ser transformados a formato py__    
│   ├── data                              __Scripts para descargar o generar datos__    
│   │   ├── __init__.py   
│   │   └── make_dataset.py    
│   ├── features                          __Scripts para convertir la data cruda a datos para el modelo__   
│   │   ├── build_features.py   
│   │   └── __init__.py   
│   ├── __init__.py  
│   ├── models                            __Scripts para entrenar el modelo y luego hacer predicciones__   
│   │   ├── __init__.py   
│   │   ├── predict_model.py   
│   │   ├── train_model.py   
│   │   └── validate_model.py  
│   │── visualization                     __Scripts para crear exploración y resultados orientados a la visualización__   
│   │   ├── __init__.py   
│   │   └── visualize.py  
│   └── inferencia                        __Scripts para inferencia, donde se usan los archivos contenidos en el resto de las carpetas__    
│       ├── mainInferencia.py            
│       
├── test_environment.py                 __aplica en caso de querer hacer test unitarios__
