#!/usr/bin/env python
# coding: utf-8

# ## Apiux & SII: calculo de indice de familiariedad en  personas juridicas.
# ## ATENCION: proyecto sujeto a mantenimiento continuo. 
# 
# ## Henry Vega (henry.vega@edu.uai.cl)
# ## Physicist and Data Scientist

# In[1]:


from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark import SparkContext, SparkConf
import pyspark
import pandas as pd
import warnings
warnings.filterwarnings('ignore', category=DeprecationWarning)


# In[2]:


spark = SparkSession.builder \
    .appName("Test") \
    .config("spark.yarn.access.hadoopFileSystems","abfs://data@datalakesii.dfs.core.windows.net/") \
    .getOrCreate()
warnings.filterwarnings('ignore', category=DeprecationWarning)
sc=spark.sparkContext
sc.setLogLevel ('ERROR')


# ## Carga de relaciones societarias(depurada)

# In[3]:


df = spark.read.options(header=True,inferSchema=True,delimiter=",").csv("data/processed/sociedades_participacion_capital_nozero.csv")
df.createOrReplaceTempView("sociedad")
#spark.sql("select * from sociedad order by RUT_SOCIEDAD asc").show()


# Veamos un ejemplo de la composicion de socios para una sociedad sospechosa en composicion

# In[4]:


#spark.sql("select count(*) from sociedad where RUT_SOCIEDAD LIKE 'Qbau/6SlJ/lEcKUD%'").show()
#spark.sql("select *  from libsdf.jab_soc_2023_inom where RUT_SOCIEDAD LIKE 'Qbau/6SlJ/lEcKUD%'").show()


# ## Tablas temporales previo a la ejecucion

# In[5]:


spark.sql("select RUT_SOCIEDAD, RUT_SOCIO from sociedad order by RUT_SOCIEDAD asc").createOrReplaceTempView("sociedad")
spark.sql("select RUT_SOCIEDAD as RUT_SOCIEDAD_AUX ,RUT_SOCIO as RUT_SOCIO_AUX from sociedad order by RUT_SOCIEDAD asc").createOrReplaceTempView("aux")
#spark.sql("select RUT_SOCIEDAD, COUNT(*) AS F from sociedad group by RUT_SOCIEDAD ORDER BY F DESC").show()



# ## Calculo de arbol de socios naturales

# In[6]:


for a in range (1,10):
    spark.sql("select * from sociedad left join aux on sociedad.RUT_SOCIO=aux.RUT_SOCIEDAD_AUX ").createOrReplaceTempView("sociedad")
    spark.sql("select * from sociedad order by RUT_SOCIEDAD_AUX desc").createOrReplaceTempView("sociedad")
#    spark.sql("select * from sociedad").show()
    spark.sql("select RUT_SOCIEDAD, CASE WHEN RUT_SOCIEDAD_AUX is null then RUT_SOCIO else RUT_SOCIO_AUX END AS RUT_SOCIO from sociedad  order by RUT_SOCIEDAD_AUX desc").createOrReplaceTempView("sociedad")
#    spark.sql("select * from sociedad").show()
#    spark.sql("select RUT_SOCIEDAD, COUNT(*) as d from sociedad GROUP BY RUT_SOCIEDAD order by d desc ").show()


# In[7]:


oscuridad=spark.sql("select * from libsdf.jab_materia_inom")
oscuridad.createOrReplaceTempView("oscuridad")
spark.sql("select RUT_SOCIEDAD, CONT_RUT from sociedad left join oscuridad on sociedad.RUT_SOCIO=oscuridad.CONT_RUT ").createOrReplaceTempView("socios_final")


# ## Sociedades por persona natural

# Tambien obtendremos un output que nos permita establecer cuanta sociedades esta relacionado con cada uno de las personas naturales relacionadas.

# In[8]:


sociedades_por_socio=spark.sql("select CONT_RUT, count(RUT_SOCIEDAD) as SOCIEDADES_RELACIONADAS from socios_final where CONT_RUT is not null group by CONT_RUT order by SOCIEDADES_RELACIONADAS DESC").toPandas()
sociedades_por_socio.to_csv('artefactos/Familiaridad/sociedades_por_socio.csv', index=False)


# In[9]:


spark.sql("select RUT_SOCIEDAD, COUNT(*) AS NONULOS from socios_final where CONT_RUT is not null group by RUT_SOCIEDAD ").createOrReplaceTempView("nonulos")
spark.sql("select RUT_SOCIEDAD, COUNT(*) AS TOTAL from socios_final group by RUT_SOCIEDAD ").createOrReplaceTempView("total")

spark.sql("select  total.RUT_SOCIEDAD as RUT_SOCIEDAD, TOTAL, NONULOS from total left join nonulos on total.RUT_SOCIEDAD=nonulos.RUT_SOCIEDAD").createOrReplaceTempView("final")


# Trabajaremos con las sociedades que en su arbol relacional contienen todas sus personas naturales completas y con un numero total de personas naturales conectadas menor que 100.

# In[10]:


spark.sql("select socios_final.RUT_SOCIEDAD, CONT_RUT as RUT_SOCIO from socios_final left join final on socios_final.RUT_SOCIEDAD=final.RUT_SOCIEDAD where TOTAL=NONULOS AND TOTAL<100 ORDER BY TOTAL desc").createOrReplaceTempView("final")


# ## Combinatoria de pool de socios

# A continuacion, se hace un auto join con el fin de obtener todas las combinaciones posibles entre socios diferentes para comparar con las relaciones familiares. Luego de ello se concatenan los socios para establecer un codigo especifico para cada combinacion de ello.

# In[11]:


spark.sql("select t1.RUT_SOCIEDAD, t1.RUT_SOCIO AS RUT_SOCIO_1, t2.RUT_SOCIO AS RUT_SOCIO_2 FROM final AS t1 JOIN final AS t2 ON t1.RUT_SOCIEDAD = t2.RUT_SOCIEDAD  WHERE t1.RUT_SOCIO<>t2.RUT_SOCIO").createOrReplaceTempView("final")
spark.sql("select RUT_SOCIEDAD, RUT_SOCIO_1,RUT_SOCIO_2,RUT_SOCIO_1||RUT_SOCIO_2 as key from final").createOrReplaceTempView("final")


# ## Exploracion, limpieza y ampliacion de  data de relaciones familiares

# A continuacion se obtiene el archivo de relaciones familiares que permitira obtener las relaciones entre socios y comparar con los datos de composicion de sociedades. En este archivo hay diferentes relaciones familiares. Veamos cuales hay:

# In[12]:


spark.read.parquet("abfs://data@datalakesii.dfs.core.windows.net/DatosOrigen/LibSDF/REL_FAMILIARES_AARI_EH").createOrReplaceTempView("familiar")
spark.sql("SELECT TIPO_RELACION, count(*) FROM familiar group by TIPO_RELACION").show()
spark.sql("SELECT count(*) from familiar").show()


# In[13]:


spark.sql("SELECT CONT_RUT, count(*) as c FROM familiar group by CONT_RUT order by c desc").show()


# A continuacion se toman en cuenta solo una vez los datos repetidos y no consideramos las relaciones donde CONT_RUT sea igual a RUT_FAM.

# In[14]:


spark.sql("select CONT_RUT,RUT_FAM,COUNT(*) as c from familiar where CONT_RUT!=RUT_FAM group by CONT_RUT,RUT_FAM order by c desc ").createOrReplaceTempView("familiar")
spark.sql("select CONT_RUT,RUT_FAM from familiar").createOrReplaceTempView("familiar")


# Duplicamos la data e invertimos las relaciones porque la familiariedad es bidireccional.

# In[15]:


#spark.sql("select CONT_RUT as RUT_FAM,RUT_FAM as CONT_RUT from familiar").createOrReplaceTempView("familiar2")
#spark.sql("SELECT * FROM familiar UNION ALL SELECT * FROM familiar2").createOrReplaceTempView("familiar")


# In[16]:


spark.sql("select CONT_RUT, RUT_FAM,count(*) AS C from familiar GROUP BY CONT_RUT, RUT_FAM ORDER BY C DESC").createOrReplaceTempView("familiar")
spark.sql("select  CONT_RUT || RUT_FAM  as key from familiar").createOrReplaceTempView("familiar")


# ## Cruce de relaciones familiares calculadas con pool de socios por sociedad

# In[17]:


spark.sql("select RUT_SOCIEDAD,RUT_SOCIO_1, RUT_SOCIO_2, familiar.key as FAMILIAR_KEY from final left join familiar on final.key= familiar.key order by RUT_SOCIEDAD").createOrReplaceTempView("relaciones")


# Lo que haremos a continuacion es obtener los valores unicos de socios que si tienen relaciones, que son los distintos valores de RUT_SOCIO que tienen FAMILIAR_KEY no nulo.

# In[18]:


spark.sql("select RUT_SOCIEDAD,COUNT(DISTINCT(RUT_SOCIO_1)) AS FAMILIARES from relaciones WHERE FAMILIAR_KEY IS NOT NULL GROUP BY RUT_SOCIEDAD").createOrReplaceTempView("socios_familia")
spark.sql("select socios_familia.RUT_SOCIEDAD, FAMILIARES, TOTAL  from socios_familia left join total on socios_familia.RUT_SOCIEDAD=total.RUT_SOCIEDAD").createOrReplaceTempView("socios_familia")


# In[19]:


## Calculo final de metrica asociadaa familiaridad. Explicacion y 


# In[20]:


spark.sql("select  RUT_SOCIEDAD, FAMILIARES, TOTAL, FAMILIARES/TOTAL*100 as TASA_FAMILIARIDAD from socios_familia").toPandas().to_csv('artefactos/Familiaridad/familiaridad.csv', index=False)


# In[21]:


#a=spark.sql("select * from socios_familia").toPandas()


# In[22]:


#a.to_csv('testeo_familiar.csv', index=False)


# In[23]:


#a.write.csv("familiar_output.csv")


# In[ ]:




