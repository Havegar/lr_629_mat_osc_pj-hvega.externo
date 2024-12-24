#!/usr/bin/env python
# coding: utf-8

# ## Analisis de sociedades con oscuridad completa

# In[3]:


import pandas as pd
import seaborn as sns
import numpy as np

import matplotlib.pyplot as plt
sns.set(rc={'figure.figsize':(20,10)})


# Usaremos el output de la segunda version de  ejecucion:
# 
# *artefactos/Ejecucionv2/Sociedades_oscuridad_completa.csv*: archivo con rut de la sociedad completada, su valor de ocuridad y el numero de iteraciones para lograr la completitud.
# 
# *artefactos/Ejecucionv2/Sociedades_oscuridad_actualizada.csv*: archivo con los datos de composicion y oscuridad de todas las sociedades con oscuridad por socio, incluidos los datos faltantes.

# In[5]:


complete = pd.read_csv('/home/cdsw/artefactos/Oscuridad/Sociedades_oscuridad_completa.csv', delimiter=',')
society_all= pd.read_csv('/home/cdsw/artefactos/Oscuridad/Sociedades_oscuridad_actualizada.csv', delimiter=',')


# In[6]:


len(society_all['RUT_SOCIEDAD'].unique()), len(complete) 


# In[7]:


len(complete) /(len(society_all['RUT_SOCIEDAD'].unique()))*100


# Es decir se completa el 96.976% de las sociedades. Ahora veamos la composicion de estos registros completos.

# In[8]:


complete.describe()


# Vemos que los cuartiles son muy similares, veamos la distribucion por valor de oscuridad.

# In[10]:


x = complete['Value']
sns.histplot(x = x, bins=10).set_title('Distribucion de materia oscura')
sns.set(font_scale=1)


# Claramente los valores de oscuridad encontrados estan entre  0.2 y 0.4, lo cuale es coherennte con la mediana 2.505881e-01, y la media 2.459072e-01. El histograma   de distribucion acumulado:

# In[11]:


x = complete['Value']
sns.histplot(x = x, bins=10, cumulative=True).set_title('Distribucion acumulada de materia oscura')
sns.set(font_scale=1)


#  Comparado al total de datos, las personas juridicas con mas oscuridad que 0.4, parece ser marginal. Ahora bien, en cuanto a las iteraciones y la completitud, vemos que:

# In[12]:


x = complete['iterations']
sns.histplot(x = x,  bins=10)
sns.set(font_scale=1)


# Es decir, la mayoria de las sociedades son completadas en la primera iteracion. Grafiquemos iteracions vs oscuridad.

# In[13]:


sns.histplot(
    complete, x="Value", y="iterations",
    bins=30, discrete=(False, True),
    cbar=True, cbar_kws=dict(shrink=.75),
)


# Vemos que la mayoria de los registros es completado en 1 iteracion y tienen oscuridad cercana a 0.2
# Ahora analicemos los registros con valores mayores o iguales a una oscuridad de 0.75.

# In[14]:


sup=complete[complete['Value']>=0.75]
sup.describe()


# Observemos que son solo un poco mas de 2000 sociedades con estas caracteristicas. Recordemos que las iteraciones corresponden a niveles de relacion, a mayor profundida de la relacion es necesario mas iteraciones para completar las sociedades. Veamos la distribucion de materia oscura en esta seleccion.

# In[15]:


x = sup['Value']
sns.histplot(x = x,  bins=10, cumulative=False).set_title('Distribucion de materia oscura con indice mayor a 0.75')
sns.set(font_scale=1)


# In[16]:


sup.describe()


# En este caso, tenemos que el maximo de profundidad es 2 y casi todos los valores de oscuridad rondan a 0.79
# 
# Finalmente, tambien tenemos personas juridicas con valor de oscuridad igual a 1. 

# In[17]:


max=complete[complete['Value']==1]
max.describe()


# Tenemos 46 personas juridicas con oscuridad igual a 1, completadas todas en la primera iteracion, es decir todos sus socios son personas naturales.

# In[18]:


get_ipython().system('pip install nbconvert')


# In[19]:


get_ipython().system('jupyter nbconvert --to html reporte_oscuridad.ipynb --stdout > "reports/Oscuridad/reporte_oscuridad.html"')


# In[ ]:





# In[ ]:




