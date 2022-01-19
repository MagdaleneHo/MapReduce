#!/usr/bin/env python
# coding: utf-8

# # MapReduce Assignment

# In[1]:


get_ipython().run_line_magic('cd', 'C:\\\\Users\\\\bluec\\\\desktop\\\\BDA assignment')


# ## Objective 1

# In[ ]:


# To find the relationship between high budget movies and the average ratings.
# Took 2.48 secs


# In[63]:


import pandas as pd
import numpy as np

ratings_df = pd.read_csv('ratings.csv', delimiter=',', names = ['movie_id', 'rating', 'title', 'budget'])
averages = ratings_df.groupby(['movie_id','budget']).agg({'rating':'mean'})
averages.columns = ['avgRating']

print(averages.sort_values(by=['budget'], ascending=False).head(10))
print(ratings_df.shape)


# ## Objective 2

# In[ ]:


# To identify the behaviour of the users in this dataset whether they are generous in their ratings comparing 2017 and 2019.
# Took 5.19 seconds


# In[37]:


import pandas as pd
from collections import Counter
obj1_df = pd.read_csv('obj1.csv', delimiter=',', names = ['movie_id', 'rating'])

z = obj1_df['rating']
Counter(z)


# In[38]:


import pandas as pd
from collections import Counter
obj2_df = pd.read_csv('obj2.csv', delimiter=',', names = ['movie_id', 'rating'])

z = obj2_df['rating']
Counter(z)


# ## Objective 3 

# In[ ]:


# To identify the top movies rated and average ratings for each movie. 


# In[2]:


import pandas as pd
import numpy as np

ratings_df = pd.read_csv('ratings.csv', delimiter=',', names = ['movie_id', 'rating', 'title', 'budget'])
averages = ratings_df.groupby(['title']).agg({'rating':'mean'})
averages.columns = ['avgRating']

print(averages.sort_values(by=['avgRating'], ascending=False).head(10))

#Takes 4.38 seconds


# In[3]:


import pandas as pd
import numpy as np

ratings_df = pd.read_csv('ratings.csv', delimiter=',', names = ['movie_id', 'rating', 'title', 'budget'])
averages = ratings_df.groupby(['title']).agg({'rating':'mean'})
averages.columns = ['avgRating']

print(averages)

#Takes 2.75 seconds


# ## Objective 4 

# In[ ]:


# To find the top 10 popular movies in this dataset by the number of ratings. 
# Takes 2.82 seconds


# In[4]:


import pandas as pd
from collections import Counter
ratings_df = pd.read_csv('ratings.csv', delimiter=',', names = ['movie_id', 'rating', 'title', 'budget'])

z = ratings_df['title']
y = Counter(z)
print(y.most_common(10))


# In[ ]:




