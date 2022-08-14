#!/usr/bin/env python
# coding: utf-8

# In[6]:


import json
def chunk(lst, n):
    for i in range(0, len(lst), n):
        yield lst[i:i + n]

with open('/Users/davidacharya/Desktop/Antra/Assignment/movie.json', 'r') as f_in:
    data = json.load(f_in)


for i, fact in enumerate(chunk(data['movie'], 1250), 1):  
    with open('Movie_{}.json'.format(i), 'w') as f_out:
        d = {}
        d['movie'] = fact
        json.dump(d, f_out, indent=4)

