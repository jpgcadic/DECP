#!/usr/bin/env python
# coding: utf-8

# In[1]:


getCodeInVersion = "MATCH(c:CPV), (v:CPV) WHERE c.code = '{}' AND v.versionCPV = '{}' AND ((c:CPV)-[:IS_IN_CATEGORY*1..5]-(v:CPV)) RETURN c"
# request = getCodeInVersion.format(code, version)
# result = db.cypher_query(request, resolve_objects=True)

