
# coding: utf-8

# In[1]:


from SPARQLWrapper import SPARQLWrapper, JSON
import collections
import random
import time
from zhconv import convert
from gensim.models import word2vec
import numpy as np
import jieba
from zhconv import convert
from sklearn.preprocessing import OneHotEncoder
import pickle
import heapq


# In[2]:


def _call_dbpedia_endpoint(query_string):
    sparql = SPARQLWrapper("http://dbpedia.org/sparql")
    sparql.setReturnFormat("json")
    sparql.setQuery(query_string)
    results = sparql.query().convert()
    time.sleep(10) 
    return results

def parse_result(results, type_s):
    return_info = []
    if type_s == 'movie':
        # 获取到作品名称、主演、导演、摘要、imdb主页地址等信息
        name = ""
        imdb = ""
        directors = ""
        actors = []
        abstract = ""
        for i in range(len(results['results']['bindings'])):
            name = results['results']['bindings'][i]['name']['value']
            imdb = results['results']['bindings'][i]['imdb']['value']
            directors = list(set([director.strip().split('/')[-1] for director in results['results']['bindings'][i]['directors']['value'].strip().split(' ')]))            
            actors = list(set([actor.strip().split('/')[-1] for actor in results['results']['bindings'][i]['actors']['value'].strip().split(' ')]))
            abstract = results['results']['bindings'][i]['abs']['value']
        return_info.append([name, imdb, directors, actors, abstract])
    
    elif type_s == 'book':
        # 获取到作品名称、主页地址，作者，类型，摘要等信息
        name = ""
        book = ""
        author = ""
        genre = ""
        abstract = ""
        for i in range(len(results['results']['bindings'])):
            name = results['results']['bindings'][i]['name']['value']
            book = results['results']['bindings'][i]['book']['value']
            author = list(set([author.strip().split('/')[-1] for author in results['results']['bindings'][i]['authors']['value'].strip().split(' ')]))        
            genre = results['results']['bindings'][i]['genre']['value'].strip().split('/')[-1]
            abstract = results['results']['bindings'][i]['abs']['value']
        return_info.append([name, book, author, genre, abstract])
        
    elif type_s == 'game':
        # 获取到作品名称, 主页地址，开发者，类型，摘要等信息
        name = ""
        game = ""
        developer = ""
        genre = ""
        abstract = ""
        for i in range(len(results['results']['bindings'])):
            name = results['results']['bindings'][i]['name']['value']
            game = results['results']['bindings'][i]['game']['value']
            developer = list(set([author.strip().split('/')[-1] for author in results['results']['bindings'][i]['developers']['value'].strip().split(' ')] ))        
            genre = results['results']['bindings'][i]['genre']['value'].strip().split('/')[-1]
            abstract = results['results']['bindings'][i]['abs']['value']
        return_info.append([name, game, developer, genre, abstract])
    else:
        print('请输入正确的查询类别')
        
    return return_info

def get_search_result(name, nums= 1, type_s='movie'):
    """ Functions:
            基于传入的名字搜索相关的电影信息及资源
        Params:
            name(str): 作品名字
            nums(int): 返回结果数量
            type_s(str): movie电影 book书籍 game游戏
        Returns:
            Json(list(list)): 以json的形式返回结果
    """
    # 1.通过dbpedia和imdb查询所需要的作品信息
    query_string = ""
    if type_s == 'movie':
        print(name)
        query_string = """
                    SELECT distinct ?name ?imdb ?film group_concat(?director, ' ') AS ?directors group_concat(?actor, ' ') AS ?actors ?abs
                    WHERE
                    {{ 
                        ?film rdf:type dbo:Film .
                        ?film dbo:wikiPageExternalLink ?imdb.
                        ?film rdfs:label ?name .
                        ?film dbo:director ?director .
                        ?film dbo:starring ?actor .
                        ?film dbo:abstract ?abs.

                        FILTER regex(?imdb, "http://www.imdb.com/title/.*", "i").
                        FILTER ((lang(?name)="zh")&&(lang(?abs)="zh")&&(regex(?name, '%s'))). \
                    }}LIMIT %d
                  """ % (name, nums)
    elif type_s == 'book':
        query_string = """
                        SELECT ?name ?book group_concat(?author, ' ') AS ?authors  ?genre ?abs
                        WHERE 
                        {{
                            ?book rdf:type dbo:Book .
                            ?book rdfs:label ?name .
                            ?book dbo:author ?author .
                            ?book dbo:literaryGenre ?genre .
                            ?book dbo:abstract ?abs.
                            FILTER ((lang(?name)="zh")&&(lang(?abs)="zh")&&(regex(?name, '%s'))) .
                        }}LIMIT %d
                        """ % (name, nums)
    elif type_s == 'game':
        query_string = """
                        SELECT ?name ?game group_concat(?developer, ' ') AS ?developers ?genre ?abs
                        WHERE 
                        {{
                            ?game rdf:type dbo:VideoGame .
                            ?game rdfs:label ?name .
                            ?game dbo:developer ?developer .
                            ?game dbo:genre ?genre .
                            ?game dbo:abstract ?abs
                            FILTER (  (lang(?name)="zh")&&(lang(?abs)="zh")&&(regex(?name, '%s'))).
                        }}LIMIT %d
                        """ % (name, nums)
    else:
        print('请输入正确的查询类别')

    # 2.调用DBpedia的endpoint
    results = _call_dbpedia_endpoint(query_string)
    
    
    # 3.解析sparql返回的json结果使之获取我们需要的内容
    return parse_result(results, type_s)
    


# In[46]:


def _cold_start(type_s='movie'):
    """
        作品冷启动
    """
    pass
    if type_s == 'movie':      
        query_string = """
                        SELECT distinct ?name ?imdb ?film group_concat(?director, ' ') AS ?directors group_concat(?actor, ' ') AS ?actors ?abs
                        WHERE
                        {{ 
                            ?film rdf:type dbo:Film .
                            ?film dbo:wikiPageExternalLink ?imdb.
                            ?film rdfs:label ?name .
                            ?film dbo:director ?director .
                            ?film dbo:starring ?actor .
                            ?film dbo:abstract ?abs.

                            FILTER regex(?imdb, "http://www.imdb.com/title/.*", "i").
                            FILTER ((lang(?name)="zh")&&(lang(?abs)="zh")).
                            FILTER regex(?abs, ".*热门电影.*", "i").
                        }}LIMIT 5

                        """
    elif type_s == 'book':
        query_string = """
                        SELECT ?name ?book ?author ?genre ?abs
                        WHERE 
                        {{
                            ?book rdf:type dbo:Book .
                            ?book rdfs:label ?name .
                            ?book dbo:author ?author .
                            ?book dbo:literaryGenre ?genre .
                            ?book dbo:abstract ?abs.
                            FILTER ((lang(?name)="zh")&&(lang(?abs)="zh")) .
                            FILTER regex(?abs, ".*热门书籍.*", "i").
                        }}LIMIT 5
                        """
    elif type_s == 'game':
        query_string = """
                        SELECT ?name ?game ?developer ?genre ?abs
                        WHERE 
                        {{
                            ?game rdf:type dbo:VideoGame .
                            ?game rdfs:label ?name .
                            ?game dbo:developer ?developer .
                            ?game dbo:genre ?genre .
                            ?game dbo:abstract ?abs
                            FILTER (  (lang(?name)="zh")&&(lang(?abs)="zh")).
                        }}LIMIT 5
                        """
    else:
        print('请输入正确的查询类别')
    results = _call_dbpedia_endpoint(query_string)
    return parse_result(results, type_s)
    
    

def _weighted_mean(weights):
    """
        根据作品的人名出现数量设置推荐电影的权重
    """
    total_weight = sum(weights)
    res = []
    for weight in weights[:-1]:
        res.append(5 * weight / total_weight)
    res.append(5 - sum(res))
    return res

        
##余弦相似度计算
def cos_sim(vector_a, vector_b):
    vector_a = np.mat(vector_a)
    vector_b = np.mat(vector_b)
    num = float(vector_a * vector_b.T)
    denom = np.linalg.norm(vector_a) * np.linalg.norm(vector_b)
    cos = num / denom
    sim = 0.5 + 0.5 * cos
    return sim


##名字余弦相似度相似度排名
def name_simi(name,flag):
    model = word2vec.Word2Vec.load('D:/jupyter/competition/word2vec/word2vec_wx')
    if flag=="movie":
        temp = open('pickle/name.pk','rb')
        res = pickle.load(temp)
        result = []
        candi = get_search_result(name, nums= 1, type_s='movie')
        candi_name = candi[0][0]
        temp = np.zeros([256])
        name = convert(candi_name, 'zh-cn')
        seg_list = jieba.cut(name, cut_all=False)
        num = 0
        for word in seg_list:
            try:
                temp += model[word]
                num+=1
            except KeyError:
                continue
        candi_name_matrix = temp/num
        for i in range(len(res)):
            result.append(cos_sim(candi_name_matrix,res[i]))
        return result
    elif flag=="book":
        temp = open('pickle/name_book.pk','rb')
        res = pickle.load(temp)
        result = []
        candi = get_search_result(name, nums= 1, type_s='book')
        candi_name = candi[0][0]
        temp = np.zeros([256])
        name = convert(candi_name, 'zh-cn')
        seg_list = jieba.cut(name, cut_all=False)
        num = 0
        for word in seg_list:
            try:
                temp += model[word]
                num+=1
            except KeyError:
                continue
        candi_name_matrix = temp/num
        for i in range(len(res)):
            result.append(cos_sim(candi_name_matrix,res[i]))
        return result
    elif flag=="game":
        temp = open('pickle/name_game.pk','rb')
        res = pickle.load(temp)
        result = []
        candi = get_search_result(name, nums= 1, type_s='game')
        candi_name = candi[0][0]
        temp = np.zeros([256])
        name = convert(candi_name, 'zh-cn')
        seg_list = jieba.cut(name, cut_all=False)
        num = 0
        for word in seg_list:
            try:
                temp += model[word]
                num+=1
            except KeyError:
                continue
        candi_name_matrix = temp/num
        for i in range(len(res)):
            result.append(cos_sim(candi_name_matrix,res[i]))
        return result

def person_simi(name,flag):
    if flag == "movie":
        candi = get_search_result(name, nums= 1, type_s='movie')
        candi_person = []
        candi_person.extend(candi[0][2])
        person = []
        person.extend(candi_person)
        search_movie_person = []
        person_list = []
        query_string = """
        SELECT distinct ?name ?imdb ?film group_concat(?director, ' ') AS ?directors group_concat(?actor, ' ') AS ?actors ?abs
        {{ 
            ?film rdf:type dbo:Film .
            ?film dbo:wikiPageExternalLink ?imdb.
            ?film rdfs:label ?name .
            ?film dbo:director ?director .
            ?film dbo:starring ?actor .
            ?film dbo:abstract ?abs.
    
            FILTER regex(?imdb, "http://www.imdb.com/title/.*", "i").
            FILTER ((lang(?name)="zh")&&(lang(?abs)="zh")).
        }}LIMIT 10000
    
        """
        results = _call_dbpedia_endpoint(query_string)
        for i in range(len(results['results']['bindings'])):
            name = results['results']['bindings'][i]['name']['value']
            director = list(set([director.strip().split('/')[-1] for director in results['results']['bindings'][i]['directors']['value'].strip().split(' ')]))
            actor = list(set([actor.strip().split('/')[-1] for actor in results['results']['bindings'][i]['actors']['value'].strip().split(' ')]))
            person.extend(actor)
            person.extend(director)
            person.extend(search_movie_person)
            temp = actor+director
            person_list.append(temp)
        person_list = np.delete(person_list,[10,35,50],axis = 0)
        enc = OneHotEncoder()
        person = np.asarray(person).reshape(-1,1)
        enc.fit(person)
    #     person_list = enc.fit(person_list)
        res = []
        for i in range(len(person_list)):
            num = 0
            for j in range(len(candi_person)):
                for k in range(len(person_list[i])):
                    word1 = np.asarray(person_list[i][k]).reshape(-1,1)
                    word2 = np.asarray(candi_person[j]).reshape(-1,1)
                    a = enc.transform(word1).toarray()
                    b = enc.transform(word2).toarray()
                    num+=np.sum((a*b))
            res.append(num)
        return res
    if flag == "book":
        candi = get_search_result(name, nums=1, type_s='book')
        candi_person = []
        candi_person.extend(candi[0][2])
        person = []
        person.extend(candi_person)
        search_movie_person = []
        person_list = []
        query_string = """
                        SELECT ?name ?book ?author ?genre ?abs
                        WHERE 
                        {{
                            ?book rdf:type dbo:Book .
                            ?book rdfs:label ?name .
                            ?book dbo:author ?author .
                            ?book dbo:literaryGenre ?genre .
                            ?book dbo:abstract ?abs.
                            FILTER ((lang(?name)="zh")&&(lang(?abs)="zh")) .
                        }}LIMIT 100
                        """
        results = _call_dbpedia_endpoint(query_string)
        for i in range(len(results['results']['bindings'])):
            name = results['results']['bindings'][i]['name']['value']
            author = list(set([author.strip().split('/')[-1] for author in
                               results['results']['bindings'][i]['author']['value'].strip().split(' ')]))
            person.extend(author)
            person.extend(search_movie_person)
            person_list.append(author)
        person_list = np.delete(person_list, [60,77], axis=0)
        enc = OneHotEncoder()
        person = np.asarray(person).reshape(-1, 1)
        enc.fit(person)
        #     person_list = enc.fit(person_list)
        res = []
        for i in range(len(person_list)):
            num = 0
            for j in range(len(candi_person)):
                for k in range(len(person_list[i])):
                    word1 = np.asarray(person_list[i][k]).reshape(-1, 1)
                    word2 = np.asarray(candi_person[j]).reshape(-1, 1)
                    a = enc.transform(word1).toarray()
                    b = enc.transform(word2).toarray()
                    num += np.sum((a * b))
            res.append(num)
        return res
    if flag == "game":
        candi = get_search_result(name, nums=1, type_s='game')
        candi_person = []
        candi_person.extend(candi[0][2])
        person = []
        person.extend(candi_person)
        search_movie_person = []
        person_list = []
        query_string = """
                        SELECT ?name ?game ?developer ?genre ?abs
                        WHERE 
                        {{
                            ?game rdf:type dbo:VideoGame .
                            ?game rdfs:label ?name .
                            ?game dbo:developer ?developer .
                            ?game dbo:genre ?genre .
                            ?game dbo:abstract ?abs
                            FILTER (  (lang(?name)="zh")&&(lang(?abs)="zh")).
                        }}LIMIT 100
                        """
        results = _call_dbpedia_endpoint(query_string)
        for i in range(len(results['results']['bindings'])):
            name = results['results']['bindings'][i]['name']['value']
            developer = list(set([author.strip().split('/')[-1] for author in results['results']['bindings'][i]['developer']['value'].strip().split(' ')] ))

            person.extend(developer)
            person_list.append(developer)
        person_list = np.delete(person_list, [29], axis=0)
        enc = OneHotEncoder()
        person = np.asarray(person).reshape(-1, 1)
        enc.fit(person)
        #     person_list = enc.fit(person_list)
        res = []
        for i in range(len(person_list)):
            num = 0
            for j in range(len(candi_person)):
                for k in range(len(person_list[i])):
                    word1 = np.asarray(person_list[i][k]).reshape(-1, 1)
                    word2 = np.asarray(candi_person[j]).reshape(-1, 1)
                    a = enc.transform(word1).toarray()
                    b = enc.transform(word2).toarray()
                    num += np.sum((a * b))
            res.append(num)
        return res
    
    
def _embedding_recommand(name,flag):
    # form = SearchForm()
    # 电影
    print(1)
    if flag == 'movie':
        print("haha")
        # print(form.publication.data)
        # return
        # json = get_search_result(name=form.publication.data,type_s=form.choice.data)
        # name = "新難兄難弟"
        res1 = np.asarray(name_simi(name,"movie"))
        res2 = np.asarray(person_simi(name,"movie"))
        order = res1*2 + res2
        five = heapq.nlargest(5,range(len(order)), order.take)
        temp = open('pickle/res_list.pk', 'rb')
        op_list = pickle.load(temp)
        print(five)
        res = []
        for index in five:
            res.append(op_list[index])
        for i in range(len(res)):
            res[i][1] = res[i][1].strip().split(" , ")
            res[i][2] = res[i][2].strip().split(" , ")
    
        return res

    elif flag == 'book':
        # print(form.publication.data)
        # return
        # json = get_search_result(name=form.publication.data,type_s=form.choice.data)
        # name = "十日談
        res1 = np.asarray(name_simi(name,'book'))
        print(res1)
        res2 = np.asarray(person_simi(name,'book'))
        print(res2)
        order = res1*2 + res2
        five = heapq.nlargest(5,range(len(order)), order.take)
        temp = open('pickle/res_list_book.pk', 'rb')
        op_list = pickle.load(temp)
        print(five)
        res = []
        for index in five:
            res.append(op_list[index])
        for i in range(len(res)):
            res[i][2] = res[i][2].strip().split(" , ")
        return res
    
    elif flag == 'game':
        # print(form.publication.data)
        # return
        # json = get_search_result(name=form.publication.data,type_s=form.choice.data)
        # name = "穿越火线"
        res1 = np.asarray(name_simi(name,'game'))
        res2 = np.asarray(person_simi(name,'game'))
        order = res1*2 + res2
        five = heapq.nlargest(5,range(len(order)), order.take)
        temp = open('pickle/res_list_game.pk', 'rb')
        op_list = pickle.load(temp)
        print(five)
        res = []
        for index in five:
            res.append(op_list[index])
        for i in range(len(res)):
            res[i][2] = res[i][2].strip().split(" , ")
        return res       

            
            
def recommand(history_list, type_s='movie', nums=5):
    """ Functions:
            基于用户看过的作品历史提供待推荐的作品
            由于每次调用推荐结果时间较长，建议提前调用好结果并存储起来供推荐使用
            ** 冷启动推荐 + sparql推荐 + 作品内容相似度推荐 = 混合推荐系统 **
                冷启动：热门的作品推荐给冷启动用户
                sparql推荐：基于用户的看过的作品给推荐用户感兴趣的作品
                作品内容相似度的推荐 利用作品的各项信息进行embedding，两两作品做余弦相似度，取最相似并且未看过电影的的top
        Args:
            history_list(list): 用户看过的作品名字列表
            type_s(str): movie电影 book书籍 game游戏
            type_s(str): movie电影 book书籍 game游戏
        Returns:
            Json(list(list)): 以json的形式返回结果
    """
    # 1.若用户没有看过的作品历史，提供热门作品作为推荐冷启动
    if not history_list:
        return _cold_start(type_s)
    
    # 2.sparql推荐，通过搜索函数搜索到相关的作品信息，利用其中的人名搜索其他的作品
    infos = []
    # 2.1 搜索电影的信息
    for name in history_list:   
        info = get_search_result(name, nums=1, type_s=type_s)
        if info[0] == '':
            continue
        infos.extend(info)
    # 2.2 统计出现过的人名较多的前三个名字
    persons = []
    for info in infos:
        persons.extend(info[2] + ([info[3]] if type(info[3]) != list else info[3]))
    persons_cnt = collections.Counter(persons)
    # #输出：[('zhangsan', 4), ('lisi', 3), ('wangwu', 2)]
    persons_cnt = persons_cnt.most_common(3)
    weight_list = _weighted_mean([cnt for person, cnt in persons_cnt])

    persons = []
    for person, cnt in persons_cnt:
        persons.append(person)
        persons.append(person)
    persons = tuple(persons)
    return_info = []
    # 2.3 根据这3个名字返回相应的作品结果列表 每个名字出现的数量作为权重
    query_string = ""
    if type_s == 'movie':
        query_string = """
                        SELECT distinct ?name ?imdb ?film group_concat(?director, ' ') AS ?directors group_concat(?actor, ' ') AS ?actors ?abs
                        {{ 
                            ?film rdf:type dbo:Film .
                            ?film dbo:wikiPageExternalLink ?imdb.
                            ?film rdfs:label ?name .
                            ?film dbo:director ?director .
                            ?film dbo:starring ?actor .
                            ?film dbo:abstract ?abs.

                            FILTER ((lang(?name)="zh")&&(lang(?abs)="zh")).
                            FILTER regex(?imdb, "http://www.imdb.com/title/.*", "i").
                            {   
                                FILTER regex(?actor, "http://dbpedia.org/resource/%s").
                            } UNION {
                                FILTER regex(?director, "http://dbpedia.org/resource/%s", "i") .
                            } UNION  {   
                                FILTER regex(?actor, "http://dbpedia.org/resource/%s").
                            } UNION {
                                FILTER regex(?director, "http://dbpedia.org/resource/%s", "i") .
                            } UNION  {   
                                FILTER regex(?actor, "http://dbpedia.org/resource/%s").
                            } UNION {
                                FILTER regex(?director, "http://dbpedia.org/resource/%s", "i") .
                            } 
                        }}LIMIT 10
                        """ % persons
    elif type_s == 'book':
        query_string = """
                        SELECT ?name ?book group_concat(?author, ' ') AS ?authors ?genre ?abs
                        WHERE 
                        {{
                            ?book rdf:type dbo:Book .
                            ?book rdfs:label ?name .
                            ?book dbo:author ?author .
                            ?book dbo:literaryGenre ?genre .
                            ?book dbo:abstract ?abs.
                            FILTER ((lang(?name)="zh")&&(lang(?abs)="zh")) .

                            {   
                                FILTER regex(?author, "http://dbpedia.org/resource/%s").
                            } UNION {
                                FILTER regex(?genre, "http://dbpedia.org/resource/%s", "i") .
                            } UNION  {   
                                FILTER regex(?author, "http://dbpedia.org/resource/%s").
                            } UNION {
                                FILTER regex(?genre, "http://dbpedia.org/resource/%s", "i") .
                            } UNION  {   
                                FILTER regex(?author, "http://dbpedia.org/resource/%s").
                            } UNION {
                                FILTER regex(?genre, "http://dbpedia.org/resource/%s", "i") .
                            } 
                        }}LIMIT 10
                        """ % persons
    elif type_s == 'game':
        query_string = """
                        SELECT ?name ?game group_concat(?developer, ' ') AS ?developers ?genre ?abs
                        WHERE 
                        {{
                            ?game rdf:type dbo:VideoGame .
                            ?game rdfs:label ?name .
                            ?game dbo:developer ?developer .
                            ?game dbo:genre ?genre .
                            ?game dbo:abstract ?abs
                            FILTER (  (lang(?name)="zh")&&(lang(?abs)="zh")).
                            {   
                                FILTER regex(?developer, "http://dbpedia.org/resource/%s").
                            } UNION {
                                FILTER regex(?genre, "http://dbpedia.org/resource/%s", "i") .
                            } UNION  {   
                                FILTER regex(?developer, "http://dbpedia.org/resource/%s").
                            } UNION {
                                FILTER regex(?genre, "http://dbpedia.org/resource/%s", "i") .
                            } UNION  {   
                                FILTER regex(?developer, "http://dbpedia.org/resource/%s").
                            } UNION {
                                FILTER regex(?genre, "http://dbpedia.org/resource/%s", "i") .
                            } 
                        }}LIMIT 10
                        """ % persons
    else:
        print('请输入正确的查询类别')
            

    time.sleep(10) 
#     results = _call_dbpedia_endpoint(query_string)
#     return_info = parse_result(results, type_s)
    
    # 3.内容embedding推荐
    embed_return_info = _embedding_recommand(name, type_s)
    return_info.extend(embed_return_info)
    
    # 2.4 从返回的作品集合选出 nums=5 部待推荐作品
    index_list = random.sample(range(len(return_info)), k=nums) 
    return_info = [return_info[i] for i in index_list]
    
    return return_info
    
        
        


# In[37]:


# lis = _embedding_recommand("穿越火线",'game')


# In[49]:


# history_movie_list = ['穿越火线', '三国志X']
# recommand(history_movie_list, nums=5, type_s='game')


# In[41]:


# lis[4]


# In[31]:


# for i in range(len(lis)):
#     lis[i][2] = lis[i][2].strip().split(" , ")
    


# In[28]:


# lis[1][2]

