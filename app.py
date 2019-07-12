from flask import Flask
from flask import render_template
from flask import request

import os
import pickle

from app_form import SearchForm
from search_and_recommend import get_search_result, recommand

app = Flask(__name__)
app.config.from_object('config')


@app.route('/', methods=['GET', 'POST'])
def search():
    pre_recommand()
    f = open('pickle/movies_cand.pkl', 'rb')
    m_list = pickle.load(f)
    for i in range(len(m_list)):
        m_list[i][1] = " . ".join(m_list[i][1])
        m_list[i][2] = " . ".join(m_list[i][2])
    f = open('pickle/books_cand.pkl', 'rb')
    b_list = pickle.load(f)
    for i in range(len(b_list)):
        b_list[i][2] = " . ".join(b_list[i][2])
    f = open('pickle/games_cand.pkl', 'rb')
    g_list = pickle.load(f)
    for i in range(len(g_list)):
        g_list[i][2] = " . ".join(g_list[i][2])
    f.close()
    movie_list = [[0] * 5 for i in range(len(m_list))]
    # print(my_list)
    for i in range(len(m_list)):
        for j in range(5):
            dic = {}
            dic["id"] = j + 1
            dic["value"] = m_list[i][j]
            movie_list[i][j] = (dic)
    book_list = [[0] * 5 for i in range(len(b_list))]
    # print(my_list)
    for i in range(len(b_list)):
        for j in range(5):
            dic = {}
            dic["id"] = j + 1
            dic["value"] = b_list[i][j]
            book_list[i][j] = (dic)
    game_list = [[0] * 5 for i in range(len(g_list))]
    # print(my_list)
    for i in range(len(g_list)):
        for j in range(5):
            dic = {}
            dic["id"] = j + 1
            dic["value"] = g_list[i][j]
            game_list[i][j] = (dic)
    form = SearchForm()
    return render_template('index.html', form=form, movie_list=movie_list, book_list=book_list, game_list=game_list)


@app.route('/search_result', methods=['GET', 'POST'])
def search_result():
    form = SearchForm()
    name = (form.publication.data)
    type_s = form.choice.data
    # 电影
    json = []
    if type_s == 'movie':
        json = get_search_result(name=form.publication.data, type_s=form.choice.data)
        director = json[0][2]
        imdb = json[0][1]
        actor = json[0][3]
        json[0][1] = director
        json[0][2] = actor
        json[0][3] = imdb
        for i in range(len(json)):
            json[i][1] = " . ".join(json[i][1])
            json[i][2] = " . ".join(json[i][2])
        title = '电影搜索结果'
        page = 'index_movie.html'

    # 游戏
    elif type_s == 'book':
        json = get_search_result(name=form.publication.data, type_s=form.choice.data)
        for i in range(len(json)):
            json[i][2] = " . ".join(json[i][2])
        title = '书籍搜索结果'
        page = 'index_book.html'

    # 书籍
    elif type_s == 'game':
        json = get_search_result(name=form.publication.data, type_s=form.choice.data)
        for i in range(len(json)):
            json[i][2] = " . ".join(json[i][2])
        title = '游戏搜索结果'
        page = 'index_game.html'
    my_list = [[0] * 5 for i in range(len(json))]
    # print(my_list)
    for i in range(len(json)):
        for j in range(5):
            dic = {}
            dic["id"] = j + 1
            dic["value"] = json[i][j]
            my_list[i][j] = (dic)

    return render_template(page, title=title,my_list=my_list)


def pre_recommand():
    """
        预先推荐，推荐结果存入本地文件

        本地方硬编码了用户的一些历史记录
    """
    print('推荐开始')
    if not os.path.exists('pickle/movies_cand.pkl'):
        # 设置用户的观影历史集合
        history_movie_list = ['最愛女人購物狂', '新難兄難弟', '愛我一下夏','戀愛初歌', '同居蜜友', '扶桑花女孩']
        movies = recommand(history_movie_list, nums=5, type_s='movie')
        f = open('pickle/movies_cand.pkl', 'wb')
        pickle.dump(movies, f, 0)
        f.close()

    if not os.path.exists('pickle/books_cand.pkl'):
        # 设置用户的阅读历史集合
        history_book_list = ['十日談', '少年维特的烦恼', '龍紋身的女孩', '東方快車謀殺案']
        books = recommand(history_book_list, nums=5, type_s='book')
        f = open('pickle/books_cand.pkl', 'wb')
        pickle.dump(books, f, 0)
        f.close()

    if not os.path.exists('pickle/games_cand.pkl'):
        # 设置用户的游戏历史集合
        history_game_list = ['穿越火线', '三国志X', '狙擊之神', '冒险岛2', '合金弹头3']
        games = recommand(history_game_list, nums=5, type_s='game')
        f = open('pickle/games_cand.pkl', 'wb')
        pickle.dump(games, f, 0)
        f.close()
    print('推荐结束')


# step1 定义过滤器
def do_listreverse(li):
    temp_li = list(li)
    temp_li.reverse()
    return temp_li


# step2 添加自定义过滤器
app.add_template_filter(do_listreverse, 'listreverse')

if __name__ == '__main__':
    # print(1)
    # pre_recommand()
    app.run(debug=True)