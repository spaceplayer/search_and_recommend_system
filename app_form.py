from wtforms import BooleanField, StringField, PasswordField, validators, SubmitField, SelectField
from flask_wtf import Form



class SearchForm(Form):
    publication = StringField('请输入您要查询的出版物：',validators=[validators.DataRequired()])
    choice = SelectField('选择类型',choices=[
        ('movie','电影'),
        ('book','书籍'),
        ('game','游戏')
    ])
    submit = SubmitField('提交')