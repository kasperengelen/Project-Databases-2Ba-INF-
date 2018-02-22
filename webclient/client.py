from flask import Flask, render_template, flash, redirect, url_for, session, logging, request
from flask_wtf import FlaskForm
from wtforms import Form, StringField, TextAreaField, PasswordField, validators
from passlib.hash import sha256_crypt

app = Flask(__name__)

class RegisterForm(Form):
    firstname = StringField('Firstname', [validators.Length(min=1, max=50)])
    lastname = StringField('Lastname', [validators.Length(min=1, max=50)])
    email = StringField('Email', [validators.Length(min=6, max=50)])
    password = PasswordField('Password', [
        validators.DataRequired(),
        validators.EqualTo('confirm', message='Passwords do not match')
    ])
    confirm = PasswordField('Confirm Password')

class LoginForm(Form):
    email = StringField('Email', [validators.Length(min=6, max=50)])
    password = PasswordField('Password', [ validators.DataRequired() ])

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/login', methods=['GET', 'POST'])
def login():
    form = LoginForm(request.form)
    return render_template('login.html', form = form)

@app.route('/register', methods=['GET', 'POST'])
def register():
    form = RegisterForm(request.form)
    return render_template('register.html', form = form)

if __name__ == '__main__':
    app.run(debug=True)
