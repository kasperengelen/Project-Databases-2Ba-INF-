from flask import Flask, render_template, flash, redirect, url_for, session, logging
from wtforms import Form, StringField, TextAreaField, PasswordField, validators
from passlib.hash import sha256_crypt

app = Flask(__name__)

@app.route('/')
def index():
	return render_template('index.html')

@app.route('/login')
def login():
	return render_template('login.html')

@app.route('/register')
def register():
	return render_template('register.html')

if __name__ == '__main__':
	app.run(debug=True)