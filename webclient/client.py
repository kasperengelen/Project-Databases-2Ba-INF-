from flask import Flask, render_template, redirect, request
import user_manager


app = Flask(__name__)
app.config.update(dict(
    SECRET_KEY = "\xbf\xcf\xde\xee\xe8\xc1\x8c\\\xfd\xe6\\!t^(\x1c/\xc6l\xe1,\xc9#\xd7",
    WTF_CSRF_SECRET_KEY = "Uei\xc2&\x8a\x18.H\x87\xc5\x1d\xd1\xc8\xc3\xcf\xe5\xfft_\x8c:\x03r"
))

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/login', methods=['GET', 'POST'])
def login():
    return user_manager.login_user(request)

@app.route('/register', methods=['GET', 'POST'])
def register():
    return user_manager.register_user(request)

if __name__ == '__main__':
    app.run(debug=True)
