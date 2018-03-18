from flask import Flask, flash, render_template, redirect, request, url_for, session
import user_utils
import dataset_utils
from db_wrapper import DBConnection
import utils

from dataset_utils import dataset_pages
from user_utils import user_pages
from admin_utils import admin_pages

app = Flask(__name__)
app.config.update(dict(
    SECRET_KEY = "\xbf\xcf\xde\xee\xe8\xc1\x8c\\\xfd\xe6\\!t^(\x1c/\xc6l\xe1,\xc9#\xd7",
    WTF_CSRF_SECRET_KEY = "Uei\xc2&\x8a\x18.H\x87\xc5\x1d\xd1\xc8\xc3\xcf\xe5\xfft_\x8c:\x03r"
))

app.register_blueprint(dataset_pages)
app.register_blueprint(user_pages)
app.register_blueprint(admin_pages)

@app.route('/')
def index():
    return render_template('index.html')

#################################################### ERROR HANDLING PAGES ####################################################
@app.errorhandler(403)
def handle_403(e):
    return render_template('error.html', message="403, forbidden."), 403

@app.errorhandler(404)
def handle_404(e):
    return render_template('error.html', message="404, page not found."), 404

@app.errorhandler(500)
def handle_500(e):
    return render_template('error.html', message="500, internal error."), 500

if __name__ == '__main__':
    app.run(debug=True)
