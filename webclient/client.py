import sys
sys.path.append('./View/')
sys.path.append('./Model/')
sys.path.append('./Controller/')

from flask import Flask, render_template, g, session
import utils
from utils import LoginManager
from UserManager import UserManager
import user_pages
import admin_pages
import dataset_pages
from DatabaseConfiguration import DatabaseConfiguration

app = Flask(__name__, template_folder="./View/templates/")
app.config.update(dict(
    SECRET_KEY = "\xbf\xcf\xde\xee\xe8\xc1\x8c\\\xfd\xe6\\!t^(\x1c/\xc6l\xe1,\xc9#\xd7",
    WTF_CSRF_SECRET_KEY = "Uei\xc2&\x8a\x18.H\x87\xc5\x1d\xd1\xc8\xc3\xcf\xe5\xfft_\x8c:\x03r",
    UPLOAD_FOLDER = "./upload",
    DOWNLOAD_FOLDER = "./download"
))

app.register_blueprint(user_pages.user_pages)
app.register_blueprint(admin_pages.admin_pages)
app.register_blueprint(dataset_pages.dataset_pages)
@app.before_request
def before_request():
    """Prepare request."""

    g.db_conn = DatabaseConfiguration.toPSQL()
    g.sqla_engine = DatabaseConfiguration.toSQLA()

    if not 'loggedin' in session:
        session['loggedin'] = False
        session['userdata'] = None

    # make sure that user information is up to date
    if 'loggedin' in session and session['loggedin']:
        if not UserManager.existsID(session['userdata']['userid']):
            LoginManager.setLoggedOut()
            return
        utils.sync_user_info()

        if not session['userdata']['active']:
            LoginManager.setLoggedOut()
            return
# ENDFUNCTION

@app.teardown_request
def teardown_request(e):
    """Postprocess request."""
    g.db_conn.close()
    g.sqla_engine.dispose()
# ENDFUNCTION

@app.route('/')
def index():
    return render_template('index.html')

################################################### ERROR HANDLING PAGES ####################################################
@app.errorhandler(400)
def handle_400(e):
    return render_template('error.html', message="400, bad request."), 400

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
